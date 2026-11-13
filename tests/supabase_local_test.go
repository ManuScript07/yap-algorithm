package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"yap-algorithm/internal/adapters/postgres"
	"yap-algorithm/internal/application"
	"yap-algorithm/internal/domain"
)

const sbModel = "intfloat/multilingual-e5-small@614241f622f53c4eeff9890bdc4f31cfecc418b3:onnx-int8:query"

type sbFixture struct {
	admin                    *pgxpool.Pool
	author, viewer, stranger string
}

func sbExec(t *testing.T, pool *pgxpool.Pool, sql string, args ...any) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), sql, args...); err != nil {
		t.Fatal(err)
	}
}

func sbUsers(t *testing.T, admin *pgxpool.Pool) sbFixture {
	t.Helper()
	f := sbFixture{admin: admin, author: domain.UUID(), viewer: domain.UUID(), stranger: domain.UUID()}
	ids := []string{f.author, f.viewer, f.stranger}
	for _, id := range ids {
		sbExec(t, admin, `insert into auth.users(id,is_sso_user,is_anonymous,raw_user_meta_data)values($1,false,false,'{"name":"Recommendation test"}')`, id)
	}
	t.Cleanup(func() {
		sbExec(t, admin, "delete from private.account_deletion_requests where target_user_id=any($1::uuid[])", ids)
		sbExec(t, admin, "delete from private.global_account_bans where target_user_id=any($1::uuid[])", ids)
		sbExec(t, admin, "delete from public.profiles where id=any($1::uuid[])", ids)
		sbExec(t, admin, "delete from auth.users where id=any($1::uuid[])", ids)
	})
	return f
}

func (f sbFixture) rpc(user, query string, args ...any) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := f.admin.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(ctx, "select set_config('request.jwt.claim.sub',$1,true)", user); err != nil {
		return nil, err
	}
	if _, err = tx.Exec(ctx, "set local role authenticated"); err != nil {
		return nil, err
	}
	var out []byte
	if err = tx.QueryRow(ctx, query, args...).Scan(&out); err != nil {
		return nil, err
	}
	return out, tx.Commit(ctx)
}
func (f sbFixture) create(t *testing.T, text, audience string) string {
	t.Helper()
	id := domain.UUID()
	if _, err := f.rpc(f.author, "select public.create_feed_post($1,$2,$3,'ru')", id, text, audience); err != nil {
		t.Fatal(err)
	}
	return id
}
func sbErrorCode(t *testing.T, err error, code string) {
	t.Helper()
	var pgerr *pgconn.PgError
	if !errors.As(err, &pgerr) || pgerr.Code != code {
		t.Fatalf("expected SQLSTATE %s, got %v", code, err)
	}
}
func sbVector() []float64 { v := make([]float64, domain.Dimensions); v[0] = 1; return v }

func sbWorkerCommand(workerDSN string) *exec.Cmd {
	cmd := exec.Command("go", "run", "./cmd/worker", "-once", "-concurrency", "2")
	cmd.Dir = ".."
	for _, entry := range os.Environ() {
		name := strings.SplitN(entry, "=", 2)[0]
		if !strings.HasPrefix(name, "YAP_SUPABASE_") && name != "DATABASE_URL" && name != "MODEL_URL" {
			cmd.Env = append(cmd.Env, entry)
		}
	}
	cmd.Env = append(cmd.Env, "DATABASE_URL="+workerDSN, "MODEL_URL=http://127.0.0.1:8091")
	return cmd
}

type sbEncoder struct{ calls atomic.Int32 }

func (e *sbEncoder) Encode(context.Context, string) ([]float64, error) {
	e.calls.Add(1)
	return sbVector(), nil
}

// These tests explicitly opt into the application's real local Supabase. They
// never apply migrations and never accept a remote database or API endpoint.
func TestSupabaseLocalPipeline(t *testing.T) {
	if os.Getenv("YAP_SUPABASE_INTEGRATION") != "1" {
		t.Skip("run scripts/test-supabase-local.ps1 against local YapChat Supabase")
	}
	dsn := os.Getenv("YAP_SUPABASE_LOCAL_ADMIN_DSN")
	u, err := url.Parse(dsn)
	if err != nil || !localSupabaseTarget(t).acceptsURL(u) {
		t.Fatal("tests require the selected local Supabase database")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal("cannot open local admin pool")
	}
	t.Cleanup(admin.Close)
	var pending int
	if err = admin.QueryRow(ctx, "select count(*)from recsys.index_jobs where done_at is null").Scan(&pending); err != nil {
		t.Fatal("apply the canonical YapChat feed migration first")
	}
	if pending != 0 {
		t.Fatal("stop development workers and drain the existing local index backlog before this isolated test")
	}
	role := "yap_recsys_test_" + strings.ReplaceAll(domain.UUID(), "-", "")
	password := domain.UUID()
	roleSQL := pgx.Identifier{role}.Sanitize()
	sbExec(t, admin, "create role "+roleSQL+" login password '"+password+"' nosuperuser nocreatedb nocreaterole inherit nobypassrls")
	sbExec(t, admin, "grant yap_recommendation_worker to "+roleSQL)
	t.Cleanup(func() { sbExec(t, admin, "drop role "+roleSQL) })
	u.User = url.UserPassword(role, password)
	repo, err := postgres.OpenIndexRepository(ctx, u.String(), sbModel, 10)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(repo.Close)
	workerPool, err := pgxpool.New(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(workerPool.Close)

	t.Run("real_schema_rpc_and_indexer", func(t *testing.T) {
		f := sbUsers(t, admin)
		text := strings.Repeat("😀", 1000)
		id := f.create(t, text, "public")
		for i := 0; i < 3; i++ {
			if _, err := f.rpc(f.author, "select public.create_feed_post($1,$2,'public','ru')", id, text); err != nil {
				t.Fatal(err)
			}
		}
		var jobs, charges int
		if err := admin.QueryRow(ctx, "select (select count(*)from recsys.index_jobs where post_id=$1),(select request_count from private.feed_operation_limits where user_id=$2 and operation='post_create')", id, f.author).Scan(&jobs, &charges); err != nil {
			t.Fatal(err)
		}
		if jobs != 1 || charges != 1 {
			t.Fatalf("non-idempotent retry: jobs=%d quota=%d", jobs, charges)
		}
		_, err := f.rpc(f.author, "select public.create_feed_post($1,$2,'public','ru')", domain.UUID(), text+"я")
		sbErrorCode(t, err, "22023")
		encoder := &sbEncoder{}
		if worked, err := (application.Indexer{Repository: repo, Encoder: encoder}).Once(ctx); err != nil || !worked {
			t.Fatalf("index: %v", err)
		}
		var version int
		if err := admin.QueryRow(ctx, "select post_version from recsys.post_embeddings where post_id=$1", id).Scan(&version); err != nil || version != 1 {
			t.Fatalf("derived vector missing: %v", err)
		}
		body, err := f.rpc(f.viewer, "select public.get_feed_post($1)", id)
		if err != nil || !bytes.Contains(body, []byte("author_id")) || bytes.Contains(body, []byte("embedding")) {
			t.Fatal("incorrect client response")
		}
	})
	t.Run("edit_and_expired_lease_fencing", func(t *testing.T) {
		f := sbUsers(t, admin)
		id := f.create(t, "old text", "public")
		old, found, err := repo.Claim(ctx)
		if err != nil || !found || old.PostID != id {
			t.Fatal("claim failed")
		}
		if _, err := f.rpc(f.author, "select public.edit_feed_post($1,1,'new text')", id); err != nil {
			t.Fatal(err)
		}
		if accepted, err := repo.Complete(ctx, old, sbVector()); err != nil || !accepted {
			t.Fatalf("complete superseded job: %v", err)
		}
		var count int
		_ = admin.QueryRow(ctx, "select count(*)from recsys.post_embeddings where post_id=$1", id).Scan(&count)
		if count != 0 {
			t.Fatal("old content vector was published")
		}
		current, found, err := repo.Claim(ctx)
		if err != nil || !found || current.Version != 2 {
			t.Fatal("new version not queued")
		}
		sbExec(t, admin, "update recsys.index_jobs set locked_until=clock_timestamp()-interval '1 second' where id=$1", current.ID)
		reclaimed, found, err := repo.Claim(ctx)
		if err != nil || !found || reclaimed.ID != current.ID || reclaimed.Lease == current.Lease {
			t.Fatal("expired lease not reclaimed")
		}
		if accepted, err := repo.Complete(ctx, current, sbVector()); err != nil || accepted {
			t.Fatal("expired owner completed job")
		}
		if err := repo.Retry(ctx, current); err != nil {
			t.Fatal(err)
		}
		if accepted, err := repo.Complete(ctx, reclaimed, sbVector()); err != nil || !accepted {
			t.Fatalf("new lease rejected: %v", err)
		}
		if accepted, err := repo.Complete(ctx, reclaimed, sbVector()); err != nil || accepted {
			t.Fatal("duplicate completion accepted")
		}
		_, err = f.rpc(f.author, "select public.edit_feed_post($1,1,'lost update')", id)
		sbErrorCode(t, err, "40001")
		// Retrying the original creation request after editing is still safe.
		if _, err := f.rpc(f.author, "select public.create_feed_post($1,'old text','public','ru')", id); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("privacy_is_checked_on_each_read", func(t *testing.T) {
		f := sbUsers(t, admin)
		id := f.create(t, "friends text", "friends")
		get := func(wantVisible bool) {
			t.Helper()
			raw, err := f.rpc(f.viewer, "select public.get_feed_post($1)", id)
			if err != nil || (len(raw) > 0) != wantVisible {
				t.Fatalf("visibility want=%v raw=%s err=%v", wantVisible, raw, err)
			}
		}
		get(false)
		sbExec(t, admin, "insert into public.friendships(user_one_id,user_two_id)values(least($1::uuid,$2::uuid),greatest($1::uuid,$2::uuid))", f.author, f.viewer)
		get(true)
		sbExec(t, admin, "delete from public.friendships where user_one_id=least($1::uuid,$2::uuid)and user_two_id=greatest($1::uuid,$2::uuid)", f.author, f.viewer)
		get(false)
		publicID := f.create(t, "public text", "public")
		id = publicID
		get(true)
		for _, pair := range [][2]string{{f.viewer, f.author}, {f.author, f.viewer}} {
			sbExec(t, admin, "insert into public.user_blocks(blocker_user_id,blocked_user_id)values($1,$2)", pair[0], pair[1])
			get(false)
			sbExec(t, admin, "delete from public.user_blocks where blocker_user_id=$1 and blocked_user_id=$2", pair[0], pair[1])
		}
		sbExec(t, admin, "insert into private.global_account_bans(target_user_id,note)values($1,'local recommendation test')", f.author)
		get(false)
		sbExec(t, admin, "delete from private.global_account_bans where target_user_id=$1", f.author)
		get(true)
		sbExec(t, admin, "insert into private.account_deletion_requests(target_user_id)values($1)", f.author)
		get(false)
		_, err := f.rpc(f.author, "select public.get_feed_post($1)", publicID)
		sbErrorCode(t, err, "P0001")
		_, err = f.rpc(f.stranger, "select public.edit_feed_post($1,1,'hijacked')", publicID)
		sbErrorCode(t, err, "P0002")
	})
	t.Run("parallel_claims_and_idempotent_creation", func(t *testing.T) {
		f := sbUsers(t, admin)
		id := domain.UUID()
		var group sync.WaitGroup
		errs := make(chan error, 100)
		for i := 0; i < 12; i++ {
			group.Add(1)
			go func() {
				defer group.Done()
				_, err := f.rpc(f.author, "select public.create_feed_post($1,'concurrent post','public','ru')", id)
				if err != nil {
					errs <- err
				}
			}()
		}
		group.Wait()
		for i := 0; i < 31; i++ {
			sbExec(t, admin, `insert into feed.posts(id,author_id,text,audience,language,creation_request)values($1,$2,'batch corpus','public','en','[]')`, domain.UUID(), f.author)
		}
		encoder := &sbEncoder{}
		indexer := application.Indexer{Repository: repo, Encoder: encoder}
		for i := 0; i < 8; i++ {
			group.Add(1)
			go func() {
				defer group.Done()
				for {
					worked, err := indexer.Once(ctx)
					if err != nil {
						errs <- err
						return
					}
					if !worked {
						return
					}
				}
			}()
		}
		group.Wait()
		close(errs)
		for err := range errs {
			t.Error(err)
		}
		if encoder.calls.Load() != 32 {
			t.Fatalf("duplicate/lost claims: encoded %d, want 32", encoder.calls.Load())
		}
		var total, charges int
		if err := admin.QueryRow(ctx, "select (select count(*)from recsys.post_embeddings e join feed.posts p on p.id=e.post_id where p.author_id=$1),(select request_count from private.feed_operation_limits where user_id=$1 and operation='post_create')", f.author).Scan(&total, &charges); err != nil || total != 32 || charges != 1 {
			t.Fatalf("parallel result total=%d quota=%d err=%v", total, charges, err)
		}
	})
	t.Run("completion_races_with_edit_without_deadlock", func(t *testing.T) {
		f := sbUsers(t, admin)
		id := f.create(t, "start", "public")
		for i := 0; i < 25; i++ {
			job, found, err := repo.Claim(ctx)
			if err != nil || !found {
				t.Fatalf("claim iteration %d: %v", i, err)
			}
			errs := make(chan error, 2)
			go func() { _, err := repo.Complete(ctx, job, sbVector()); errs <- err }()
			go func() {
				_, err := admin.Exec(ctx, "update feed.posts set text=$2 where id=$1", id, fmt.Sprint("edit ", i))
				errs <- err
			}()
			for n := 0; n < 2; n++ {
				if err := <-errs; err != nil {
					t.Fatal(err)
				}
			}
			var stale int
			if err := admin.QueryRow(ctx, "select count(*)from recsys.post_embeddings e join feed.posts p on p.id=e.post_id where p.id=$1 and e.post_version<>p.version", id).Scan(&stale); err != nil || stale != 0 {
				t.Fatalf("stale vector published: %v", err)
			}
		}
	})
	t.Run("quota_and_worker_privileges", func(t *testing.T) {
		f := sbUsers(t, admin)
		for i := 0; i < 12; i++ {
			f.create(t, fmt.Sprint("post ", i), "public")
		}
		_, err := f.rpc(f.author, "select public.create_feed_post($1,'too many','public','ru')", domain.UUID())
		sbErrorCode(t, err, "42901")
		duplicateID := domain.UUID()
		for i := 0; i < 60; i++ {
			if _, err := f.rpc(f.stranger, "select public.create_feed_post($1,'idempotent','public','ru')", duplicateID); err != nil {
				t.Fatal(err)
			}
		}
		_, err = f.rpc(f.stranger, "select public.create_feed_post($1,'idempotent','public','ru')", duplicateID)
		sbErrorCode(t, err, "42901")
		// Counts are based on actual publications over a sliding hour, even if
		// a post is soft-deleted. A fixed minute/day counter cannot reset it.
		var oldest string
		if err := admin.QueryRow(ctx, "select id::text from feed.posts where author_id=$1 order by created_at,id limit 1", f.author).Scan(&oldest); err != nil {
			t.Fatal(err)
		}
		if _, err := f.rpc(f.author, "select public.delete_feed_post($1)", oldest); err != nil {
			t.Fatal(err)
		}
		_, err = f.rpc(f.author, "select public.create_feed_post($1,'delete bypass','public','ru')", domain.UUID())
		sbErrorCode(t, err, "42901")
		sbExec(t, admin, "update feed.posts set created_at=statement_timestamp()-interval '61 minutes'where id=$1", oldest)
		if _, err := f.rpc(f.author, "select public.create_feed_post($1,'hour slot released','public','ru')", domain.UUID()); err != nil {
			t.Fatal(err)
		}
		for _, query := range []string{"select * from feed.posts", "update feed.posts set text='worker overwrite'", "select * from public.profiles", "select * from recsys.post_embeddings"} {
			_, err := workerPool.Exec(ctx, query)
			sbErrorCode(t, err, "42501")
		}
		_, err = f.rpc(f.author, "select recsys.pipeline_contract()")
		sbErrorCode(t, err, "42501")
		sbExec(t, admin, "update private.feed_operation_limits set request_count=60 where user_id=$1 and operation='post_read'", f.viewer)
		// A read quota row is first created by a successful normal read.
		var postID string
		_ = admin.QueryRow(ctx, "select id::text from feed.posts where author_id=$1 limit 1", f.author).Scan(&postID)
		if _, err := f.rpc(f.viewer, "select public.get_feed_post($1)", postID); err != nil {
			t.Fatal(err)
		}
		sbExec(t, admin, "update private.feed_operation_limits set request_count=60 where user_id=$1 and operation='post_read'", f.viewer)
		_, err = f.rpc(f.viewer, "select public.get_feed_post($1)", postID)
		sbErrorCode(t, err, "42901")
	})
	t.Run("retry_backoff_and_source_delete", func(t *testing.T) {
		f := sbUsers(t, admin)
		id := f.create(t, "retry post", "public")
		job, found, err := repo.Claim(ctx)
		if err != nil || !found {
			t.Fatal("no retry job")
		}
		if err := repo.Retry(ctx, job); err != nil {
			t.Fatal(err)
		}
		if _, found, err := repo.Claim(ctx); err != nil || found {
			t.Fatal("durable backoff ignored")
		}
		sbExec(t, admin, "update recsys.index_jobs set available_at=clock_timestamp()-interval '1 second' where id=$1", job.ID)
		job, found, err = repo.Claim(ctx)
		if err != nil || !found {
			t.Fatal("retry not available")
		}
		if _, err := f.rpc(f.author, "select public.delete_feed_post($1)", id); err != nil {
			t.Fatal(err)
		}
		if _, err := repo.Complete(ctx, job, sbVector()); err != nil {
			t.Fatal(err)
		}
		var count int
		_ = admin.QueryRow(ctx, "select count(*)from recsys.post_embeddings where post_id=$1", id).Scan(&count)
		if count != 0 {
			t.Fatal("deleted post was indexed")
		}
	})
	t.Run("bounded_backlog_and_read_burst", func(t *testing.T) {
		if os.Getenv("YAP_SUPABASE_LOAD_TEST") != "1" {
			t.Skip("enable -WithLoad")
		}
		f := sbUsers(t, admin)
		started := time.Now()
		sbExec(t, admin, `insert into feed.posts(id,author_id,text,audience,language,creation_request)
select extensions.gen_random_uuid(),$1,'Technical queue load sample '||n,'public','en','[]' from generate_series(1,10001)n`, f.author)
		seedDuration := time.Since(started)
		encoder := &sbEncoder{}
		indexer := application.Indexer{Repository: repo, Encoder: encoder}
		var group sync.WaitGroup
		errs := make(chan error, 8)
		started = time.Now()
		for i := 0; i < 8; i++ {
			group.Add(1)
			go func() {
				defer group.Done()
				for {
					worked, err := indexer.Once(ctx)
					if err != nil {
						errs <- err
						return
					}
					if !worked {
						return
					}
				}
			}()
		}
		group.Wait()
		close(errs)
		for err := range errs {
			t.Error(err)
		}
		drainDuration := time.Since(started)
		if encoder.calls.Load() != 10001 {
			t.Fatalf("backlog: %d outputs, want 10001", encoder.calls.Load())
		}
		var indexed int
		if err := admin.QueryRow(ctx, "select count(*)from recsys.post_embeddings e join feed.posts p on p.id=e.post_id where p.author_id=$1 and e.post_version=p.version", f.author).Scan(&indexed); err != nil || indexed != 10001 {
			t.Fatalf("backlog result: %d %v", indexed, err)
		}
		var id string
		if err := admin.QueryRow(ctx, "select id::text from feed.posts where author_id=$1 limit 1", f.author).Scan(&id); err != nil {
			t.Fatal(err)
		}
		viewers := []string{}
		for i := 0; i < 4; i++ {
			fixture := sbUsers(t, admin)
			viewers = append(viewers, fixture.author, fixture.viewer, fixture.stranger)
		}
		latencies := make(chan time.Duration, 500)
		readErrors := make(chan error, 500)
		started = time.Now()
		for worker := 0; worker < 16; worker++ {
			group.Add(1)
			go func(worker int) {
				defer group.Done()
				for i := worker; i < 500; i += 16 {
					before := time.Now()
					raw, err := f.rpc(viewers[i%len(viewers)], "select public.get_feed_post($1)", id)
					if err != nil {
						readErrors <- err
					} else if len(raw) == 0 {
						readErrors <- errors.New("public post missing")
					}
					latencies <- time.Since(before)
				}
			}(worker)
		}
		group.Wait()
		close(latencies)
		close(readErrors)
		readDuration := time.Since(started)
		for err := range readErrors {
			t.Error(err)
		}
		values := make([]time.Duration, 0, 500)
		for latency := range latencies {
			values = append(values, latency)
		}
		sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
		t.Logf("10,001 posts+outbox: %s; queue drain with fake encoder, 8 workers: %s (%.1f jobs/s)", seedDuration, drainDuration, 10001/drainDuration.Seconds())
		t.Logf("500 SQL RPC reads, 12 users, 16 parallel: %s; p95=%s p99=%s (excludes Auth/PostgREST network and recommendation serving)", readDuration, values[474], values[494])
	})
	t.Run("actual_auth_and_postgrest", func(t *testing.T) { sbREST(t, admin, u.String()) })
	t.Run("personalized_feed", func(t *testing.T) { sbRecommendations(t, admin, repo, workerPool) })
}

func sbREST(t *testing.T, admin *pgxpool.Pool, workerDSN string) {
	base := os.Getenv("YAP_SUPABASE_LOCAL_URL")
	anon := os.Getenv("YAP_SUPABASE_LOCAL_ANON_KEY")
	service := os.Getenv("YAP_SUPABASE_LOCAL_SERVICE_KEY")
	if base != localSupabaseTarget(t).apiURL || anon == "" || service == "" {
		t.Fatal("local Auth/API test configuration missing")
	}
	client := &http.Client{Timeout: 10 * time.Second}
	request := func(path, key, token string, payload any, output any) int {
		t.Helper()
		body, _ := json.Marshal(payload)
		req, err := http.NewRequest("POST", base+path, bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("apikey", key)
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal("local HTTP request failed")
		}
		defer resp.Body.Close()
		if output != nil && resp.StatusCode < 300 {
			if err := json.NewDecoder(resp.Body).Decode(output); err != nil {
				t.Fatal("invalid local HTTP JSON")
			}
		}
		return resp.StatusCode
	}
	email := "recsys-" + domain.UUID() + "@example.com"
	password := domain.UUID()
	var user struct {
		ID string `json:"id"`
	}
	if status := request("/auth/v1/admin/users", service, service, map[string]any{"email": email, "password": password, "email_confirm": true}, &user); status != 200 || !domain.UUIDValid(user.ID) {
		t.Fatalf("create local Auth fixture: status %d", status)
	}
	t.Cleanup(func() {
		sbExec(t, admin, "delete from public.profiles where id=$1", user.ID)
		sbExec(t, admin, "delete from auth.users where id=$1", user.ID)
	})
	var session struct {
		Token string `json:"access_token"`
	}
	if status := request("/auth/v1/token?grant_type=password", anon, anon, map[string]string{"email": email, "password": password}, &session); status != 200 || session.Token == "" {
		t.Fatalf("real Auth login: status %d", status)
	}
	id := domain.UUID()
	payload := map[string]any{"requested_id": id, "requested_text": "Real Supabase integration", "requested_audience": "public", "requested_language": "en"}
	var post map[string]any
	if status := request("/rest/v1/rpc/create_feed_post", anon, session.Token, payload, &post); status != 200 || post["id"] != id {
		t.Fatalf("real RPC create: status %d", status)
	}
	if os.Getenv("YAP_SUPABASE_MODEL_TEST") == "1" {
		cmd := sbWorkerCommand(workerDSN)
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("real worker command: %v\n%s", err, output)
		}
		var version int
		var revision string
		var norm float64
		if err := admin.QueryRow(context.Background(), "select post_version,model,extensions.vector_norm(embedding)from recsys.post_embeddings where post_id=$1", id).Scan(&version, &revision, &norm); err != nil || version != 1 || revision != sbModel || norm < .999 || norm > 1.001 {
			t.Fatalf("real E5 output was not committed: %v", err)
		}
	}
	if status := request("/rest/v1/rpc/get_feed_post", anon, session.Token, map[string]string{"requested_id": id}, &post); status != 200 || post["id"] != id {
		t.Fatalf("real RPC read: status %d", status)
	}
	if status := request("/rest/v1/rpc/get_feed_post", anon, anon, map[string]string{"requested_id": id}, nil); status != 401 && status != 403 {
		t.Fatalf("anonymous access: status %d", status)
	}
	// A second real Auth user exercises the future client's exact feed/action
	// contract. This is separate from SQL identity emulation used by other tests.
	readerEmail := "reader-" + domain.UUID() + "@example.com"
	readerPassword := domain.UUID()
	var reader struct {
		ID string `json:"id"`
	}
	if status := request("/auth/v1/admin/users", service, service, map[string]any{"email": readerEmail, "password": readerPassword, "email_confirm": true}, &reader); status != 200 || reader.ID == "" {
		t.Fatalf("reader Auth setup: %d", status)
	}
	t.Cleanup(func() {
		sbExec(t, admin, "delete from public.profiles where id=$1", reader.ID)
		sbExec(t, admin, "delete from auth.users where id=$1", reader.ID)
	})
	var readerSession struct {
		Token string `json:"access_token"`
	}
	if status := request("/auth/v1/token?grant_type=password", anon, anon, map[string]string{"email": readerEmail, "password": readerPassword}, &readerSession); status != 200 {
		t.Fatalf("reader Auth login: %d", status)
	}
	var feed sbRecommendationPage
	if status := request("/rest/v1/rpc/get_for_you_feed", anon, readerSession.Token, map[string]int{"requested_limit": 20}, &feed); status != 200 || len(feed.Items) != 1 {
		t.Fatalf("real feed RPC: %d", status)
	}
	var action map[string]any
	if status := request("/rest/v1/rpc/apply_feed_action", anon, readerSession.Token, map[string]any{"requested_id": domain.UUID(), "requested_delivery": feed.Items[0].Delivery, "requested_action": "like", "expected_version": 0}, &action); status != 200 || action["liked"] != true || action["action_version"] != float64(1) {
		t.Fatalf("real action RPC: %d", status)
	}
	if status := request("/rest/v1/rpc/apply_feed_action", anon, readerSession.Token, map[string]any{"requested_id": domain.UUID(), "requested_delivery": feed.Items[0].Delivery, "requested_action": "unlike", "expected_version": 0}, nil); status != 409 {
		t.Fatalf("optimistic conflict must be HTTP 409, got %d", status)
	}
	sbExec(t, admin, "update recsys.deliveries set delivered_at=statement_timestamp()-interval '2 seconds'where id=$1", feed.Items[0].Delivery)
	var eventResult map[string]any
	events := map[string]any{"requested_events": []map[string]any{{"event_id": domain.UUID(), "delivery_id": feed.Items[0].Delivery, "kind": "impression", "visible_ms": 1000, "visible_fraction": 1}}}
	if status := request("/rest/v1/rpc/record_feed_events", anon, readerSession.Token, events, &eventResult); status != 200 || eventResult["accepted"] != float64(1) {
		t.Fatalf("real telemetry RPC: %d", status)
	}
	if status := request("/rest/v1/rpc/record_feed_events", anon, readerSession.Token, events, &eventResult); status != 200 || eventResult["duplicates"] != float64(1) {
		t.Fatalf("real telemetry retry: %d", status)
	}
	if status := request("/rest/v1/rpc/get_for_you_feed", anon, readerSession.Token, map[string]int{"requested_limit": 20}, &feed); status != 200 {
		t.Fatalf("refresh request: %d", status)
	}
	if os.Getenv("YAP_SUPABASE_MODEL_TEST") == "1" {
		cmd := sbWorkerCommand(workerDSN)
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("real personalization worker: %v\n%s", err, output)
		}
		if status := request("/rest/v1/rpc/get_for_you_feed", anon, readerSession.Token, map[string]int{"requested_limit": 20}, &feed); status != 200 || feed.Model != "personalized-v2" || len(feed.Items) != 1 {
			t.Fatalf("real prepared feed: %d", status)
		}
		var clusters int
		if err := admin.QueryRow(context.Background(), "select count(*)from recsys.interest_clusters where user_id=$1", reader.ID).Scan(&clusters); err != nil || clusters != 1 {
			t.Fatal("real E5-backed like did not create an interest")
		}
	}
	if status := request("/rest/v1/rpc/get_for_you_feed", anon, anon, map[string]int{"requested_limit": 20}, nil); status != 401 && status != 403 {
		t.Fatalf("anonymous feed access: %d", status)
	}
	sbExec(t, admin, "update recsys.feed_snapshots set expires_at=statement_timestamp()-interval '1 second'where id=$1", feed.Session)
	if status := request("/rest/v1/rpc/get_for_you_feed", anon, readerSession.Token, map[string]any{"requested_session": feed.Session}, nil); status != 410 {
		t.Fatalf("expired session must be HTTP 410: %d", status)
	}
	if status := request("/rest/v1/rpc/get_for_you_feed", anon, readerSession.Token, map[string]any{"requested_session": domain.UUID()}, nil); status != 404 {
		t.Fatalf("unknown session must be HTTP 404: %d", status)
	}
}
