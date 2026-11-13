package tests

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"yap-algorithm/internal/adapters/postgres"
	"yap-algorithm/internal/application"
	"yap-algorithm/internal/domain"
)

// Only used against the explicit local fixture; never passed to worker processes.
func localMediaJWT(t *testing.T, id string) string {
	t.Helper()
	secret := os.Getenv("YAP_SUPABASE_LOCAL_JWT_SECRET")
	if secret == "" {
		t.Fatal("local JWT secret missing")
	}
	encode := base64.RawURLEncoding.EncodeToString
	header := encode([]byte(`{"alg":"HS256","typ":"JWT"}`))
	payload, _ := json.Marshal(map[string]any{"role": "authenticated", "sub": id, "aud": "authenticated", "iat": time.Now().Unix(), "exp": time.Now().Add(3 * time.Hour).Unix()})
	body := header + "." + encode(payload)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(body))
	return body + "." + encode(mac.Sum(nil))
}

func capacityAdmin(t *testing.T) *pgxpool.Pool {
	t.Helper()
	if os.Getenv("YAP_SUPABASE_MEDIA_TEST") != "1" {
		t.Skip("enable -WithMedia")
	}
	if os.Getenv("YAP_SUPABASE_LOCAL_URL") != localSupabaseTarget(t).apiURL {
		t.Fatal("local Supabase required")
	}
	cfg, err := pgxpool.ParseConfig(os.Getenv("YAP_SUPABASE_LOCAL_ADMIN_DSN"))
	if err != nil || !localSupabaseTarget(t).acceptsPool(cfg) {
		t.Fatal("explicit local database required")
	}
	cfg.MaxConns = 16
	p, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p
}

func assertMediaAccounting(t *testing.T, p *pgxpool.Pool) {
	t.Helper()
	var okay bool
	err := p.QueryRow(context.Background(), `select c.active=(select count(*)from media.reservations)
 and c.unfinished=(select count(*)from media.reservations where unfinished)
 and c.reserved_bytes=coalesce((select sum(bytes)from media.reservations),0)
 and c.temporary_bytes=coalesce((select sum(estimated_bytes)from media.cleanup_jobs),0)
 from media.capacity c`).Scan(&okay)
	if err != nil || !okay {
		t.Fatalf("media counter drift: %v", err)
	}
}

func TestSupabaseLocalMediaCapacity(t *testing.T) {
	p := capacityAdmin(t)
	ctx := context.Background()
	sbExec(t, p, `update media.capacity set paused=false,max_active=256,max_unfinished=120,max_reserved_bytes=536870912,max_temporary_bytes=1073741824,max_queue_age_seconds=60,max_cleanup_backlog=2048,max_parallel=2,max_workers=2,max_worker_connections=6`)
	t.Cleanup(func() {
		sbExec(t, p, `update media.capacity set paused=false,max_active=256,max_unfinished=120,max_reserved_bytes=536870912,max_temporary_bytes=1073741824,max_queue_age_seconds=60,max_cleanup_backlog=2048,max_parallel=2,max_workers=2,max_worker_connections=6`)
		assertMediaAccounting(t, p)
	})
	f := sbUsers(t, p)
	ids := []string{f.author, f.viewer, f.stranger}
	t.Cleanup(func() {
		for _, id := range ids {
			sbExec(t, p, "delete from public.profiles where id=$1", id)
			sbExec(t, p, "delete from media.cleanup_jobs where path like $1", id+"/%")
		}
	})
	reserve := func(user, post, id string) ([]byte, error) {
		return f.rpc(user, "select public.reserve_feed_photo($1,$2,1024,'image/jpeg')", post, id)
	}

	t.Run("atomic_admission_across_users_and_replay_while_paused", func(t *testing.T) {
		sbExec(t, p, "update media.capacity set max_active=8,max_unfinished=8")
		posts := make([]string, 9)
		for i := range posts {
			posts[i] = domain.UUID()
		}
		var wg sync.WaitGroup
		var mu sync.Mutex
		accepted := 0
		busy := 0
		var firstPost, firstPhoto, firstOwner string
		for i := 0; i < 24; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				owner := ids[i/8]
				post := posts[(i/8)*3+(i%8)/3]
				id := domain.UUID()
				_, err := reserve(owner, post, id)
				mu.Lock()
				defer mu.Unlock()
				if err == nil {
					accepted++
					firstPost, firstPhoto, firstOwner = post, id, owner
				} else {
					busy++
					var pgerr *pgconn.PgError
					if !errors.As(err, &pgerr) || pgerr.Code != "PGRST" {
						t.Errorf("unexpected concurrent reservation error: %v", err)
					}
				}
			}(i)
		}
		wg.Wait()
		if accepted != 8 || busy != 16 {
			t.Fatalf("accepted=%d busy=%d", accepted, busy)
		}
		assertMediaAccounting(t, p)
		sbExec(t, p, "update media.capacity set paused=true")
		if _, err := reserve(firstOwner, firstPost, firstPhoto); err != nil {
			t.Fatal("exact retry was blocked", err)
		}
		if _, err := f.rpc(firstOwner, "select public.get_feed_photo_status($1)", firstPost); err != nil {
			t.Fatal(err)
		}
		// Verify the actual HTTP status/header, not just the SQLSTATE.
		data, _ := json.Marshal(map[string]any{"requested_post": domain.UUID(), "requested_photo": domain.UUID(), "requested_bytes": 1024, "requested_mime": "image/jpeg"})
		req, _ := http.NewRequest("POST", os.Getenv("YAP_SUPABASE_LOCAL_URL")+"/rest/v1/rpc/reserve_feed_photo", bytes.NewReader(data))
		req.Header.Set("apikey", os.Getenv("YAP_SUPABASE_LOCAL_ANON_KEY"))
		req.Header.Set("Authorization", "Bearer "+localMediaJWT(t, ids[2]))
		req.Header.Set("Content-Type", "application/json")
		resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 429 || resp.Header.Get("Retry-After") != "15" || !bytes.Contains(body, []byte("media_capacity_exceeded")) {
			t.Fatalf("busy contract: %d header=%q body=%s", resp.StatusCode, resp.Header.Get("Retry-After"), body)
		}
		if _, err := f.rpc(firstOwner, "select to_jsonb(public.cancel_feed_draft($1))", firstPost); err != nil {
			t.Fatal("cancel during overload", err)
		}
		for _, post := range posts {
			sbExec(t, p, "delete from feed.post_drafts where id=$1", post)
		}
		sbExec(t, p, "update media.capacity set paused=false,max_active=256,max_unfinished=120")
		assertMediaAccounting(t, p)
	})
	t.Run("expiry_is_explicit_and_releases_budget", func(t *testing.T) {
		post, id := domain.UUID(), domain.UUID()
		if _, err := reserve(f.author, post, id); err != nil {
			t.Fatal(err)
		}
		sbExec(t, p, "update feed.post_drafts set expires_at=statement_timestamp()-interval '1 second'where id=$1", post)
		sbExec(t, p, "update media.reservations set expires_at=statement_timestamp()-interval '1 second'where photo_id=$1", id)
		_, err := f.rpc(f.author, "select public.confirm_feed_photo($1)", id)
		sbErrorCode(t, err, "PT410")
		raw, err := f.rpc(f.author, "select public.get_feed_photo_status($1)", post)
		if err != nil || !bytes.Contains(raw, []byte(`"expired"`)) {
			t.Fatalf("expired status: %s %v", raw, err)
		}
		sbExec(t, p, "select media.expire_reservations(256)")
		assertMediaAccounting(t, p)
		sbExec(t, p, "delete from feed.post_drafts where id=$1", post)
	})
	t.Run("bytes_temporary_debt_age_and_transaction_rollback", func(t *testing.T) {
		post, id := domain.UUID(), domain.UUID()
		sbExec(t, p, "update media.capacity set max_reserved_bytes=5242880")
		if _, err := f.rpc(f.author, "select public.reserve_feed_photo($1,$2,5242880,'image/jpeg')", post, id); err != nil {
			t.Fatal(err)
		}
		_, err := reserve(f.viewer, domain.UUID(), domain.UUID())
		sbErrorCode(t, err, "PGRST")
		sbExec(t, p, "delete from feed.post_drafts where id=$1", post)
		sbExec(t, p, "update media.capacity set max_reserved_bytes=536870912,max_temporary_bytes=16777216")
		for i := 0; i < 3; i++ {
			sbExec(t, p, "insert into media.cleanup_jobs(bucket,path)values('feed-media',$1)", f.author+"/debt-"+domain.UUID()+"/image.jpg")
		}
		_, err = reserve(f.viewer, domain.UUID(), domain.UUID())
		sbErrorCode(t, err, "PGRST")
		sbExec(t, p, "delete from media.cleanup_jobs where path like $1", f.author+"/%")
		sbExec(t, p, "update media.capacity set max_temporary_bytes=1073741824,max_queue_age_seconds=1")
		post, id = domain.UUID(), domain.UUID()
		if _, err := reserve(f.author, post, id); err != nil {
			t.Fatal(err)
		}
		sbExec(t, p, "update feed.photo_uploads set state='queued'where id=$1", id)
		sbExec(t, p, "update feed.photo_uploads set queued_at=statement_timestamp()-interval '2 seconds'where id=$1", id)
		_, err = reserve(f.viewer, domain.UUID(), domain.UUID())
		sbErrorCode(t, err, "PGRST")
		sbExec(t, p, "delete from feed.post_drafts where id=$1", post)
		sbExec(t, p, "update media.capacity set max_queue_age_seconds=60")
		tx, err := p.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = tx.Exec(ctx, "select set_config('request.jwt.claim.sub',$1,true)", f.viewer); err != nil {
			t.Fatal(err)
		}
		if _, err = tx.Exec(ctx, "set local role authenticated"); err != nil {
			t.Fatal(err)
		}
		if _, err = tx.Exec(ctx, "select public.reserve_feed_photo($1,$2,1024,'image/jpeg')", domain.UUID(), domain.UUID()); err != nil {
			t.Fatal(err)
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		assertMediaAccounting(t, p)
	})
	t.Run("multi_row_expiry_cancel_and_cascade_do_not_deadlock", func(t *testing.T) {
		var posts []string
		for i := 0; i < 9; i++ {
			post := domain.UUID()
			posts = append(posts, post)
			for j := 0; j < 3; j++ {
				if _, err := reserve(ids[i%3], post, domain.UUID()); err != nil {
					t.Fatal(err)
				}
			}
		}
		sbExec(t, p, "update media.reservations set expires_at=statement_timestamp()-interval '1 second'where draft_id=any($1::uuid[])", posts)
		var wg sync.WaitGroup
		for i := 0; i < 6; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				for j := 0; j < 10; j++ {
					var err error
					if i%3 == 0 {
						_, err = p.Exec(ctx, "select media.expire_reservations(4)")
					} else if i%3 == 1 {
						_, err = p.Exec(ctx, "update feed.post_drafts set cancelled_at=clock_timestamp()where id=any($1::uuid[])", posts)
					} else {
						_, err = p.Exec(ctx, "delete from feed.post_drafts where id=any($1::uuid[])", posts)
					}
					if err != nil {
						t.Error(err)
						return
					}
				}
			}(i)
		}
		wg.Wait()
		assertMediaAccounting(t, p)
	})
	t.Run("global_slots_worker_budget_and_stale_registration", func(t *testing.T) {
		a, err := postgres.OpenMediaRepository(ctx, os.Getenv("YAP_SUPABASE_MEDIA_DSN"), 2)
		if err != nil {
			t.Fatal(err)
		}
		defer a.Close()
		b, err := postgres.OpenMediaRepository(ctx, os.Getenv("YAP_SUPABASE_MEDIA_DSN"), 2)
		if err != nil {
			t.Fatal(err)
		}
		defer b.Close()
		c, err := postgres.OpenMediaRepository(ctx, os.Getenv("YAP_SUPABASE_MEDIA_DSN"), 2)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		if err := a.Register(ctx, 1); err != nil {
			t.Fatal(err)
		}
		defer a.Unregister(ctx)
		if err := b.Register(ctx, 1); err != nil {
			t.Fatal(err)
		}
		defer b.Unregister(ctx)
		if err := c.Register(ctx, 1); err == nil {
			t.Fatal("third worker exceeded budget")
		}
		post := domain.UUID()
		var photos []string
		for i := 0; i < 3; i++ {
			id := domain.UUID()
			if _, err := reserve(f.viewer, post, id); err != nil {
				t.Fatal(err)
			}
			photos = append(photos, id)
		}
		sbExec(t, p, "update feed.photo_uploads set state='queued',queued_at=clock_timestamp()where id=any($1::uuid[])", photos)
		j1, ok, err := a.ClaimPhoto(ctx)
		if err != nil || !ok {
			t.Fatal("first slot", err)
		}
		j2, ok, err := b.ClaimPhoto(ctx)
		if err != nil || !ok || j1.ID == j2.ID {
			t.Fatal("second slot", err)
		}
		if _, ok, err := c.ClaimPhoto(ctx); err != nil || ok {
			t.Fatal("global slot exceeded", err)
		}
		// Registration expiry fences completion even while the job lease remains valid.
		sbExec(t, p, "update media.workers set expires_at=statement_timestamp()-interval '1 second'")
		if err := a.Heartbeat(ctx); err == nil {
			t.Fatal("expired registration resurrected")
		}
		if accepted, err := a.CompletePhoto(ctx, j1, application.PreparedPhoto{}); err != nil || accepted {
			t.Fatal("unregistered output accepted", err)
		}
		if err := a.FailPhoto(ctx, j1, true); err != nil {
			t.Fatal(err)
		}
		if err := b.FailPhoto(ctx, j2, true); err != nil {
			t.Fatal(err)
		}
		if _, ok, err := c.ClaimPhoto(ctx); err != nil || !ok {
			t.Fatal("released slot unavailable", err)
		}
		sbExec(t, p, "delete from feed.post_drafts where id=$1", post)
		assertMediaAccounting(t, p)
	})
	t.Run("raw_tables_and_internal_metrics_are_not_client_capabilities", func(t *testing.T) {
		var allowed bool
		err := p.QueryRow(ctx, `select has_table_privilege('authenticated','media.capacity','SELECT')or has_function_privilege('authenticated','media.queue_snapshot()','EXECUTE')or has_function_privilege('anon','media.register_worker(uuid,integer,integer)','EXECUTE')`).Scan(&allowed)
		if err != nil || allowed {
			t.Fatal("client capability leak", err)
		}
	})
}
