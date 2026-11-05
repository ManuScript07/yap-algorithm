package tests

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"yap-algorithm/internal/adapters/postgres"
	"yap-algorithm/internal/application"
	"yap-algorithm/internal/domain"
	"yap-algorithm/internal/model"
	"yap-algorithm/internal/ranking"
)

func stage5Pool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	if os.Getenv("YAP_SUPABASE_INTEGRATION") != "1" {
		t.Skip("local integration required")
	}
	cfg, err := pgxpool.ParseConfig(os.Getenv("YAP_SUPABASE_LOCAL_ADMIN_DSN"))
	if err != nil || cfg.ConnConfig.Host != "127.0.0.1" || cfg.ConnConfig.Port != 54322 || os.Getenv("YAP_SUPABASE_LOCAL_URL") != "http://127.0.0.1:54321" {
		t.Fatal("explicit local Supabase required")
	}
	cfg.MaxConns = 16
	p, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p
}
func stage5Worker(t *testing.T) (*postgres.IndexRepository, *postgres.RecommendationRepository) {
	t.Helper()
	raw, err := os.ReadFile("../.local/supabase-worker.json")
	if err != nil {
		t.Fatal(err)
	}
	var config struct {
		DSN string `json:"database_url"`
	}
	if json.Unmarshal(raw, &config) != nil {
		t.Fatal("worker config")
	}
	parsed, err := url.Parse(config.DSN)
	if err != nil || parsed.Hostname() != "127.0.0.1" || parsed.Port() != "54322" {
		t.Fatal("local worker identity required")
	}
	repo, err := postgres.OpenIndexRepository(context.Background(), config.DSN, sbModel, 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(repo.Close)
	rec, err := repo.Recommendations(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return repo, rec
}
func stage5RPC(t *testing.T, user, name string, args any, want int) map[string]any {
	t.Helper()
	client := soakHTTP{os.Getenv("YAP_SUPABASE_LOCAL_URL"), os.Getenv("YAP_SUPABASE_LOCAL_ANON_KEY"), &http.Client{Timeout: 15 * time.Second}}
	raw, status, err := client.rpc(localMediaJWT(t, user), name, args)
	if err != nil || status != want {
		t.Fatalf("%s status=%d want=%d body=%s error=%v", name, status, want, raw, err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestSupabaseLocalPreparation(t *testing.T) {
	p := stage5Pool(t)
	ctx := context.Background()
	repo, rec := stage5Worker(t)
	t.Run("queue_diagnostics_include_bounded_pool_aggregates", func(t *testing.T) {
		raw, err := rec.Diagnostics(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var snapshot struct {
			Pool struct {
				Max   int     `json:"max"`
				Total int     `json:"total"`
				Wait  float64 `json:"empty_acquire_wait_seconds"`
			} `json:"pool"`
			Index json.RawMessage `json:"index"`
		}
		if json.Unmarshal(raw, &snapshot) != nil || snapshot.Pool.Max != 4 || snapshot.Pool.Total > 4 || snapshot.Pool.Wait < 0 || len(snapshot.Index) == 0 {
			t.Fatal("invalid aggregate diagnostics")
		}
	})
	t.Run("restricted_worker_rejects_unbounded_requests", func(t *testing.T) {
		_, err := repo.ClaimBatch(ctx, "live", 9)
		sbErrorCode(t, err, "22023")
		_, err = repo.EnqueueBackfill(ctx, "", 501)
		sbErrorCode(t, err, "22023")
	})
	t.Run("interactive_refresh_overtakes_cold_requests_and_coalesces", func(t *testing.T) {
		f := sbUsers(t, p)
		f.create(t, "Priority original", "public")
		page := stage5RPC(t, f.viewer, "get_for_you_feed", map[string]int{"requested_limit": 20}, 200)
		if page["versions"].(map[string]any)["features"] != "none" {
			t.Fatal("fallback version mislabeled", page)
		}
		var priority, rows int
		if err := p.QueryRow(ctx, "select priority from recsys.refresh_jobs where user_id=$1", f.viewer).Scan(&priority); err != nil || priority != 1 {
			t.Fatal("cold request was not background", err)
		}
		for i := 0; i < 8; i++ {
			stage5RPC(t, f.viewer, "set_feed_author_followed", map[string]any{"requested_id": domain.UUID(), "requested_author": f.author, "requested_following": i%2 == 0, "expected_version": i}, 200)
		}
		if err := p.QueryRow(ctx, "select count(*),min(priority)from recsys.refresh_jobs where user_id=$1", f.viewer).Scan(&rows, &priority); err != nil || rows != 1 || priority != 0 {
			t.Fatal("interactive changes were not coalesced/promoted", err)
		}
		job, ok, err := rec.Lane(0).ClaimRefresh(ctx)
		if err != nil || !ok || job.UserID != f.viewer {
			t.Fatalf("interactive lane: %v %v %v", job, ok, err)
		}
		if _, ok, err := rec.Lane(1).ClaimRefresh(ctx); err != nil || ok {
			t.Fatal("background lane claimed an interactive lease", err)
		}
		if err := rec.RetryRefresh(ctx, job); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("batch_completion_fences_edits_and_backfill_is_idempotent", func(t *testing.T) {
		f := sbUsers(t, p)
		post := f.create(t, "Before batch edit", "public")
		sbExec(t, p, "update recsys.index_jobs set job_kind='backfill',available_at='2000-01-01'where post_id=$1", post)
		jobs, err := repo.ClaimBatch(ctx, "backfill", 1)
		if err != nil || len(jobs) != 1 || jobs[0].PostID != post {
			t.Fatal("batch claim", err, jobs)
		}
		if _, err := f.rpc(f.author, "select public.edit_feed_post($1,1,'After batch edit')", post); err != nil {
			t.Fatal(err)
		}
		if err := repo.CompleteBatch(ctx, []application.IndexCompletion{{Job: jobs[0], Vector: sbVector()}}); err != nil {
			t.Fatal(err)
		}
		var count int
		if err := p.QueryRow(ctx, "select count(*)from recsys.post_embeddings where post_id=$1", post).Scan(&count); err != nil || count != 0 {
			t.Fatal("old batch wrote obsolete vector", err)
		}
		sbExec(t, p, "update recsys.index_jobs set available_at='2000-01-01'where post_id=$1 and post_version=2", post)
		current, err := repo.ClaimBatch(ctx, "live", 1)
		if err != nil || len(current) != 1 || current[0].Version != 2 {
			t.Fatal("edit did not retain live priority", err, current)
		}
		if err := repo.CompleteBatch(ctx, []application.IndexCompletion{{Job: current[0], Vector: sbVector()}}); err != nil {
			t.Fatal(err)
		}
		// A stale lease is harmless even if it arrives after a newer completion.
		if err := repo.CompleteBatch(ctx, []application.IndexCompletion{{Job: jobs[0], Vector: sbVector()}}); err != nil {
			t.Fatal(err)
		}
		var version int
		if err := p.QueryRow(ctx, "select post_version from recsys.post_embeddings where post_id=$1", post).Scan(&version); err != nil || version != 2 {
			t.Fatal("late result replaced current vector", err)
		}
		sbExec(t, p, "delete from recsys.post_embeddings where post_id=$1", post)
		for i := 0; i < 2; i++ {
			if _, err := repo.EnqueueBackfill(ctx, "", 500); err != nil {
				t.Fatal(err)
			}
		}
		var kind string
		if err := p.QueryRow(ctx, "select job_kind from recsys.index_jobs where post_id=$1 and post_version=2 and done_at is null", post).Scan(&kind); err != nil || kind != "backfill" {
			t.Fatal("missing vector did not reenter bounded backfill", err)
		}
		if err := p.QueryRow(ctx, "select count(*)from recsys.index_jobs where post_id=$1 and post_version=2", post).Scan(&count); err != nil || count != 1 {
			t.Fatal("duplicate backfill job", err)
		}
	})
	t.Run("model_failure_retries_every_claimed_job_without_partial_vectors", func(t *testing.T) {
		f := sbUsers(t, p)
		a := f.create(t, "Batch outage one", "public")
		b := f.create(t, "Batch outage two", "public")
		sbExec(t, p, "update recsys.index_jobs set job_kind='backfill',available_at='1900-01-01'where post_id=any($1::uuid[])", []string{a, b})
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }))
		defer server.Close()
		processor := application.BatchIndexer{Repository: repo, Encoder: model.New(server.URL, sbModel), Kind: "backfill", Size: 2}
		if worked, err := processor.Once(ctx); !worked || err == nil {
			t.Fatal("model outage was ignored")
		}
		var retry, embeddings int
		if err := p.QueryRow(ctx, "select count(*)from recsys.index_jobs where post_id=any($1::uuid[])and lease is null and attempts=1 and last_error='processing_failed'and available_at>statement_timestamp()", []string{a, b}).Scan(&retry); err != nil || retry != 2 {
			t.Fatal("batch lost retries", err, retry)
		}
		if err := p.QueryRow(ctx, "select count(*)from recsys.post_embeddings where post_id=any($1::uuid[])", []string{a, b}).Scan(&embeddings); err != nil || embeddings != 0 {
			t.Fatal("partial vectors after outage", err)
		}
	})
	t.Run("obsolete_embedding_space_cannot_replace_current_vector", func(t *testing.T) {
		f := sbUsers(t, p)
		post := f.create(t, "Embedding revision fence", "public")
		sbExec(t, p, "insert into recsys.post_embeddings(post_id,post_version,model,embedding)values($1,1,$2,$3::text::extensions.vector)", post, sbModel, "[1,"+strings.Repeat("0,", 382)+"0]")
		lease := domain.UUID()
		var id int64
		if err := p.QueryRow(ctx, "insert into recsys.index_jobs(post_id,post_version,model,lease,locked_until)values($1,1,'retired-embedding-space',$2,statement_timestamp()+interval '1 minute')returning id", post, lease).Scan(&id); err != nil {
			t.Fatal(err)
		}
		if err := repo.CompleteBatch(ctx, []application.IndexCompletion{{Job: application.IndexJob{ID: id, Lease: lease}, Vector: sbVector()}}); err != nil {
			t.Fatal(err)
		}
		var space string
		if err := p.QueryRow(ctx, "select model from recsys.post_embeddings where post_id=$1", post).Scan(&space); err != nil || space != sbModel {
			t.Fatal("obsolete space replaced active vector", err)
		}
	})
	t.Run("discovery_candidates_use_author_coverage_across_posts", func(t *testing.T) {
		f, g := sbUsers(t, p), sbUsers(t, p)
		established := f.create(t, "New post by widely seen author", "public")
		underexposed := g.create(t, "Some repeat views of a little known author", "public")
		sbExec(t, p, "update recsys.author_statistics set impressions=200,viewers=20 where author_id=$1", f.author)
		sbExec(t, p, "update recsys.post_statistics set impressions=40 where post_id=$1", underexposed)
		stage5RPC(t, f.viewer, "get_for_you_feed", map[string]any{}, 200)
		sbExec(t, p, "update recsys.refresh_jobs set available_at=statement_timestamp()where user_id=$1", f.viewer)
		job, ok, err := rec.ClaimRefresh(ctx)
		if err != nil || !ok {
			t.Fatal("discovery claim", err)
		}
		input, err := rec.LoadProfile(ctx, job)
		if err != nil {
			t.Fatal(err)
		}
		candidates, err := rec.LoadCandidates(ctx, job, input, nil)
		if err != nil {
			t.Fatal(err)
		}
		seen := 0
		for _, candidate := range candidates {
			if candidate.ID == established {
				seen++
				if slices.Contains(candidate.Sources, "discovery") {
					t.Error("new post reset established author coverage")
				}
			}
			if candidate.ID == underexposed {
				seen++
				if !slices.Contains(candidate.Sources, "discovery") {
					t.Error("individual post views excluded an underexposed author")
				}
			}
		}
		if seen != 2 {
			t.Fatal("discovery fixtures missing", seen)
		}
	})
	t.Run("parallel_reversed_author_events_preserve_reach_without_deadlock", func(t *testing.T) {
		f, g := sbUsers(t, p), sbUsers(t, p)
		a := f.create(t, "First coverage author", "public")
		b := g.create(t, "Second coverage author", "public")
		users := []string{f.viewer, g.viewer}
		batches := make([][]any, 2)
		for i, user := range users {
			for _, post := range []string{a, b} {
				receipt := sbDelivery(t, f, user, post)
				sbExec(t, p, "update recsys.deliveries set delivered_at=statement_timestamp()-interval '3 seconds'where id=$1", receipt)
				batches[i] = append(batches[i], map[string]any{"event_id": domain.UUID(), "delivery_id": receipt, "kind": "impression", "visible_ms": 1000, "visible_fraction": 0.75})
			}
		}
		batches[1][0], batches[1][1] = batches[1][1], batches[1][0]
		client := soakHTTP{os.Getenv("YAP_SUPABASE_LOCAL_URL"), os.Getenv("YAP_SUPABASE_LOCAL_ANON_KEY"), &http.Client{Timeout: 10 * time.Second}}
		tokens := []string{localMediaJWT(t, users[0]), localMediaJWT(t, users[1])}
		var group sync.WaitGroup
		for i := range users {
			group.Add(1)
			go func(i int) {
				defer group.Done()
				_, status, err := client.rpc(tokens[i], "record_feed_events", map[string]any{"requested_events": batches[i]})
				if err != nil || status != 200 {
					t.Errorf("concurrent events status=%d err=%v", status, err)
				}
			}(i)
		}
		group.Wait()
		var correct int
		if err := p.QueryRow(ctx, "select count(*)from recsys.author_statistics where author_id=any($1::uuid[])and impressions=2 and viewers=2", []string{f.author, g.author}).Scan(&correct); err != nil || correct != 2 {
			t.Fatal("concurrent author counters lost events", correct, err)
		}
	})
	t.Run("snapshot_versions_stale_lease_and_atomic_switch", func(t *testing.T) {
		f := sbUsers(t, p)
		post := f.create(t, "Versioned snapshot source", "public")
		stage5RPC(t, f.viewer, "set_feed_author_followed", map[string]any{"requested_id": domain.UUID(), "requested_author": f.author, "requested_following": true, "expected_version": 0}, 200)
		sbExec(t, p, "insert into recsys.post_embeddings(post_id,post_version,model,embedding)values($1,1,$2,$3::text::extensions.vector)", post, sbModel, "[1,"+strings.Repeat("0,", 382)+"0]")
		job, ok, err := rec.ClaimRefresh(ctx)
		if err != nil || !ok || job.UserID != f.viewer {
			t.Fatal("snapshot claim", err, job)
		}
		input, err := rec.LoadProfile(ctx, job)
		if err != nil {
			t.Fatal(err)
		}
		profile := ranking.BuildProfile(input.Evidence, input.At)
		candidates, err := rec.LoadCandidates(ctx, job, input, profile)
		if err != nil {
			t.Fatal(err)
		}
		ranking.Score(candidates, profile, input.At)
		candidates = ranking.SelectForYou(candidates)
		result := application.RecommendationResult{Input: input, Profile: profile, Candidates: candidates, FeatureVersion: application.FeatureVersion, PolicyVersion: "obsolete-policy"}
		_, err = rec.Publish(ctx, job, result)
		sbErrorCode(t, err, "22023")
		result.PolicyVersion = application.PolicyVersion
		sbExec(t, p, "update recsys.refresh_jobs set locked_until=statement_timestamp()-interval '1 second'where user_id=$1", f.viewer)
		if accepted, err := rec.Publish(ctx, job, result); err != nil || accepted {
			t.Fatal("expired lease published", err)
		}
		next, ok, err := rec.ClaimRefresh(ctx)
		if err != nil || !ok {
			t.Fatal(err)
		}
		if accepted, err := rec.Publish(ctx, next, result); err != nil || !accepted {
			t.Fatal("current result rejected", err)
		}
		var features, policy, space string
		if err := p.QueryRow(ctx, "select s.feature_version,s.policy_version,s.embedding_space from recsys.user_states u join recsys.feed_snapshots s on s.id=u.current_snapshot where u.user_id=$1", f.viewer).Scan(&features, &policy, &space); err != nil || features != application.FeatureVersion || policy != application.PolicyVersion || space != sbModel {
			t.Fatal("snapshot provenance lost", err)
		}
		page := stage5RPC(t, f.viewer, "get_for_you_feed", map[string]any{}, 200)
		if page["versions"].(map[string]any)["policy"] != application.PolicyVersion {
			t.Fatal(page)
		}
		var receiptFeature string
		if err := p.QueryRow(ctx, "select feature_version from recsys.deliveries where user_id=$1 and surface='for_you'limit 1", f.viewer).Scan(&receiptFeature); err != nil || receiptFeature != application.FeatureVersion {
			t.Fatal("delivery lost producing feature version", err)
		}
	})
	t.Run("qualified_author_reach_is_not_delivery_or_duplicate_events", func(t *testing.T) {
		f := sbUsers(t, p)
		post := f.create(t, "Actual coverage source", "public")
		page := stage5RPC(t, f.viewer, "get_feed_post", map[string]any{"requested_id": post}, 200)
		var views, reach int
		read := func() {
			t.Helper()
			if err := p.QueryRow(ctx, "select impressions,viewers from recsys.author_statistics where author_id=$1", f.author).Scan(&views, &reach); err != nil {
				t.Fatal(err)
			}
		}
		read()
		if views != 0 || reach != 0 {
			t.Fatal("delivery counted as attention")
		}
		receipt := page["delivery_id"].(string)
		sbExec(t, p, "update recsys.deliveries set delivered_at=statement_timestamp()-interval '3 seconds'where id=$1", receipt)
		event := map[string]any{"event_id": domain.UUID(), "delivery_id": receipt, "kind": "impression", "visible_ms": 1000, "visible_fraction": 0.75}
		for i := 0; i < 2; i++ {
			stage5RPC(t, f.viewer, "record_feed_events", map[string]any{"requested_events": []any{event}}, 200)
		}
		read()
		if views != 1 || reach != 1 {
			t.Fatalf("qualified/retry reach: views=%d reach=%d", views, reach)
		}
		event["event_id"] = domain.UUID()
		stage5RPC(t, f.viewer, "record_feed_events", map[string]any{"requested_events": []any{event}}, 200)
		read()
		if views != 1 || reach != 1 {
			t.Fatal("repeat same-day impression inflated coverage")
		}
		another := f.create(t, "Same author's second post", "public")
		page = stage5RPC(t, f.viewer, "get_feed_post", map[string]any{"requested_id": another}, 200)
		sbExec(t, p, "update recsys.deliveries set delivered_at=statement_timestamp()-interval '3 seconds'where id=$1", page["delivery_id"])
		event = map[string]any{"event_id": domain.UUID(), "delivery_id": page["delivery_id"], "kind": "impression", "visible_ms": 1000, "visible_fraction": 0.75}
		stage5RPC(t, f.viewer, "record_feed_events", map[string]any{"requested_events": []any{event}}, 200)
		read()
		if views != 2 || reach != 1 {
			t.Fatal("author coverage reset for another post", views, reach)
		}
	})
	t.Run("real_batch_model_contract", func(t *testing.T) {
		if os.Getenv("YAP_SUPABASE_MODEL_TEST") != "1" {
			t.Skip("enable -WithModel")
		}
		encoder := model.New("http://127.0.0.1:8091", sbModel)
		vectors, err := encoder.EncodeBatch(ctx, []string{"Программирование на Go и базы данных", "Travel and new places", "Программирование на Go и базы данных"})
		if err != nil || len(vectors) != 3 {
			t.Fatal("real ONNX batch", err)
		}
		if domain.Cosine(vectors[0], vectors[2]) < .99999 {
			t.Fatal("duplicate texts changed batch mapping")
		}
		for _, vector := range vectors {
			if len(vector) != 384 {
				t.Fatal("space mismatch")
			}
		}
	})
}
