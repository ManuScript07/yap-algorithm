package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	"yap-algorithm/internal/domain"
)

// Exercises the shipped worker binary, restricted DB identity and real ONNX
// model while cold requests and old content compete with interactive feedback.
func TestSupabaseLocalPreparationLoad(t *testing.T) {
	if os.Getenv("YAP_SUPABASE_LOAD_TEST") != "1" || os.Getenv("YAP_SUPABASE_MODEL_TEST") != "1" {
		t.Skip("enable -WithLoad -WithModel")
	}
	p := stage5Pool(t)
	ctx := context.Background()
	f := sbUsers(t, p)
	rows, err := p.Query(ctx, `insert into auth.users(id,is_sso_user,is_anonymous,raw_user_meta_data)
 select extensions.gen_random_uuid(),false,false,'{"name":"Preparation load fixture"}' from generate_series(1,156) returning id::text`)
	if err != nil {
		t.Fatal(err)
	}
	var users []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		users = append(users, id)
	}
	rows.Close()
	if rows.Err() != nil {
		t.Fatal(rows.Err())
	}
	t.Cleanup(func() {
		sbExec(t, p, "delete from public.profiles where id=any($1::uuid[])", users)
		sbExec(t, p, "delete from auth.users where id=any($1::uuid[])", users)
	})
	readers, authors, cold := users[:12], users[12:108], users[108:]
	// Synthetic bulk insertion deliberately bypasses the product's 12/hour quota.
	sbExec(t, p, `insert into feed.posts(id,author_id,text,audience,language,creation_request,latitude,longitude)
 select extensions.gen_random_uuid(),($1::uuid[])[1+(n-1)%96],
 'Travel, music and programming discussion number '||n,'public','en','[]',55.0,37.0 from generate_series(1,2000)n`, authors)
	sbExec(t, p, "update recsys.index_jobs set job_kind='backfill'where post_id in(select id from feed.posts where author_id=any($1::uuid[]))", authors)
	sbExec(t, p, "insert into public.user_locations(user_id,latitude,longitude,updated_at)select unnest($1::uuid[]),55,37,statement_timestamp()", readers)
	for _, user := range append(append([]string{}, readers...), cold...) {
		stage5RPC(t, user, "get_for_you_feed", map[string]int{"requested_limit": 10}, 200)
	}
	target := f.create(t, "Программирование на Go, безопасные базы данных и распределённые системы", "public")
	startPreparationWorker(t, "http://127.0.0.1:8091", "../.local/stage5-load-worker.log")
	wait := func(deadline time.Time, query string, args ...any) {
		t.Helper()
		for time.Now().Before(deadline) {
			var ready bool
			if err := p.QueryRow(ctx, query, args...).Scan(&ready); err != nil {
				t.Fatal(err)
			}
			if ready {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatal("preparation did not meet its bounded deadline; inspect private worker log")
	}
	wait(time.Now().Add(40*time.Second), "select exists(select 1 from recsys.post_embeddings where post_id=$1)", target)
	client := soakHTTP{os.Getenv("YAP_SUPABASE_LOCAL_URL"), os.Getenv("YAP_SUPABASE_LOCAL_ANON_KEY"), &http.Client{Timeout: 10 * time.Second}}
	tokens := make([]string, len(readers))
	deliveries := make([]string, len(readers))
	for i, user := range readers {
		tokens[i] = localMediaJWT(t, user)
		deliveries[i] = stage5RPC(t, user, "get_feed_post", map[string]any{"requested_id": target}, 200)["delivery_id"].(string)
	}
	var group sync.WaitGroup
	defer group.Wait()
	var mu sync.Mutex
	var latencies []time.Duration
	bySurface := make(map[string][]time.Duration)
	errors := make(chan error, 510)
	start := time.Now()
	var memoryBefore runtime.MemStats
	runtime.ReadMemStats(&memoryBefore)
	if os.Getenv("YAP_FEED_TRACE") == "1" {
		client.client.Transport = preparationTraceTransport{t, start}
		stopTrace := tracePreparationWaits(t, p)
		defer stopTrace()
	}
	// 500 arrivals/minute, bounded to eight simultaneous HTTP requests.
	group.Add(1)
	go func() {
		defer group.Done()
		ticker := time.NewTicker(120 * time.Millisecond)
		defer ticker.Stop()
		semaphore := make(chan struct{}, 8)
		var reads sync.WaitGroup
		for i := 0; i < 500; i++ {
			<-ticker.C
			semaphore <- struct{}{}
			reads.Add(1)
			go func(i int) {
				defer reads.Done()
				defer func() { <-semaphore }()
				name := []string{"get_for_you_feed", "get_friends_feed", "get_nearby_feed"}[i%3]
				before := time.Now()
				// Rotate the reader assignment each round: every reader visits all
				// three tabs instead of i%12 permanently assigning one tab to them.
				reader := (i + i/len(readers)) % len(readers)
				body, status, err := client.rpc(tokens[reader], name, map[string]int{"requested_limit": 10})
				mu.Lock()
				latencies = append(latencies, time.Since(before))
				bySurface[name] = append(bySurface[name], time.Since(before))
				mu.Unlock()
				if err != nil || status != 200 {
					errors <- fmt.Errorf("mixed read %s: status=%d error=%v", name, status, err)
					return
				}
				var page map[string]any
				if json.Unmarshal(body, &page) != nil || page["items"] == nil {
					errors <- fmt.Errorf("malformed %s page", name)
				}
			}(i)
		}
		reads.Wait()
	}()
	// Independent actions, distributed across readers. Every probe waits for a
	// complete revision AND a real semantic interest, not just an empty snapshot.
	var effects []time.Duration
	for i, user := range readers {
		before := time.Now()
		body, status, err := client.rpc(tokens[i], "apply_feed_action", map[string]any{"requested_id": domain.UUID(), "requested_delivery": deliveries[i], "requested_action": "like", "expected_version": 0})
		if err != nil || status != 200 {
			t.Fatalf("feedback status=%d err=%v", status, err)
		}
		var action map[string]any
		if json.Unmarshal(body, &action) != nil {
			t.Fatal("malformed action")
		}
		var revision int64
		if err := p.QueryRow(ctx, "select revision from recsys.user_states where user_id=$1", user).Scan(&revision); err != nil {
			t.Fatal(err)
		}
		wait(before.Add(30*time.Second), `select exists(select 1 from recsys.user_states s where user_id=$1 and processed_revision>=$2 and profile_complete
 and exists(select 1 from recsys.interest_clusters c where c.user_id=s.user_id and not negative)
 and exists(select 1 from recsys.feed_snapshots f where f.id=s.current_snapshot and f.feature_version='feed-features-v2'))`, user, revision)
		effects = append(effects, time.Since(before))
	}
	group.Wait()
	var memoryAfter runtime.MemStats
	runtime.ReadMemStats(&memoryAfter)
	t.Logf("load client runtime: heap_start=%d heap_end=%d sys=%d GC_cycles=%d GC_pause_total=%s", memoryBefore.HeapAlloc, memoryAfter.HeapAlloc, memoryAfter.Sys, memoryAfter.NumGC-memoryBefore.NumGC, time.Duration(memoryAfter.PauseTotalNs-memoryBefore.PauseTotalNs))
	close(errors)
	for err := range errors {
		t.Error(err)
	}
	var backfillDone, scheduledDone int
	if err := p.QueryRow(ctx, `select count(*)from recsys.index_jobs j join feed.posts p on p.id=j.post_id where p.author_id=any($1::uuid[])and j.done_at is not null`, authors).Scan(&backfillDone); err != nil {
		t.Fatal(err)
	}
	if err := p.QueryRow(ctx, "select count(*)from recsys.user_states where user_id=any($1::uuid[])and prepared_at is not null", cold).Scan(&scheduledDone); err != nil {
		t.Fatal(err)
	}
	if backfillDone == 0 || scheduledDone == 0 {
		t.Fatalf("starved background lane: backfill=%d scheduled=%d", backfillDone, scheduledDone)
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	sort.Slice(effects, func(i, j int) bool { return effects[i] < effects[j] })
	for _, name := range []string{"get_for_you_feed", "get_friends_feed", "get_nearby_feed"} {
		samples := bySurface[name]
		millis := make([]float64, len(samples))
		for i, sample := range samples {
			millis[i] = float64(sample) / float64(time.Millisecond)
		}
		summary := summarizeFeedTiming(millis)
		t.Logf("%s first-page HTTP n=%d ms p50=%.2f p95=%.2f p99=%.2f", name, summary.Count, summary.P50, summary.P95, summary.P99)
		if summary.Count == 0 || summary.P95 > 300 || summary.P99 > 800 {
			t.Errorf("%s missed minimum-tier per-surface latency budget", name)
		}
	}
	t.Logf("real E5 + shipped worker/2 tasks/batch4: 2000 queued backfill posts,48 cold readers; 500 mixed HTTP reads in %s p95=%s p99=%s; action-to-complete-semantic-snapshot n=%d p95=%s max=%s; backfill completed=%d scheduled users=%d", time.Since(start), latencies[474], latencies[494], len(effects), effects[11], effects[11], backfillDone, scheduledDone)
	if time.Since(start) > 75*time.Second {
		t.Error("mixed read latency/arrival target missed")
	}
}
