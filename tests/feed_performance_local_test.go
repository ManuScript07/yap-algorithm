package tests

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"yap-algorithm/internal/application"
	"yap-algorithm/internal/domain"
)

type feedPerfSample struct {
	RPC, Page           string
	HTTPMS, AdmissionMS float64
	Status              int
	ErrorCode           string `json:"error_code,omitempty"`
	TransportError      bool   `json:"transport_error,omitempty"`
	Timeout             bool   `json:"timeout,omitempty"`
}
type feedPerfSummary struct {
	Count              int `json:"count"`
	P50, P95, P99, Max float64
}

func summarizeFeedTiming(values []float64) feedPerfSummary {
	if len(values) == 0 {
		return feedPerfSummary{}
	}
	ordered := append([]float64(nil), values...)
	sort.Float64s(ordered)
	percentile := func(p int) float64 { return ordered[(len(ordered)*p+99)/100-1] }
	return feedPerfSummary{len(ordered), percentile(50), percentile(95), percentile(99), ordered[len(ordered)-1]}
}

// Opt-in HTTP benchmark, separate from the correctness/race suite. Compare modes
// with identical fixture parameters; fake inference measures worker SQL/locks,
// never embedding quality or real model throughput.
func TestSupabaseLocalFeedPerformance(t *testing.T) {
	mode := os.Getenv("YAP_FEED_PERF_MODE")
	if mode == "" || os.Getenv("YAP_SUPABASE_LOAD_TEST") != "1" {
		t.Skip("use scripts/benchmark-feed-local.ps1")
	}
	if mode != "off" && mode != "worker" && mode != "inference" {
		t.Fatal("unknown performance mode")
	}
	if mode == "inference" && os.Getenv("YAP_SUPABASE_MODEL_TEST") != "1" {
		t.Fatal("real model required")
	}
	label := os.Getenv("YAP_FEED_PERF_LABEL")
	if ok, _ := regexp.MatchString(`^[a-zA-Z0-9-]{1,48}$`, label); !ok {
		t.Fatal("safe benchmark label required")
	}
	readInt := func(name string, min, max int) int {
		t.Helper()
		v, e := strconv.Atoi(os.Getenv(name))
		if e != nil || v < min || v > max {
			t.Fatalf("invalid %s", name)
		}
		return v
	}
	rate, seconds := readInt("YAP_FEED_PERF_RATE", 500, 3000), readInt("YAP_FEED_PERF_SECONDS", 10, 180)
	pageSize := readInt("YAP_FEED_PERF_PAGE_SIZE", 1, 30)
	p := stage5Pool(t)
	ctx := context.Background()
	var existingWorkers int
	if err := p.QueryRow(ctx, "select count(*)from pg_stat_activity where usename='yap_recsys_local_worker'").Scan(&existingWorkers); err != nil || existingWorkers != 0 {
		t.Fatal("stop other local recommendation workers before comparative benchmarks")
	}
	rows, err := p.Query(ctx, `insert into auth.users(id,is_sso_user,is_anonymous,raw_user_meta_data)
 select extensions.gen_random_uuid(),false,false,'{"name":"Feed performance fixture"}' from generate_series(1,156) returning id::text`)
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
	readers, authors := users[:60], users[60:]
	sbExec(t, p, `insert into feed.posts(id,author_id,text,audience,language,creation_request,latitude,longitude)
 select extensions.gen_random_uuid(),($1::uuid[])[1+(n-1)%96],'Read benchmark post '||n,'public','en','[]',55,37 from generate_series(1,2000)n`, authors)
	vector, err := domain.Vector(sbVector())
	if err != nil {
		t.Fatal(err)
	}
	sbExec(t, p, `insert into recsys.post_embeddings(post_id,post_version,model,embedding)select id,version,$2,$3::text::extensions.vector from feed.posts where author_id=any($1::uuid[])`, authors, sbModel, vector)
	sbExec(t, p, "update recsys.index_jobs set done_at=statement_timestamp()where post_id in(select id from feed.posts where author_id=any($1::uuid[]))", authors)
	sbExec(t, p, "insert into public.user_locations(user_id,latitude,longitude,updated_at)select unnest($1::uuid[]),55,37,statement_timestamp()", readers)
	sbExec(t, p, "insert into feed.author_follows(follower_id,author_id,following,version)select u,a,true,1 from unnest($1::uuid[])u cross join unnest($2::uuid[])a", readers, authors[:8])
	_, rec := stage5Worker(t)
	client := soakHTTP{os.Getenv("YAP_SUPABASE_LOCAL_URL"), os.Getenv("YAP_SUPABASE_LOCAL_ANON_KEY"), &http.Client{Timeout: 10 * time.Second}}
	tokens := make([]string, len(readers))
	var fallbackMS []float64
	for i, user := range readers {
		tokens[i] = localMediaJWT(t, user)
		before := time.Now()
		body, status, err := client.rpc(tokens[i], "get_for_you_feed", map[string]int{"requested_limit": pageSize})
		fallbackMS = append(fallbackMS, float64(time.Since(before))/float64(time.Millisecond))
		var page sbRecommendationPage
		if err != nil || status != 200 || json.Unmarshal(body, &page) != nil || page.Model != "fallback-v1" || len(page.Items) != pageSize {
			t.Fatalf("cold fallback status=%d error=%v", status, err)
		}
	}
	sbExec(t, p, "update recsys.refresh_jobs set available_at=statement_timestamp()where user_id=any($1::uuid[])", readers)
	for i := 0; i < 120; i++ {
		worked, err := (application.Personalizer{Repository: rec}).Once(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !worked {
			break
		}
	}
	var prepared int
	if err := p.QueryRow(ctx, "select count(*)from recsys.user_states where user_id=any($1::uuid[])and prepared_at is not null and profile_complete", readers).Scan(&prepared); err != nil || prepared != 60 {
		t.Fatalf("warm fixtures prepared=%d err=%v", prepared, err)
	}
	// Same backlog in all modes, older than the foreground corpus. Technical
	// warm vectors are constant: this benchmark does not evaluate model quality.
	sbExec(t, p, `insert into feed.posts(id,author_id,text,audience,language,creation_request,created_at,latitude,longitude)
 select extensions.gen_random_uuid(),($1::uuid[])[1+(n-1)%96],'Unique background reading, travel and software sample '||n||' run '||$2::text,'public','en','[]',statement_timestamp()-interval '2 hours',55,37 from generate_series(1,2000)n`, authors, domain.UUID())
	sbExec(t, p, "update recsys.index_jobs set job_kind='backfill'where done_at is null and post_id in(select id from feed.posts where author_id=any($1::uuid[]))", authors)
	output := fmt.Sprintf("feed-perf-%s-%s-%d-%ds", label, mode, rate, seconds)
	if pageSize != 10 {
		output += fmt.Sprintf("-p%d", pageSize)
	}
	if mode != "off" {
		modelURL := "http://127.0.0.1:8091"
		if mode == "worker" {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body struct {
					Texts []string `json:"texts"`
				}
				if r.URL.Path != "/embed-batch" || json.NewDecoder(r.Body).Decode(&body) != nil || len(body.Texts) < 1 || len(body.Texts) > 8 {
					http.Error(w, "invalid", 400)
					return
				}
				vectors := make([][]float64, len(body.Texts))
				for i := range vectors {
					vectors[i] = sbVector()
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{"revision": sbModel, "embeddings": vectors})
			}))
			t.Cleanup(server.Close)
			modelURL = server.URL
		}
		startPreparationWorker(t, modelURL, filepath.Join("..", ".local", output+"-worker.log"))
	}
	names := []string{"get_for_you_feed", "get_friends_feed", "get_nearby_feed"}
	type slot struct {
		sync.Mutex
		sequence  int
		first     sbRecommendationPage
		seenFirst map[string]bool
	}
	slots := make([]slot, len(readers)*3)
	type job struct {
		index int
		due   time.Time
	}
	jobs := make(chan job, 128)
	var group sync.WaitGroup
	var mu sync.Mutex
	var samples []feedPerfSample
	var failures []string
	for worker := 0; worker < 16; worker++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for work := range jobs {
				reader := work.index % len(readers)
				surface := (work.index/len(readers) + work.index) % 3
				s := &slots[reader*3+surface]
				s.Lock()
				kind := []string{"first", "next", "replay"}[s.sequence%3]
				args := map[string]any{"requested_limit": pageSize}
				if kind != "first" {
					args["requested_session"] = s.first.Session
					args["after_position"] = -1
					if kind == "next" && s.first.Next != nil {
						args["after_position"] = s.first.Next.After
					}
				}
				before := time.Now()
				admission := before.Sub(work.due)
				body, status, err := client.rpc(tokens[reader], names[surface], args)
				elapsed := time.Since(before)
				var page sbRecommendationPage
				problem := ""
				var rpcError struct {
					Code string `json:"code"`
				}
				if status != 200 {
					_ = json.Unmarshal(body, &rpcError)
				}
				var netError net.Error
				timedOut := errors.As(err, &netError) && netError.Timeout()
				if err != nil || status != 200 {
					problem = fmt.Sprintf("%s/%s status=%d transport=%v", names[surface], kind, status, err)
				} else if json.Unmarshal(body, &page) != nil || len(page.Items) != pageSize {
					problem = fmt.Sprintf("%s/%s malformed or unfilled page", names[surface], kind)
				} else if surface == 0 && page.Model != "personalized-v2" {
					problem = "unexpected fallback in prepared benchmark"
				} else {
					if kind == "first" {
						s.first = page
						s.seenFirst = map[string]bool{}
						for _, item := range page.Items {
							s.seenFirst[item.Post.ID] = true
						}
						if page.Next == nil {
							problem = "missing continuation"
						}
					}
					if kind == "next" {
						for _, item := range page.Items {
							if s.seenFirst[item.Post.ID] {
								problem = "pagination repeated an issued post"
							}
						}
					}
					if kind == "replay" {
						for i, item := range page.Items {
							if item.Post.ID != s.first.Items[i].Post.ID || item.Delivery != s.first.Items[i].Delivery {
								problem = "replayed page changed IDs or receipts"
							}
						}
					}
				}
				s.sequence++
				s.Unlock()
				mu.Lock()
				samples = append(samples, feedPerfSample{RPC: names[surface], Page: kind, HTTPMS: float64(elapsed) / float64(time.Millisecond), AdmissionMS: float64(admission) / float64(time.Millisecond), Status: status, ErrorCode: rpcError.Code, TransportError: err != nil, Timeout: timedOut})
				if problem != "" {
					failures = append(failures, problem)
				}
				mu.Unlock()
			}
		}()
	}
	// Samples waits rather than resetting global pg_stat_* or changing server
	// instrumentation; only aggregate active counts, never raw SQL/user content.
	monitorCtx, cancelMonitor := context.WithCancel(ctx)
	var monitor sync.WaitGroup
	maxLocks, maxActive, monitorSamples, monitorErrors := 0, 0, 0, 0
	monitor.Add(1)
	go func() {
		defer monitor.Done()
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-monitorCtx.Done():
				return
			case <-ticker.C:
				var locks, active int
				if err := p.QueryRow(monitorCtx, "select count(*)filter(where wait_event_type='Lock'),count(*)from pg_stat_activity where state='active'and usename in('authenticator','yap_recsys_local_worker')").Scan(&locks, &active); err == nil {
					maxLocks = max(maxLocks, locks)
					maxActive = max(maxActive, active)
					monitorSamples++
				} else if monitorCtx.Err() == nil {
					monitorErrors++
				}
			}
		}
	}()
	start := time.Now()
	count := rate * seconds / 60
	dropped := 0
	interval := time.Minute / time.Duration(rate)
	ticker := time.NewTicker(interval)
	for i := 0; i < count; i++ {
		due := <-ticker.C
		select {
		case jobs <- job{i, due}:
		default:
			dropped++
		}
	}
	ticker.Stop()
	close(jobs)
	group.Wait()
	cancelMonitor()
	monitor.Wait()
	elapsed := time.Since(start)
	aggregates := map[string]feedPerfSummary{}
	endToEnd := map[string]feedPerfSummary{}
	latencyBudgetMet := true
	for _, name := range names {
		var httpTimes []float64
		var totalTimes []float64
		for _, sample := range samples {
			if sample.RPC == name {
				httpTimes = append(httpTimes, sample.HTTPMS)
				totalTimes = append(totalTimes, sample.HTTPMS+sample.AdmissionMS)
			}
		}
		aggregates[name] = summarizeFeedTiming(httpTimes)
		endToEnd[name] = summarizeFeedTiming(totalTimes)
		latencyBudgetMet = latencyBudgetMet && aggregates[name].P95 <= 300 && aggregates[name].P99 <= 800
		for _, kind := range []string{"first", "next", "replay"} {
			var times []float64
			for _, sample := range samples {
				if sample.RPC == name && sample.Page == kind {
					times = append(times, sample.HTTPMS)
				}
			}
			aggregates[name+"/"+kind] = summarizeFeedTiming(times)
		}
	}
	var admission []float64
	outcomes := map[string]map[string]int{}
	for _, sample := range samples {
		admission = append(admission, sample.AdmissionMS)
		if outcomes[sample.RPC] == nil {
			outcomes[sample.RPC] = map[string]int{}
		}
		outcome := strconv.Itoa(sample.Status)
		if sample.ErrorCode == "42901" {
			outcome = "rate_limited"
		}
		if sample.TransportError {
			outcome = "transport_error"
		}
		if sample.Timeout {
			outcome = "timeout"
		}
		outcomes[sample.RPC][outcome]++
	}
	var done int
	if err := p.QueryRow(ctx, "select count(*)from recsys.index_jobs where job_kind='backfill'and done_at is not null and post_id in(select id from feed.posts where author_id=any($1::uuid[]))", authors).Scan(&done); err != nil {
		t.Fatal(err)
	}
	report := map[string]any{"mode": mode, "label": label, "cold_fallback_http_ms": summarizeFeedTiming(fallbackMS), "target_per_minute": rate, "duration_seconds": seconds, "page_size": pageSize, "elapsed_seconds": elapsed.Seconds(), "arrivals": count, "completed": len(samples), "admission_dropped": dropped, "admission_ms": summarizeFeedTiming(admission), "http_ms": aggregates, "scheduled_end_to_end_ms": endToEnd, "http_latency_budget_met": latencyBudgetMet, "achieved_completions_per_minute": float64(len(samples)) * 60 / elapsed.Seconds(), "failures": failures, "max_sampled_lock_waiters": maxLocks, "wait_monitor_samples": monitorSamples, "wait_monitor_errors": monitorErrors, "outcomes": outcomes, "max_sampled_active_connections": maxActive, "backfill_done": done, "samples": samples, "note": "60 readers; 96 authors; 2000 warm technical vectors + 2000 background posts; sample p99 is descriptive, not a sustained SLA"}
	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join("..", ".local", output+".json"), encoded, 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("cold fallback HTTP ms n=%d p95=%.2f p99=%.2f", len(fallbackMS), summarizeFeedTiming(fallbackMS).P95, summarizeFeedTiming(fallbackMS).P99)
	for _, name := range names {
		summary := aggregates[name]
		t.Logf("%s %s n=%d HTTP ms p50=%.2f p95=%.2f p99=%.2f max=%.2f", mode, name, summary.Count, summary.P50, summary.P95, summary.P99, summary.Max)
	}
	t.Logf("rate=%d/min completed=%d/%d elapsed=%s admission_drops=%d admission_p99_ms=%.2f lock_waiters_max=%d active_max=%d backfill_done=%d", rate, len(samples), count, elapsed, dropped, summarizeFeedTiming(admission).P99, maxLocks, maxActive, done)
	if monitorSamples == 0 || monitorErrors != 0 {
		t.Error("database wait sampling incomplete")
	}
	if len(failures) > 0 {
		t.Error(strings.Join(failures[:min(5, len(failures))], "; "))
	}
	if dropped > 0 || len(samples) != count || elapsed > time.Duration(seconds+15)*time.Second {
		t.Error("load generator could not sustain requested arrival rate")
	}
	// Higher tiers locate saturation and report all values. Only the agreed
	// minimum tier gates the normal page-latency budget.
	if rate == 500 {
		if summarizeFeedTiming(admission).P99 > 100 {
			t.Error("generator admission wait obscured minimum-tier latency")
		}
		for _, name := range names {
			summary := aggregates[name]
			if summary.P95 > 300 || summary.P99 > 800 {
				t.Errorf("%s missed minimum-tier HTTP latency budget", name)
			}
		}
	}
	if mode != "off" && done == 0 {
		t.Error("background worker made no indexing progress")
	}
}
