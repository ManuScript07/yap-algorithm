package tests

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"yap-algorithm/internal/domain"
)

// Real writes as well as reads: 500 accepted transitions and 500 page reads in
// one minute. Per-reader versions and quotas remain enabled throughout.
func TestSupabaseLocalFeedbackLoad(t *testing.T) {
	if os.Getenv("YAP_SUPABASE_LOAD_TEST") != "1" {
		t.Skip("enable -WithLoad")
	}
	p := stage5Pool(t)
	f := sbUsers(t, p)
	post := f.create(t, "Journal write load", "public")
	var readers, deliveries, tokens []string
	for i := 0; i < 4; i++ {
		users := sbUsers(t, p)
		for _, user := range []string{users.author, users.viewer, users.stranger} {
			readers = append(readers, user)
			deliveries = append(deliveries, sbDelivery(t, f, user, post))
			tokens = append(tokens, localMediaJWT(t, user))
		}
	}
	client := soakHTTP{os.Getenv("YAP_SUPABASE_LOCAL_URL"), os.Getenv("YAP_SUPABASE_LOCAL_ANON_KEY"), &http.Client{Timeout: 10 * time.Second}}
	type result struct {
		actionMS, readMS         float64
		actionStatus, readStatus int
		err                      error
	}
	results := make(chan result, 500)
	slots := make(chan struct{}, 8)
	var group sync.WaitGroup
	start := time.Now()
	for i := 0; i < 500; i++ {
		if delay := time.Until(start.Add(time.Duration(i) * 120 * time.Millisecond)); delay > 0 {
			time.Sleep(delay)
		}
		slots <- struct{}{}
		group.Add(1)
		go func(i int) {
			defer group.Done()
			defer func() { <-slots }()
			reader, version := i%len(readers), i/len(readers)
			action := "like"
			if version%2 == 1 {
				action = "unlike"
			}
			before := time.Now()
			raw, status, err := client.rpc(tokens[reader], "apply_feed_action", map[string]any{"requested_id": domain.UUID(), "requested_delivery": deliveries[reader], "requested_action": action, "expected_version": version})
			out := result{actionMS: float64(time.Since(before)) / float64(time.Millisecond), actionStatus: status, err: err}
			var response map[string]any
			if err == nil {
				out.err = json.Unmarshal(raw, &response)
			}
			if out.err == nil && status == 200 && response["action_version"] != float64(version+1) {
				out.actionStatus = 0
			}
			before = time.Now()
			_, out.readStatus, err = client.rpc(tokens[reader], "get_for_you_feed", map[string]int{"requested_limit": 10})
			out.readMS = float64(time.Since(before)) / float64(time.Millisecond)
			if err != nil {
				out.err = err
			}
			results <- out
		}(i)
	}
	group.Wait()
	close(results)
	var writes, reads []float64
	for result := range results {
		if result.err != nil || result.actionStatus != 200 || result.readStatus != 200 {
			t.Errorf("load write=%d read=%d error=%v", result.actionStatus, result.readStatus, result.err)
		}
		writes = append(writes, result.actionMS)
		reads = append(reads, result.readMS)
	}
	for name, samples := range map[string][]float64{"action": writes, "for_you": reads} {
		summary := summarizeFeedTiming(samples)
		t.Logf("%s HTTP n=%d p95=%.2fms p99=%.2fms", name, summary.Count, summary.P95, summary.P99)
		if summary.Count != 500 || summary.P95 > 300 || summary.P99 > 800 {
			t.Errorf("%s load budget missed", name)
		}
	}
	var facts, requests, impressions int
	if err := p.QueryRow(context.Background(), `select count(*),count(distinct request_id),count(*)filter(where kind='impression')from recsys.feedback_facts where user_id=any($1::uuid[])`, readers).Scan(&facts, &requests, &impressions); err != nil {
		t.Fatal(err)
	}
	if facts != 500 || requests != 500 || impressions != 0 {
		t.Fatalf("journal cardinality facts=%d requests=%d impressions=%d", facts, requests, impressions)
	}
	if time.Since(start) > 75*time.Second {
		t.Fatal("arrival rate not sustained")
	}
	t.Logf("500 state transitions + 500 reads in %s; exactly 500 facts, no invented exposures", time.Since(start))
}
