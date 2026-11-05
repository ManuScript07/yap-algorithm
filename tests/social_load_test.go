package tests

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"yap-algorithm/internal/domain"
)

func TestSupabaseLocalSocialLoad(t *testing.T) {
	if os.Getenv("YAP_SUPABASE_INTEGRATION") != "1" || os.Getenv("YAP_SUPABASE_LOAD_TEST") != "1" {
		t.Skip("enable local integration -WithLoad")
	}
	cfg, err := pgxpool.ParseConfig(os.Getenv("YAP_SUPABASE_LOCAL_ADMIN_DSN"))
	if err != nil || cfg.ConnConfig.Host != "127.0.0.1" || cfg.ConnConfig.Port != 54322 {
		t.Fatal("explicit local database required")
	}
	cfg.MaxConns = 16
	ctx := context.Background()
	p, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	f := sbUsers(t, p)
	var readers []string
	for i := 0; i < 4; i++ {
		users := sbUsers(t, p)
		readers = append(readers, users.author, users.viewer, users.stranger)
	}
	for _, id := range readers {
		sbExec(t, p, "insert into public.friendships(user_one_id,user_two_id)values(least($1::uuid,$2::uuid),greatest($1::uuid,$2::uuid))", f.author, id)
	}
	started := time.Now()
	sbExec(t, p, `insert into feed.posts(id,author_id,text,audience,language,creation_request)
 select extensions.gen_random_uuid(),$1,'Social load fixture','public','ru','[]'from generate_series(1,10000)`, f.author)
	var post string
	if err := p.QueryRow(ctx, "select id::text from feed.posts where author_id=$1 order by id limit 1", f.author).Scan(&post); err != nil {
		t.Fatal(err)
	}
	sbExec(t, p, `insert into feed.comments(id,post_id,root_id,author_id,text,post_version,creation_request)
 select id,$1,id,$2,'Load comment',1,'[]'from(select extensions.gen_random_uuid()id from generate_series(1,10000))q`, post, f.viewer)
	t.Logf("10,000 posts + 10,000 roots seeded: %s", time.Since(started))
	c := soakHTTP{os.Getenv("YAP_SUPABASE_LOCAL_URL"), os.Getenv("YAP_SUPABASE_LOCAL_ANON_KEY"), &http.Client{Timeout: 15 * time.Second}}
	tokens := make([]string, len(readers))
	for i, id := range readers {
		tokens[i] = localMediaJWT(t, id)
	}
	type requestResult struct {
		name          string
		elapsed       time.Duration
		err           error
		status, count int
	}
	results := make(chan requestResult, 500)
	slots := make(chan struct{}, 8)
	var wg sync.WaitGroup
	timer := time.NewTicker(120 * time.Millisecond)
	defer timer.Stop()
	started = time.Now()
	for i := 0; i < 500; i++ {
		<-timer.C
		slots <- struct{}{}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			defer func() { <-slots }()
			name := "get_feed_comments"
			args := map[string]any{"requested_post": post, "requested_limit": 20}
			if i%2 == 0 {
				name = "get_friends_feed"
				args = map[string]any{"requested_limit": 20}
			}
			before := time.Now()
			raw, status, err := c.rpc(tokens[i%len(tokens)], name, args)
			var page struct {
				Items []json.RawMessage `json:"items"`
			}
			if err == nil {
				err = json.Unmarshal(raw, &page)
			}
			results <- requestResult{name, time.Since(before), err, status, len(page.Items)}
		}(i)
	}
	wg.Wait()
	close(results)
	elapsed := time.Since(started)
	var latency []time.Duration
	byRPC := map[string][]time.Duration{}
	for result := range results {
		if result.err != nil || result.status != 200 || result.count != 20 {
			t.Errorf("load %s status=%d items=%d error=%v", result.name, result.status, result.count, result.err)
		}
		if result.elapsed > 500*time.Millisecond {
			t.Logf("slow %s: %s", result.name, result.elapsed)
		}
		latency = append(latency, result.elapsed)
		byRPC[result.name] = append(byRPC[result.name], result.elapsed)
	}
	for name, samples := range byRPC {
		sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
		t.Logf("%s: p95=%s p99=%s", name, samples[len(samples)*95/100-1], samples[len(samples)*99/100-1])
	}
	sort.Slice(latency, func(i, j int) bool { return latency[i] < latency[j] })
	t.Logf("500 mixed actual HTTP comment/friends pages, target 500/min: %s p95=%s p99=%s", elapsed, latency[474], latency[494])
	if elapsed > 75*time.Second {
		t.Errorf("500/min target exceeded the 25%% timing allowance: %s", elapsed)
	}
	session := domain.UUID()
	sbExec(t, p, "insert into feed.page_sessions(id,user_id,post_id,sort,page_size,next_position)values($1,$2,$3,'popular',30,9960)", session, readers[0], post)
	sbExec(t, p, `insert into feed.page_items(session_id,position,comment_id)
 select $1,row_number()over(order by c.id)-1,c.id from feed.comments c where post_id=$2 order by c.id limit 9960`, session, post)
	before := time.Now()
	raw, status, err := c.rpc(tokens[0], "get_feed_comments", map[string]any{"requested_post": post, "requested_session": session, "after_position": 9959, "requested_limit": 30})
	if err != nil || status != 200 {
		t.Fatalf("deep page status=%d error=%v body=%s", status, err, raw)
	}
	var deep struct {
		Items   []json.RawMessage `json:"items"`
		HasMore bool              `json:"has_more"`
	}
	if err := json.Unmarshal(raw, &deep); err != nil || len(deep.Items) != 30 || !deep.HasMore {
		t.Fatalf("deep page missing remaining roots: count=%d has_more=%v error=%v", len(deep.Items), deep.HasMore, err)
	}
	t.Logf("popular page after 9,960 issued roots: %s", time.Since(before))
}
