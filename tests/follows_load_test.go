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

// Mass setup bypasses product quotas deliberately; all measured requests use
// actual PostgREST and per-reader JWTs. Fixtures are removed even after failure.
func TestSupabaseLocalFollowsLoad(t *testing.T) {
	if os.Getenv("YAP_SUPABASE_INTEGRATION") != "1" || os.Getenv("YAP_SUPABASE_LOAD_TEST") != "1" {
		t.Skip("local integration with -WithLoad required")
	}
	cfg, err := pgxpool.ParseConfig(os.Getenv("YAP_SUPABASE_LOCAL_ADMIN_DSN"))
	if err != nil || cfg.ConnConfig.Host != "127.0.0.1" || cfg.ConnConfig.Port != 54322 || os.Getenv("YAP_SUPABASE_LOCAL_URL") != "http://127.0.0.1:54321" {
		t.Fatal("explicit local database required")
	}
	cfg.MaxConns = 16
	ctx := context.Background()
	p, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	ids := make([]string, 1012)
	for i := range ids {
		ids[i] = domain.UUID()
	}
	t.Cleanup(func() {
		sbExec(t, p, "delete from public.profiles where id=any($1::uuid[])", ids)
		sbExec(t, p, "delete from auth.users where id=any($1::uuid[])", ids)
	})
	started := time.Now()
	sbExec(t, p, "insert into auth.users(id,is_sso_user,is_anonymous,raw_user_meta_data)select id,false,false,'{\"name\":\"Follow load\"}'from unnest($1::uuid[])id", ids)
	authors, readers := ids[:1000], ids[1000:]
	sbExec(t, p, `insert into feed.posts(id,author_id,text,audience,language,creation_request)
        select extensions.gen_random_uuid(),a,'Follow fan-in fixture','public','ru','[]'
        from unnest($1::uuid[])a cross join generate_series(1,10)`, authors)
	sbExec(t, p, `insert into feed.author_follows(follower_id,author_id,following,version)
        select r,a,true,1 from unnest($1::uuid[])r cross join unnest($2::uuid[])a`, readers, authors)
	beforeConnections := time.Now()
	var connectionCount int
	if err := p.QueryRow(ctx, "select count(*)from private.feed_timeline_authors($1)", readers[0]).Scan(&connectionCount); err != nil {
		t.Fatal(err)
	}
	t.Logf("current permission checks for %d connected authors: %s", connectionCount, time.Since(beforeConnections))
	t.Logf("1,000 followed authors, 10,000 posts, 12 readers: seeded in %s", time.Since(started))
	client := soakHTTP{os.Getenv("YAP_SUPABASE_LOCAL_URL"), os.Getenv("YAP_SUPABASE_LOCAL_ANON_KEY"), &http.Client{Timeout: 15 * time.Second}}
	tokens := make([]string, len(readers))
	for i, id := range readers {
		tokens[i] = localMediaJWT(t, id)
	}
	type result struct {
		name    string
		elapsed time.Duration
		status  int
		items   int
		err     error
	}
	results := make(chan result, 500)
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
			name := "get_friends_feed"
			args := map[string]any{"requested_limit": 20}
			if i%4 == 1 {
				name = "get_feed_following"
			}
			if i%4 == 3 {
				name = "get_feed_followers"
				args["requested_user"] = authors[i%len(authors)]
			}
			before := time.Now()
			raw, status, err := client.rpc(tokens[i%len(tokens)], name, args)
			var page struct {
				Items []json.RawMessage `json:"items"`
			}
			if err == nil {
				err = json.Unmarshal(raw, &page)
			}
			results <- result{name, time.Since(before), status, len(page.Items), err}
		}(i)
	}
	wg.Wait()
	close(results)
	elapsed := time.Since(started)
	byRPC := map[string][]time.Duration{}
	for r := range results {
		want := 20
		if r.name == "get_feed_followers" {
			want = len(readers)
		}
		if r.err != nil || r.status != 200 || r.items != want {
			t.Errorf("%s status=%d items=%d want=%d error=%v", r.name, r.status, r.items, want, r.err)
		}
		byRPC[r.name] = append(byRPC[r.name], r.elapsed)
	}
	for name, latency := range byRPC {
		sort.Slice(latency, func(i, j int) bool { return latency[i] < latency[j] })
		p95, p99 := latency[len(latency)*95/100-1], latency[len(latency)*99/100-1]
		t.Logf("%s: %d requests, p95=%s p99=%s max=%s", name, len(latency), p95, p99, latency[len(latency)-1])
		if p95 > 300*time.Millisecond || p99 > 800*time.Millisecond {
			t.Errorf("%s latency budget missed: p95=%s p99=%s", name, p95, p99)
		}
	}
	t.Logf("500 mixed real HTTP requests, <=8 concurrent: %s", elapsed)
	if elapsed > 75*time.Second {
		t.Errorf("500/min target exceeded 25%% timing allowance: %s", elapsed)
	}
	// Traverse the entire relation set with a compact UUID keyset, no offset.
	seen := map[string]bool{}
	var cursor any
	for {
		args := map[string]any{"requested_limit": 100}
		if cursor != nil {
			args["after_user"] = cursor
		}
		raw, status, err := client.rpc(tokens[0], "get_feed_following", args)
		if err != nil || status != 200 {
			t.Fatalf("following pagination: status=%d error=%v", status, err)
		}
		var page struct {
			Items []struct {
				User struct {
					ID string `json:"id"`
				} `json:"user"`
			} `json:"items"`
			HasMore bool           `json:"has_more"`
			Cursor  map[string]any `json:"next_cursor"`
		}
		if err := json.Unmarshal(raw, &page); err != nil {
			t.Fatal(err)
		}
		for _, item := range page.Items {
			if seen[item.User.ID] {
				t.Fatal("duplicate following across pages")
			}
			seen[item.User.ID] = true
		}
		if !page.HasMore {
			break
		}
		cursor = page.Cursor["after_user"]
	}
	if len(seen) != len(authors) {
		t.Fatalf("paginated %d authors, want %d", len(seen), len(authors))
	}
}
