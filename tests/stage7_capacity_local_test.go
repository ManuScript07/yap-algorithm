package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"yap-algorithm/internal/domain"
)

func stage7FixtureUsers(t *testing.T, pool *pgxpool.Pool, count int) []string {
	t.Helper()
	rows, err := pool.Query(context.Background(), `insert into auth.users(id,is_sso_user,is_anonymous,raw_user_meta_data)
		select extensions.gen_random_uuid(),false,false,'{"name":"Stage 7 fixture"}' from generate_series(1,$1::int) returning id::text`, count)
	if err != nil {
		t.Fatal(err)
	}
	users := make([]string, 0, count)
	t.Cleanup(func() {
		sbExec(t, pool, "delete from public.profiles where id=any($1::uuid[])", users)
		sbExec(t, pool, "delete from auth.users where id=any($1::uuid[])", users)
	})
	for rows.Next() {
		var user string
		if err := rows.Scan(&user); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		users = append(users, user)
	}
	rows.Close()
	if err := rows.Err(); err != nil || len(users) != count {
		t.Fatalf("created %d/%d fixture users: %v", len(users), count, err)
	}
	return users
}

type capacitySample struct {
	status             int
	valid              bool
	httpMS, endToEndMS float64
}

// Pace scheduled arrivals independently of worker completion. A buffered queue
// records demand even if the local system falls behind the intended arrival rate.
func stage7Arrivals(count, parallelism int, duration time.Duration, call func(int) (int, bool)) []capacitySample {
	samples := make([]capacitySample, count)
	jobs := make(chan int, count)
	interval := duration / time.Duration(count)
	start := time.Now()
	var group sync.WaitGroup
	for i := 0; i < parallelism; i++ {
		group.Go(func() {
			for index := range jobs {
				began := time.Now()
				status, valid := call(index)
				finished := time.Now()
				due := start.Add(time.Duration(index) * interval)
				samples[index] = capacitySample{status, valid, float64(finished.Sub(began)) / float64(time.Millisecond),
					float64(finished.Sub(due)) / float64(time.Millisecond)}
			}
		})
	}
	for i := 0; i < count; i++ {
		if delay := time.Until(start.Add(time.Duration(i) * interval)); delay > 0 {
			time.Sleep(delay)
		}
		jobs <- i
	}
	close(jobs)
	group.Wait()
	return samples
}

func reportStage7Arrivals(t *testing.T, profile string, samples []capacitySample) {
	t.Helper()
	var httpMS, scheduledMS []float64
	statuses := make(map[int]int)
	invalid := 0
	for _, sample := range samples {
		statuses[sample.status]++
		if !sample.valid {
			invalid++
		}
		httpMS = append(httpMS, sample.httpMS)
		scheduledMS = append(scheduledMS, sample.endToEndMS)
	}
	t.Logf("%s statuses=%v invalid=%d http_ms=%+v scheduled_ms=%+v", profile, statuses, invalid,
		summarizeFeedTiming(httpMS), summarizeFeedTiming(scheduledMS))
	if invalid != 0 {
		t.Errorf("%s failed %d/%d requests", profile, invalid, len(samples))
	}
}

func TestSupabaseLocalStage7HistoryBurst(t *testing.T) {
	value := os.Getenv("YAP_STAGE7_HISTORY")
	if value == "" {
		t.Skip("set YAP_STAGE7_HISTORY to 10000, 100000 or 300000 on the isolated stage 7 stack")
	}
	posts, err := strconv.Atoi(value)
	if err != nil || (posts != 10000 && posts != 100000 && posts != 300000) || localSupabaseTarget(t).port != 55322 {
		t.Fatal("history profile requires a supported size and the isolated local stack")
	}
	p := stage5Pool(t)
	users := stage7FixtureUsers(t, p, 628)
	authors, readers := users[:128], users[128:]
	seedArchiveHistory(t, p, authors, posts)
	client := soakHTTP{localSupabaseTarget(t).apiURL, os.Getenv("YAP_SUPABASE_LOCAL_ANON_KEY"), &http.Client{Timeout: 20 * time.Second}}
	tokens := make([]string, len(readers))
	for i, reader := range readers {
		tokens[i] = localMediaJWT(t, reader)
	}
	burst := stage7Arrivals(len(readers), 32, 10*time.Second, func(i int) (int, bool) {
		body, status, err := client.rpc(tokens[i], "get_for_you_feed_v2", scrollArgs(domain.UUID(), "", 30))
		var page scrollPage
		valid := err == nil && status == 200 && json.Unmarshal(body, &page) == nil && page.Contract == 2 &&
			page.More && len(page.Items) == 30 && page.Items[0].Model == "archive-v2"
		return status, valid
	})
	reportStage7Arrivals(t, "500 cold readers / 10 seconds", burst)
	// The first 360 publications are visible; after that, roughly one in 25
	// passes the current audience rules. Check both regimes in one cursor walk.
	var dense, sparse []float64
	seen := make(map[string]bool)
	session, cursor := domain.UUID(), ""
	for i := 0; i < 30; i++ {
		start := time.Now()
		page := readScroll(t, readers[0], session, cursor, 30)
		elapsed := float64(time.Since(start)) / float64(time.Millisecond)
		if len(seen) >= 360 {
			sparse = append(sparse, elapsed)
			if len(page.Items) < 1 || len(page.Items) > 6 {
				t.Fatalf("unexpected sparse archive page size %d", len(page.Items))
			}
		} else {
			dense = append(dense, elapsed)
			if len(page.Items) != 30 {
				t.Fatalf("unexpected dense archive page size %d", len(page.Items))
			}
		}
		for _, item := range page.Items {
			if seen[item.Post.ID] {
				t.Fatal("duplicate post in long archive walk")
			}
			seen[item.Post.ID] = true
		}
		if !page.More || page.Next == cursor {
			t.Fatal("archive cursor stopped or failed to advance")
		}
		cursor = page.Next
	}
	t.Logf("history posts=%d unique=%d dense_ms=%+v sparse_ms=%+v", posts, len(seen), summarizeFeedTiming(dense), summarizeFeedTiming(sparse))
}

// Archived fixtures represent old publications, so skip generating fresh model
// work and capture events during seeding. One isolated transaction restores
// both triggers before clients can read the seeded rows.
func seedArchiveHistory(t *testing.T, p *pgxpool.Pool, authors []string, posts int) {
	t.Helper()
	ctx := context.Background()
	tx, err := p.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `alter table feed.posts disable trigger feed_post_index_outbox, disable trigger feedback_content`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `insert into feed.posts(id,author_id,text,audience,language,creation_request,created_at)
		select extensions.gen_random_uuid(),($1::uuid[])[1+(n-1)%128],'Synthetic history '||n,
		case when n<=360 or n%25=0 then 'public' else 'friends' end,'en','[]',
		statement_timestamp()-interval '8 days'-n*interval '1 second'
		from generate_series(1,$2::int)n`, authors, posts); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `alter table feed.posts enable trigger feed_post_index_outbox, enable trigger feedback_content`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestSupabaseLocalStage7WriteCapacity(t *testing.T) {
	if os.Getenv("YAP_STAGE7_WRITE_CAPACITY") != "1" {
		t.Skip("enable the ten-minute stage 7 publication profile explicitly")
	}
	if localSupabaseTarget(t).port != 55322 || os.Getenv("YAP_SUPABASE_MODEL_TEST") != "1" {
		t.Fatal("publication capacity requires the isolated stack and real E5")
	}
	p := stage5Pool(t)
	const total = 10000
	const authors = 1000 // Each author publishes 10 posts, within the real hourly limit.
	users := stage7FixtureUsers(t, p, authors)
	client := soakHTTP{localSupabaseTarget(t).apiURL, os.Getenv("YAP_SUPABASE_LOCAL_ANON_KEY"), &http.Client{Timeout: 15 * time.Second}}
	tokens := make([]string, len(users))
	for i, user := range users {
		tokens[i] = localMediaJWT(t, user)
	}
	chats := createLoadChats(t, p, users[1:17], users[0])
	startPreparationWorker(t, "http://127.0.0.1:8091", filepath.Join("..", ".local", "stage7-write-worker.log"))
	ctx, cancel := context.WithCancel(context.Background())
	var monitor sync.WaitGroup
	peak, oldest, monitorErrors := 0, 0.0, 0
	var chatSamples []float64
	chatErrors := 0
	monitor.Go(func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				var backlog int
				var age float64
				err := p.QueryRow(ctx, `select count(*),coalesce(extract(epoch from clock_timestamp()-min(available_at)),0)::float8
					from recsys.index_jobs where job_kind='live' and done_at is null`).Scan(&backlog, &age)
				if err != nil && ctx.Err() == nil {
					monitorErrors++
				}
				peak, oldest = max(peak, backlog), max(oldest, age)
				reader := len(chatSamples) % len(chats)
				started := time.Now()
				body, status, err := client.rpc(tokens[1+reader], "get_chat_summaries_by_ids", map[string]any{
					"conversation_ids": []string{chats[reader]},
				})
				chatSamples = append(chatSamples, float64(time.Since(started))/float64(time.Millisecond))
				if err != nil || status != 200 || !bytes.Contains(body, []byte(chats[reader])) {
					chatErrors++
				}
			}
		}
	})
	defer func() { cancel(); monitor.Wait() }()
	started := time.Now()
	samples := stage7Arrivals(total, 32, 10*time.Minute, func(i int) (int, bool) {
		id := domain.UUID()
		language, text := "en", "A walk through the city to meet friends and discover public spaces. "
		if i%2 == 0 {
			language, text = "ru", "Прогулка по городу, чтобы встретить друзей и открыть для себя новые общественные пространства. "
		}
		suffix := "#" + strconv.Itoa(i)
		if i%20 <= 1 {
			text = string([]rune(strings.Repeat(text, 24))[:1000-len(suffix)])
		}
		body, status, err := client.rpc(tokens[i%authors], "create_feed_post", map[string]any{
			"requested_id": id, "requested_audience": "public", "requested_language": language, "requested_text": text + suffix,
		})
		var post struct {
			ID string `json:"id"`
		}
		return status, err == nil && status == 200 && json.Unmarshal(body, &post) == nil && post.ID == id
	})
	reportStage7Arrivals(t, "10000 publications / 10 minutes", samples)
	drainStart := time.Now()
	var saved, jobs, embedded int
	for {
		if err := p.QueryRow(ctx, `select count(*),count(j.id) filter(where j.done_at is not null),count(e.post_id)
			from feed.posts p left join recsys.index_jobs j on j.post_id=p.id and j.post_version=p.version
			left join recsys.post_embeddings e on e.post_id=p.id and e.post_version=p.version and e.model=$2
			where p.author_id=any($1::uuid[])`, users, sbModel).Scan(&saved, &jobs, &embedded); err != nil {
			t.Fatal(err)
		}
		if saved != total || jobs == total || time.Since(drainStart) >= 5*time.Minute {
			break
		}
		time.Sleep(time.Second)
	}
	cancel()
	monitor.Wait()
	t.Logf("publication elapsed=%s drain=%s posts=%d completed_jobs=%d real_embeddings=%d peak_pending=%d oldest_available_seconds=%.2f monitor_errors=%d",
		drainStart.Sub(started), time.Since(drainStart), saved, jobs, embedded, peak, oldest, monitorErrors)
	t.Logf("chat while publishing http_ms=%+v errors=%d", summarizeFeedTiming(chatSamples), chatErrors)
	if saved != total || jobs != total || embedded != total || monitorErrors != 0 || len(chatSamples) == 0 || chatErrors != 0 {
		t.Fatal("publication, outbox and embedding reconciliation failed")
	}
}
