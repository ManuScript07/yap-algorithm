package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"yap-algorithm/internal/adapters/postgres"
	"yap-algorithm/internal/application"
)

func sbRecommendationLoad(t *testing.T, admin *pgxpool.Pool, index *postgres.IndexRepository, repo *postgres.RecommendationRepository) {
	if os.Getenv("YAP_SUPABASE_LOAD_TEST") != "1" {
		t.Skip("enable -WithLoad")
	}
	ctx := context.Background()
	rows, err := admin.Query(ctx, `insert into auth.users(id,is_sso_user,is_anonymous,raw_user_meta_data)
 select extensions.gen_random_uuid(),false,false,'{"name":"Synthetic recommendation load"}' from generate_series(1,500) returning id::text`)
	if err != nil {
		t.Fatal(err)
	}
	users := make([]string, 0, 500)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		users = append(users, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		sbExec(t, admin, "delete from public.profiles where id=any($1::uuid[])", users)
		sbExec(t, admin, "delete from auth.users where id=any($1::uuid[])", users)
	})
	started := time.Now()
	sbExec(t, admin, `insert into feed.posts(id,author_id,text,audience,language,creation_request,created_at)
 select extensions.gen_random_uuid(),($1::uuid[])[1+(n-1)%500],'Synthetic load post '||n,'public','en','[]',
 statement_timestamp()-((n-1)/500)*interval '5 minutes' from generate_series(1,10001)n`, users)
	seedTime := time.Since(started)
	encoder := &sbEncoder{}
	indexer := application.Indexer{Repository: index, Encoder: encoder}
	runParallel := func(workers int, work func(int) error) {
		t.Helper()
		var group sync.WaitGroup
		errs := make(chan error, workers)
		for i := 0; i < workers; i++ {
			group.Add(1)
			go func(i int) {
				defer group.Done()
				if err := work(i); err != nil {
					errs <- err
				}
			}(i)
		}
		group.Wait()
		close(errs)
		for err := range errs {
			t.Error(err)
		}
		if t.Failed() {
			t.FailNow()
		}
	}
	started = time.Now()
	runParallel(8, func(int) error {
		for {
			worked, err := indexer.Once(ctx)
			if err != nil {
				return err
			}
			if !worked {
				return nil
			}
		}
	})
	indexTime := time.Since(started)
	if encoder.calls.Load() != 10001 {
		t.Fatalf("indexing lost jobs: %d", encoder.calls.Load())
	}
	f := sbFixture{admin: admin}
	queuedAt := make([]time.Time, len(users))
	started = time.Now()
	runParallel(16, func(worker int) error {
		for i := worker; i < len(users); i += 16 {
			queuedAt[i] = time.Now()
			raw, err := f.rpc(users[i], "select public.get_for_you_feed(null,-1,20)")
			if err != nil {
				return err
			}
			var page sbRecommendationPage
			if err := json.Unmarshal(raw, &page); err != nil {
				return err
			}
			if len(page.Items) != 20 {
				return fmt.Errorf("cold feed user %d: %d items", i, len(page.Items))
			}
		}
		return nil
	})
	coldTime := time.Since(started)
	started = time.Now()
	runParallel(8, func(int) error {
		for {
			worked, err := (application.Personalizer{Repository: repo}).Once(ctx)
			if err != nil {
				return err
			}
			if !worked {
				return nil
			}
		}
	})
	prepareTime := time.Since(started)
	var prepared, jobs int
	if err := admin.QueryRow(ctx, "select(select count(*)from recsys.user_states where user_id=any($1::uuid[])and prepared_at is not null),(select count(*)from recsys.refresh_jobs where user_id=any($1::uuid[]))", users).Scan(&prepared, &jobs); err != nil || prepared != 500 || jobs != 0 {
		t.Fatalf("prepared=%d pending=%d err=%v", prepared, jobs, err)
	}
	latencies := make([]time.Duration, len(users))
	started = time.Now()
	runParallel(16, func(worker int) error {
		for i := worker; i < len(users); i += 16 {
			before := time.Now()
			raw, err := f.rpc(users[i], "select public.get_for_you_feed(null,-1,20)")
			latencies[i] = time.Since(before)
			if err != nil {
				return err
			}
			var page sbRecommendationPage
			if err := json.Unmarshal(raw, &page); err != nil {
				return err
			}
			if page.Model != "personalized-v1" || len(page.Items) != 20 {
				return fmt.Errorf("unprepared feed for user %d", i)
			}
		}
		return nil
	})
	burstTime := time.Since(started)
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	lagRows, err := admin.Query(ctx, "select user_id::text,prepared_at from recsys.user_states where user_id=any($1::uuid[])", users)
	if err != nil {
		t.Fatal(err)
	}
	queued := make(map[string]time.Time, len(users))
	for i, user := range users {
		queued[user] = queuedAt[i]
	}
	lags := make([]time.Duration, 0, len(users))
	for lagRows.Next() {
		var id string
		var at time.Time
		if err := lagRows.Scan(&id, &at); err != nil {
			t.Fatal(err)
		}
		lags = append(lags, at.Sub(queued[id]))
	}
	lagRows.Close()
	if err := lagRows.Err(); err != nil {
		t.Fatal(err)
	}
	sort.Slice(lags, func(i, j int) bool { return lags[i] < lags[j] })
	t.Logf("500 active users, 10,001 posts: seed=%s; index fake encoder/8 workers=%s; cold reads=%s; prepare 500 snapshots/8 workers=%s; refresh lag p95=%s max=%s", seedTime, indexTime, coldTime, prepareTime, lags[474], lags[499])
	t.Logf("500 warm personalized SQL RPC reads/16 parallel: %s p95=%s p99=%s", burstTime, latencies[474], latencies[494])
	// One minute at the agreed aggregate arrival rate, with the normal worker
	// concurrency and cleanup schedule running, rather than an isolated burst.
	workerCtx, stopWorkers := context.WithCancel(ctx)
	var group sync.WaitGroup
	for i := 0; i < 2; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			pipeline := application.Pipeline{Processors: []application.Processor{indexer, application.Personalizer{Repository: repo}}}
			for workerCtx.Err() == nil {
				worked, err := pipeline.Once(workerCtx)
				if err != nil && workerCtx.Err() == nil {
					t.Errorf("steady worker: %v", err)
					return
				}
				if !worked {
					select {
					case <-workerCtx.Done():
						return
					case <-time.After(100 * time.Millisecond):
					}
				}
			}
		}()
	}
	t.Cleanup(func() { stopWorkers(); group.Wait() })
	steady := make([]time.Duration, 0, 500)
	started = time.Now()
	ticker := time.NewTicker(120 * time.Millisecond)
	for i := 0; i < 500; i++ {
		<-ticker.C
		before := time.Now()
		_, err := f.rpc(users[i], "select public.get_for_you_feed(null,-1,20)")
		steady = append(steady, time.Since(before))
		if err != nil {
			ticker.Stop()
			stopWorkers()
			group.Wait()
			t.Fatal(err)
		}
	}
	ticker.Stop()
	stopWorkers()
	group.Wait()
	sort.Slice(steady, func(i, j int) bool { return steady[i] < steady[j] })
	t.Logf("500 personalized SQL RPC reads at 500/min with 2 background workers: %s p95=%s p99=%s", time.Since(started), steady[474], steady[494])
	if err := repo.CleanupRecommendations(ctx); err != nil {
		t.Fatal(err)
	}
}
