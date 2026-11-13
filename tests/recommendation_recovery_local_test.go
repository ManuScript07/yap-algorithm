package tests

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"yap-algorithm/internal/domain"
)

func awaitLocalCondition(t *testing.T, pool *pgxpool.Pool, description string, timeout time.Duration, query string, args ...any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		var ok bool
		if err := pool.QueryRow(ctx, query, args...).Scan(&ok); err == nil && ok {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %s", description)
		case <-ticker.C:
		}
	}
}

func TestSupabaseLocalRecommendationRecovery(t *testing.T) {
	if os.Getenv("YAP_SUPABASE_MODEL_TEST") != "1" {
		t.Skip("recovery requires the real local E5 model")
	}
	p := stage5Pool(t)
	f := sbUsers(t, p)
	ctx := context.Background()
	const sessions = `select count(*) from pg_stat_activity where usename='yap_recsys_local_worker'`
	var open int
	if err := p.QueryRow(ctx, sessions).Scan(&open); err != nil || open != 0 {
		t.Fatalf("recovery requires the dedicated worker role to be idle: %d, %v", open, err)
	}
	endpoint := &url.URL{Scheme: "http", Host: "127.0.0.1:8091"}
	forward := httputil.NewSingleHostReverseProxy(endpoint)
	const (
		rejectInference = iota
		holdInference
		forwardInference
	)
	var mode atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch mode.Load() {
		case rejectInference:
			http.Error(w, "test model outage", http.StatusServiceUnavailable)
		case holdInference:
			_, _ = io.Copy(io.Discard, r.Body)
			<-r.Context().Done()
		case forwardInference:
			forward.ServeHTTP(w, r)
		}
	}))
	t.Cleanup(server.Close)

	post := f.create(t, "Заметка о прогулке по городу и архитектуре", "public")
	dsn := localRecommendationDSN(t)
	logs := t.TempDir()
	stop := startPreparationWorkerProcess(t, dsn, server.URL, filepath.Join(logs, "before-crash.log"))
	awaitLocalCondition(t, p, "a durable model retry", 20*time.Second, `select exists (select 1 from recsys.index_jobs
		where post_id=$1 and attempts>=1 and last_error='processing_failed' and done_at is null)`, post)
	mode.Store(holdInference)
	awaitLocalCondition(t, p, "the claim preceding the crash", 30*time.Second, `select exists (select 1 from recsys.index_jobs
		where post_id=$1 and lease is not null and attempts>=2 and locked_until>clock_timestamp())`, post)
	stop()
	page := readScroll(t, f.viewer, domain.UUID(), "", 30)
	if !slices.ContainsFunc(page.Items, func(item scrollItem) bool { return item.Post.ID == post }) {
		t.Fatal("Supabase stopped serving new eligible content during worker downtime")
	}

	proxy := newMediaDatabaseProxy(t)
	proxied, err := url.Parse(dsn)
	if err != nil {
		t.Fatal("invalid restricted worker identity")
	}
	proxied.Host = proxy.listener.Addr().String()
	mode.Store(forwardInference)
	stop = startPreparationWorkerProcess(t, proxied.String(), server.URL, filepath.Join(logs, "after-crash.log"))
	awaitLocalCondition(t, p, "replacement pool", 15*time.Second, "select ("+sessions+") between 1 and 4")
	proxy.setDown(true)
	second := f.create(t, "A public post written while the worker lost its database connection", "public")
	awaitLocalCondition(t, p, "worker connections to close during the fault", 10*time.Second, "select ("+sessions+")=0")
	if err := p.QueryRow(ctx, "select count(*) from recsys.post_embeddings where post_id=$1", second).Scan(&open); err != nil || open != 0 {
		t.Fatalf("embedding committed through the failed worker transport: %d, %v", open, err)
	}
	proxy.setDown(false)
	// The abandoned lease expires on its own. A stale completion cannot produce
	// a second embedding for the same content version.
	awaitLocalCondition(t, p, "retry and abandoned lease recovery", 75*time.Second, `select count(*)=2 from recsys.post_embeddings
		where post_id=any($1::uuid[]) and post_version=1 and model=$2 and abs(extensions.vector_norm(embedding)-1)<0.001`, []string{post, second}, sbModel)
	awaitLocalCondition(t, p, "both jobs marked complete", 5*time.Second, `select count(*)=2 from recsys.index_jobs
		where post_id=any($1::uuid[]) and done_at is not null and lease is null`, []string{post, second})
	stop()
	awaitLocalCondition(t, p, "replacement pool closure", 10*time.Second, "select ("+sessions+")=0")
}
