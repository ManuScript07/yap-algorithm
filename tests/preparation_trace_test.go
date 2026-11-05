package tests

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptrace"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type preparationTraceTransport struct {
	t     *testing.T
	start time.Time
}
type preparationTraceBody struct {
	io.ReadCloser
	complete func()
}

func (b preparationTraceBody) Close() error { err := b.ReadCloser.Close(); b.complete(); return err }
func (tr preparationTraceTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	start := time.Now()
	var mu sync.Mutex
	var gotConn, wrote, firstByte time.Duration
	trace := &httptrace.ClientTrace{
		GotConn:              func(httptrace.GotConnInfo) { mu.Lock(); gotConn = time.Since(start); mu.Unlock() },
		WroteRequest:         func(httptrace.WroteRequestInfo) { mu.Lock(); wrote = time.Since(start); mu.Unlock() },
		GotFirstResponseByte: func() { mu.Lock(); firstByte = time.Since(start); mu.Unlock() },
	}
	response, err := http.DefaultTransport.RoundTrip(req.WithContext(httptrace.WithClientTrace(req.Context(), trace)))
	if err != nil {
		return nil, err
	}
	response.Body = preparationTraceBody{response.Body, func() {
		total := time.Since(start)
		if total < 500*time.Millisecond {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		tr.t.Logf("slow HTTP %s offset=%s conn=%s wrote=%s first_byte=%s total=%s", strings.TrimPrefix(req.URL.Path, "/rest/v1/rpc/"), start.Sub(tr.start), gotConn, wrote, firstByte, total)
	}}
	return response, nil
}

// Opt-in diagnosis: expose query class/waits, never SQL text, JWT or parameters.
func tracePreparationWaits(t *testing.T, p *pgxpool.Pool) func() {
	ctx, cancel := context.WithCancel(context.Background())
	var group sync.WaitGroup
	group.Add(1)
	go func() {
		defer group.Done()
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		last := time.Time{}
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if time.Since(last) < time.Second {
					continue
				}
				var raw []byte
				err := p.QueryRow(ctx, `select coalesce(jsonb_agg(jsonb_build_object('role',usename,'kind',case when query like '%get_nearby_feed%'then 'nearby'when query like '%get_for_you_feed%'then 'for_you'when query like '%get_friends_feed%'then 'friends'when query like '%load_refresh_candidates%'then 'candidates'when query like '%publish_recommendations%'then 'publish'else 'other'end,'wait',wait_event_type,'wait_event',wait_event,'age_ms',extract(epoch from clock_timestamp()-query_start)*1000,'blockers',cardinality(pg_blocking_pids(pid)))),'[]')from pg_stat_activity where state='active'and usename in('authenticator','yap_recsys_local_worker')and(query_start<clock_timestamp()-interval '500 milliseconds'or wait_event_type='Lock')`).Scan(&raw)
				if err == nil && json.Valid(raw) && string(raw) != "[]" {
					t.Logf("slow database activity %s", raw)
					last = time.Now()
				}
			}
		}
	}()
	return func() { cancel(); group.Wait() }
}
