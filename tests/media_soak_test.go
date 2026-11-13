package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"yap-algorithm/internal/domain"
)

type soakHTTP struct {
	base, key string
	client    *http.Client
}

// Preserve the timeout category without logging URLs, headers or transport
// payloads. Load reports must distinguish a timeout from an ordinary failure.
type soakTransportFailure struct{ timeout bool }

func (e soakTransportFailure) Error() string   { return "local HTTP transport failed" }
func (e soakTransportFailure) Timeout() bool   { return e.timeout }
func (e soakTransportFailure) Temporary() bool { return false }

// Only an owned fixture that was actually deleted is an expected race. Do not
// hide expired receipts, version conflicts, transport failures or other 4xx.
func soakDeletedAction(ctx context.Context, p *pgxpool.Pool, user, delivery string, authors []string, status int, body []byte) bool {
	var problem struct{ Code, Message string }
	if status != 400 || json.Unmarshal(body, &problem) != nil || problem.Code != "22023" || problem.Message != "delivery_expired_or_invalidated" {
		return false
	}
	var deleted bool
	err := p.QueryRow(ctx, `select exists(select 1 from recsys.deliveries d join feed.posts p on p.id=d.post_id
	 where d.id=$1 and d.user_id=$2 and d.valid_until>statement_timestamp()and p.deleted_at is not null
	 and p.author_id=any($3::uuid[]))`, delivery, user, authors).Scan(&deleted)
	return err == nil && deleted
}

func (c soakHTTP) call(method, path, token, mime string, payload []byte) ([]byte, int, error) {
	req, err := http.NewRequest(method, c.base+path, bytes.NewReader(payload))
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("apikey", c.key)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", mime)
	req.Header.Set("x-upsert", "false")
	resp, err := c.client.Do(req)
	if err != nil {
		var failure net.Error
		return nil, 0, soakTransportFailure{errors.As(err, &failure) && failure.Timeout()}
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 6<<20))
	return body, resp.StatusCode, err
}
func (c soakHTTP) rpc(token, name string, args any) ([]byte, int, error) {
	b, _ := json.Marshal(args)
	return c.call("POST", "/rest/v1/rpc/"+name, token, "application/json", b)
}

type soakProcess struct {
	cmd *exec.Cmd
	log *os.File
}

func (p *soakProcess) stop() {
	if p.cmd != nil {
		_ = p.cmd.Process.Kill()
		_ = p.cmd.Wait()
		p.cmd = nil
	}
	if p.log != nil {
		_ = p.log.Close()
		p.log = nil
	}
}

// Opt-in wall-clock soak: real RPCs, Storage files, two isolated worker processes,
// bounded fixture volume, and faults injected only into these test workers.
func TestSupabaseLocalMediaSoak(t *testing.T) {
	if os.Getenv("YAP_SUPABASE_MEDIA_SOAK") != "1" {
		t.Skip("enable -WithSoak; default 60 real minutes")
	}
	minutes, err := strconv.Atoi(os.Getenv("YAP_SUPABASE_SOAK_MINUTES"))
	if err != nil || minutes < 1 || minutes > 120 {
		t.Fatal("invalid soak duration")
	}
	withModel := os.Getenv("YAP_SUPABASE_MODEL_TEST") == "1"
	if minutes >= 60 && !withModel {
		t.Fatal("hour soak requires -WithModel and a local recommendation worker identity")
	}
	p := capacityAdmin(t)
	ctx := context.Background()
	var active int
	if err := p.QueryRow(ctx, "select active from media.capacity").Scan(&active); err != nil || active != 0 {
		t.Fatal("soak requires no other active media reservations", err)
	}
	var workers int
	if err := p.QueryRow(ctx, "select count(*)from media.workers where expires_at>statement_timestamp()").Scan(&workers); err != nil {
		t.Fatal(err)
	}
	if workers != 0 {
		t.Fatal("stop other media workers before soak")
	}
	assertMediaAccounting(t, p)
	c := soakHTTP{os.Getenv("YAP_SUPABASE_LOCAL_URL"), os.Getenv("YAP_SUPABASE_LOCAL_ANON_KEY"), &http.Client{Timeout: 20 * time.Second}}
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(root, ".local", "media-soak-"+domain.UUID())
	if err := os.MkdirAll(output, 0700); err != nil {
		t.Fatal(err)
	}
	exe := filepath.Join(output, "media-worker.exe")
	build := exec.Command("go", "build", "-o", exe, "./cmd/media-worker")
	build.Dir = root
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %s %v", out, err)
	}
	// A loopback reverse proxy isolates the injected Storage outage from the app.
	target, _ := url.Parse(c.base)
	proxy := httputil.NewSingleHostReverseProxy(target)
	var storageDown atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if storageDown.Load() {
			http.Error(w, "test outage", 503)
			return
		}
		proxy.ServeHTTP(w, r)
	}))
	defer server.Close()
	procs := make([]soakProcess, 2)
	defer func() {
		for i := range procs {
			procs[i].stop()
		}
	}()
	startWorker := func(index int) {
		log, err := os.OpenFile(filepath.Join(output, fmt.Sprintf("worker-%d.log", index)), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command(exe, "-concurrency", "1")
		cmd.Dir = root
		cmd.Stdout = log
		cmd.Stderr = log
		for _, entry := range os.Environ() {
			name := strings.SplitN(entry, "=", 2)[0]
			if strings.HasPrefix(name, "YAP_SUPABASE_") || name == "MEDIA_DATABASE_URL" || name == "MEDIA_STORAGE_TOKEN" || name == "SUPABASE_URL" || name == "SUPABASE_PUBLISHABLE_KEY" {
				continue
			}
			cmd.Env = append(cmd.Env, entry)
		}
		// No admin/service role/signing secret in the production worker environment.
		cmd.Env = append(cmd.Env, "MEDIA_DATABASE_URL="+os.Getenv("YAP_SUPABASE_MEDIA_DSN"), "MEDIA_STORAGE_TOKEN="+os.Getenv("YAP_SUPABASE_MEDIA_TOKEN"), "SUPABASE_URL="+server.URL, "SUPABASE_PUBLISHABLE_KEY="+c.key, "GOMEMLIMIT=256MiB", "GOMAXPROCS=2")
		if err := cmd.Start(); err != nil {
			log.Close()
			t.Fatal(err)
		}
		procs[index] = soakProcess{cmd, log}
	}
	var semantic soakProcess
	defer semantic.stop()
	if withModel {
		semanticExe := filepath.Join(output, "recommendation-worker.exe")
		build := exec.Command("go", "build", "-o", semanticExe, "./cmd/worker")
		build.Dir = root
		if out, err := build.CombinedOutput(); err != nil {
			t.Fatalf("recommendation build: %s %v", out, err)
		}
		log, err := os.Create(filepath.Join(output, "recommendations.log"))
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command(semanticExe, "-concurrency", "2")
		cmd.Dir = root
		cmd.Stdout = log
		cmd.Stderr = log
		for _, entry := range os.Environ() {
			name := strings.SplitN(entry, "=", 2)[0]
			if strings.HasPrefix(name, "YAP_SUPABASE_") || strings.HasPrefix(name, "SUPABASE_") || strings.HasPrefix(name, "MEDIA_") || name == "DATABASE_URL" || name == "MODEL_URL" {
				continue
			}
			cmd.Env = append(cmd.Env, entry)
		}
		cmd.Env = append(cmd.Env, "DATABASE_URL="+os.Getenv("YAP_SUPABASE_RECSYS_DSN"), "MODEL_URL=http://127.0.0.1:8091", "GOMAXPROCS=2")
		if err := cmd.Start(); err != nil {
			log.Close()
			t.Fatal(err)
		}
		semantic = soakProcess{cmd, log}
	}
	var ids, tokens []string
	for i := 0; i < 144; i++ {
		id := domain.UUID()
		sbExec(t, p, `insert into auth.users(id,is_sso_user,is_anonymous,raw_user_meta_data)values($1,false,false,'{"name":"Media soak fixture"}')`, id)
		ids = append(ids, id)
		tokens = append(tokens, localMediaJWT(t, id))
	}
	t.Cleanup(func() {
		semantic.stop()
		for i := range procs {
			procs[i].stop()
		}
		// Kill is deliberate in this scenario; remove only identities that
		// processed this run's fixtures before cascading their photo rows.
		sbExec(t, p, `delete from media.workers where id in(select u.worker_id from feed.photo_uploads u
		 join feed.post_drafts d on d.id=u.draft_id where d.owner_id=any($1::uuid[]))`, ids)
		sbExec(t, p, "delete from public.profiles where id=any($1::uuid[])", ids)
		for _, bucket := range []string{"feed-uploads", "feed-media"} {
			rows, err := p.Query(ctx, "select name from storage.objects where bucket_id=$1 and split_part(name,'/',1)=any($2::text[])", bucket, ids)
			if err != nil {
				t.Error(err)
				continue
			}
			paths := []string{}
			for rows.Next() {
				var path string
				rows.Scan(&path)
				paths = append(paths, path)
			}
			rows.Close()
			for len(paths) > 0 {
				n := 100
				if len(paths) < n {
					n = len(paths)
				}
				data, _ := json.Marshal(map[string]any{"prefixes": paths[:n]})
				_, status, err := c.call("DELETE", "/storage/v1/object/"+bucket, os.Getenv("YAP_SUPABASE_LOCAL_SERVICE_KEY"), "application/json", data)
				if err != nil || status != 200 {
					t.Errorf("fixture file cleanup status=%d", status)
					break
				}
				paths = paths[n:]
			}
		}
		sbExec(t, p, "delete from media.cleanup_jobs where split_part(path,'/',1)=any($1::text[])", ids)
		sbExec(t, p, "delete from auth.users where id=any($1::uuid[])", ids)
		sbExec(t, p, "delete from media.workers where expires_at<=statement_timestamp()")
		assertMediaAccounting(t, p)
	})
	peer := ids[126]
	chats := createLoadChats(t, p, ids[128:], peer)
	sbExec(t, p, `insert into public.user_locations(user_id,latitude,longitude,updated_at)
		select unnest($1::uuid[]),55,37,statement_timestamp()`, ids[128:])
	sbExec(t, p, `insert into feed.author_follows(follower_id,author_id,following,version)
		select unnest($1::uuid[]),$2,true,1`, ids[128:], peer)
	anchorPost := domain.UUID()
	sbExec(t, p, `insert into feed.posts(id,author_id,text,audience,language,creation_request,latitude,longitude)
		values($1,$2,'Synthetic mixed workload discussion','public','en','[]',55,37)`, anchorPost, peer)
	stage5RPC(t, ids[128], "create_feed_comment", map[string]any{
		"requested_id": domain.UUID(), "requested_post": anchorPost, "requested_text": "Synthetic root comment",
	}, 200)
	// Typical phone images plus occasional 12 MP decodes. All files stay in local
	// Supabase Storage; byte buffers below are only the load generator's sources.
	images := [][]byte{mediaImage(t, 640, 480, false), mediaImage(t, 1024, 768, true), mediaImage(t, 4000, 3000, false), mediaImage(t, 4000, 5000, false)}
	var sourceBytes atomic.Int64
	var feedCount atomic.Int64
	var feedErrors atomic.Int64
	var actions, actionErrors, personalized atomic.Int64
	var staleActions atomic.Int64
	type interestProbe struct {
		revision int64
		at       time.Time
	}
	var probeMu sync.Mutex
	probes := map[string]interestProbe{}
	var interestLatency []time.Duration
	var feedMu sync.Mutex
	var feedLatency []time.Duration
	readSurfaces := []string{"get_friends_feed", "get_nearby_feed", "get_feed_comments", "get_chat_summaries_by_ids"}
	readSamples := make(map[string][]float64, len(readSurfaces))
	var readErrors int
	stopFeed, stop := context.WithCancel(context.Background())
	defer stop()
	var feedWG sync.WaitGroup
	feedWG.Add(1)
	go func() {
		defer feedWG.Done()
		timer := time.NewTicker(120 * time.Millisecond)
		defer timer.Stop()
		scrolls := make([]scrollPage, 16)
		i := 0
		for {
			select {
			case <-stopFeed.Done():
				return
			case <-timer.C:
				if i%8 == 0 {
					reader := i / 8 % len(scrolls)
					surface := readSurfaces[i/8%len(readSurfaces)]
					args := map[string]any{"requested_limit": 20}
					switch surface {
					case "get_feed_comments":
						args["requested_post"] = anchorPost
					case "get_chat_summaries_by_ids":
						args = map[string]any{"conversation_ids": []string{chats[reader]}}
					}
					start := time.Now()
					body, status, err := c.rpc(tokens[128+reader], surface, args)
					readSamples[surface] = append(readSamples[surface], float64(time.Since(start))/float64(time.Millisecond))
					if err != nil || status != 200 || (surface == "get_chat_summaries_by_ids" && !bytes.Contains(body, []byte(chats[reader]))) {
						readErrors++
					}
				}
				reader := i % len(scrolls)
				previous := scrolls[reader]
				session, cursor := previous.Session, previous.Next
				if cursor == "" || i/len(scrolls)%3 == 0 {
					session, cursor = domain.UUID(), ""
				}
				before := time.Now()
				raw, status, err := c.rpc(tokens[128+reader], "get_for_you_feed_v2", scrollArgs(session, cursor, 20))
				if err != nil || status != 200 {
					feedErrors.Add(1)
				}
				feedCount.Add(1)
				feedMu.Lock()
				feedLatency = append(feedLatency, time.Since(before))
				feedMu.Unlock()
				if status == 200 {
					var page scrollPage
					if json.Unmarshal(raw, &page) != nil || page.Session != session || page.More != (page.Next != "") {
						feedErrors.Add(1)
						i++
						continue
					}
					scrolls[reader] = page
					if len(page.Items) > 0 && page.Items[0].Model == "personalized-v2" {
						personalized.Add(1)
					}
					if withModel && i%37 == 0 && len(page.Items) > 0 {
						item := page.Items[0]
						kind := "like"
						if item.Post.Liked {
							kind = "unlike"
						}
						actionBody, s, e := c.rpc(tokens[128+reader], "apply_feed_action", map[string]any{"requested_id": domain.UUID(), "requested_delivery": item.Receipt, "requested_action": kind, "expected_version": item.Post.ActionVersion})
						if e != nil || s != 200 {
							if e == nil && soakDeletedAction(ctx, p, ids[128+reader], item.Receipt, ids[:128], s, actionBody) {
								staleActions.Add(1)
							} else {
								actionErrors.Add(1)
							}
						} else {
							actions.Add(1)
							var rev int64
							user := ids[128+reader]
							if p.QueryRow(ctx, "select revision from recsys.user_states where user_id=$1", user).Scan(&rev) == nil {
								probeMu.Lock()
								if _, exists := probes[user]; !exists {
									probes[user] = interestProbe{rev, time.Now()}
								}
								probeMu.Unlock()
							}
						}
					}
				}
				i++
			}
		}
	}()
	defer func() { stop(); feedWG.Wait() }()
	for i := range procs {
		startWorker(i)
	}
	type pendingPost struct {
		post, token string
		photos      []string
		accepted    time.Time
		published   time.Time
	}
	var pending []*pendingPost
	var accepted, busy, published, deleted int
	var expiredScrolls, peakExpiredScrolls, peakRecommendationConnections, peakMediaConnections int
	var scrollCleanupObserved bool
	var completion []time.Duration
	start := time.Now()
	// Real one-hour expiry, without changing database timestamps or lease clocks.
	expiringPost, expiringPhoto := domain.UUID(), domain.UUID()
	if minutes >= 60 {
		if _, s, e := c.rpc(tokens[127], "reserve_feed_photo", map[string]any{"requested_post": expiringPost, "requested_photo": expiringPhoto, "requested_bytes": 1024, "requested_mime": "image/jpeg"}); e != nil || s != 200 {
			t.Fatal("expiry probe reservation")
		}
	}
	duration := time.Duration(minutes) * time.Minute
	deadline := start.Add(duration)
	var stopped, restarted, storageFault, storageRestored, dbFault bool
	restartAt := start.Add(duration / 10)
	if duration/10 < 5*time.Second {
		restartAt = start.Add(5 * time.Second)
	}
	var stoppedAt time.Time
	nextUpload := start
	nextPoll := start
	nextSample := start
	logFile, err := os.Create(filepath.Join(output, "samples.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer logFile.Close()
	enc := json.NewEncoder(logFile)
	for time.Now().Before(deadline) {
		now := time.Now()
		elapsed := now.Sub(start)
		if !stopped && now.After(restartAt) {
			for i := range procs {
				procs[i].stop()
			}
			stopped = true
			stoppedAt = now
			t.Log("fault: both worker processes terminated")
		}
		if stopped && !restarted && now.Sub(stoppedAt) > 32*time.Second {
			for i := range procs {
				startWorker(i)
			}
			restarted = true
			t.Log("recovery: restarted with new worker identities")
		}
		if !storageFault && elapsed > duration/3 {
			storageDown.Store(true)
			storageFault = true
			t.Log("fault: worker Storage returns 503")
		}
		outage := 15 * time.Second
		if duration < 5*time.Minute {
			outage = 3 * time.Second
		}
		if storageFault && !storageRestored && elapsed > duration/3+outage {
			storageDown.Store(false)
			storageRestored = true
			t.Log("recovery: Storage restored")
		}
		if !dbFault && elapsed > duration/2 {
			// Disconnect only the restricted media login's idle connections; this does
			// not stop shared Postgres or interfere with other local app users.
			sbExec(t, p, "select pg_terminate_backend(pid)from pg_stat_activity where usename='yap_media_local_worker'and state='idle'and pid<>pg_backend_pid()")
			dbFault = true
			t.Log("fault: restricted worker DB connections disconnected")
		}
		if !now.Before(nextUpload) {
			interval := 5 * time.Second
			// A 45 s burst exceeds decode throughput and exercises admission/age gates.
			burst := elapsed > duration*2/3 && elapsed < duration*2/3+45*time.Second
			if burst {
				interval = 150 * time.Millisecond
			}
			nextUpload = now.Add(interval)
			owner := (accepted/3 + busy) % 128
			post := &pendingPost{post: domain.UUID(), token: tokens[owner], accepted: now}
			for slot := 0; slot < 3; slot++ {
				id := domain.UUID()
				im := images[(accepted+slot)%len(images)]
				mime := "image/jpeg"
				if (accepted+slot)%len(images) == 1 {
					mime = "image/png"
				}
				raw, status, err := c.rpc(post.token, "reserve_feed_photo", map[string]any{"requested_post": post.post, "requested_photo": id, "requested_bytes": len(im), "requested_mime": mime})
				if err != nil {
					t.Fatal("reservation transport", err)
				}
				if status == 429 {
					busy++
					break
				}
				if status != 200 {
					t.Fatalf("reserve status=%d response=%s", status, raw)
				}
				var intent struct {
					Path string `json:"path"`
				}
				if json.Unmarshal(raw, &intent) != nil || intent.Path == "" {
					t.Fatal("invalid reservation")
				}
				if sourceBytes.Add(int64(len(im))) > 2<<30 {
					t.Fatal("2 GiB source upload safety budget reached")
				}
				_, status, err = c.call("POST", "/storage/v1/object/feed-uploads/"+intent.Path, post.token, mime, im)
				if err != nil || status != 200 {
					t.Fatalf("source upload status=%d", status)
				}
				_, status, err = c.rpc(post.token, "confirm_feed_photo", map[string]any{"requested_photo": id})
				if err != nil || status != 200 {
					t.Fatalf("confirmation status=%d", status)
				}
				post.photos = append(post.photos, id)
				accepted++
			}
			if len(post.photos) > 0 {
				pending = append(pending, post)
			}
		}
		if !now.Before(nextPoll) {
			nextPoll = now.Add(2 * time.Second)
			probeMu.Lock()
			for user, probe := range probes {
				var rev int64
				if err := p.QueryRow(ctx, "select processed_revision from recsys.user_states where user_id=$1", user).Scan(&rev); err != nil {
					t.Error(err)
				} else if rev >= probe.revision {
					interestLatency = append(interestLatency, time.Since(probe.at))
					delete(probes, user)
				}
			}
			probeMu.Unlock()
			for _, post := range pending {
				if post.published.IsZero() {
					var ready int
					if err := p.QueryRow(ctx, "select count(*)from feed.photo_uploads where id=any($1::uuid[])and state='ready'", post.photos).Scan(&ready); err != nil {
						t.Fatal(err)
					}
					if ready == len(post.photos) {
						caption := ""
						if withModel && published%3 == 0 {
							caption = "Разработка игр и серверов на Go. Software engineering and multiplayer games."
						}
						raw, status, err := c.rpc(post.token, "publish_feed_post", map[string]any{"requested_id": post.post, "requested_photos": post.photos, "requested_text": caption})
						if err != nil || status != 200 {
							t.Fatalf("publish status=%d response=%s", status, raw)
						}
						post.published = now
						published++
						completion = append(completion, now.Sub(post.accepted))
					}
				} else if now.Sub(post.published) > 30*time.Second {
					raw, status, err := c.rpc(post.token, "delete_feed_post", map[string]any{"requested_id": post.post})
					if err != nil || status != 204 {
						t.Fatalf("delete status=%d response=%s", status, raw)
					}
					post.published = time.Unix(1, 0)
					deleted++
				}
			}
			kept := pending[:0]
			for _, post := range pending {
				if !post.published.Equal(time.Unix(1, 0)) {
					kept = append(kept, post)
				}
			}
			pending = kept
		}
		if !now.Before(nextSample) {
			nextSample = now.Add(30 * time.Second)
			assertMediaAccounting(t, p)
			var raw json.RawMessage
			if err := p.QueryRow(ctx, "select media.queue_snapshot()").Scan(&raw); err != nil {
				t.Fatal(err)
			}
			var slots int
			if err := p.QueryRow(ctx, "select count(*)from media.processing_slots where expires_at>statement_timestamp()").Scan(&slots); err != nil {
				t.Fatal(err)
			}
			if slots > 2 {
				t.Fatal("shared parallelism budget exceeded")
			}
			var actual int64
			if err := p.QueryRow(ctx, "select coalesce(sum((metadata->>'size')::bigint),0)from storage.objects where bucket_id in('feed-media','feed-uploads')and split_part(name,'/',1)=any($1::text[])", ids).Scan(&actual); err != nil {
				t.Fatal(err)
			}
			if actual > 2<<30 {
				t.Fatal("2 GiB live Storage safety budget exceeded")
			}
			var expired, recommendationConnections, mediaConnections int
			if err := p.QueryRow(ctx, `select
				(select count(*) from recsys.feed_scrolls where user_id=any($1::uuid[]) and expires_at<=statement_timestamp()),
				count(*) filter(where usename='yap_recsys_local_worker'), count(*) filter(where usename='yap_media_local_worker')
				from pg_stat_activity`, ids[128:]).Scan(&expired, &recommendationConnections, &mediaConnections); err != nil {
				t.Fatal(err)
			}
			scrollCleanupObserved = scrollCleanupObserved || expired < expiredScrolls
			expiredScrolls, peakExpiredScrolls = expired, max(peakExpiredScrolls, expired)
			peakRecommendationConnections = max(peakRecommendationConnections, recommendationConnections)
			peakMediaConnections = max(peakMediaConnections, mediaConnections)
			if err := enc.Encode(map[string]any{"elapsed_seconds": elapsed.Seconds(), "accepted": accepted, "busy": busy, "published": published, "deleted": deleted, "source_bytes": sourceBytes.Load(), "storage_bytes": actual, "feed_requests": feedCount.Load(), "feed_errors": feedErrors.Load(), "action_errors": actionErrors.Load(), "expected_deleted_actions": staleActions.Load(), "queue": raw,
				"expired_scrolls": expired, "recommendation_connections": recommendationConnections, "media_connections": mediaConnections}); err != nil {
				t.Fatal(err)
			}
			t.Logf("wall=%s accepted=%d busy=%d published=%d pending=%d Storage=%d bytes feed=%d errors=%d", elapsed.Round(time.Second), accepted, busy, published, len(pending), actual, feedCount.Load(), feedErrors.Load())
			// Record real per-process RSS and CPU without credentials or environment.
			var pidArgs []string
			if semantic.cmd != nil {
				pidArgs = append(pidArgs, strconv.Itoa(semantic.cmd.Process.Pid))
			}
			for i := range procs {
				if procs[i].cmd != nil {
					pidArgs = append(pidArgs, strconv.Itoa(procs[i].cmd.Process.Pid))
				}
			}
			if len(pidArgs) > 0 {
				sample := exec.Command("powershell.exe", "-NoProfile", "-Command", "Get-Process -Id "+strings.Join(pidArgs, ",")+" -ErrorAction SilentlyContinue | Select-Object Id,CPU,WorkingSet64,PrivateMemorySize64 | ConvertTo-Json -Compress")
				if data, err := sample.Output(); err == nil {
					if _, err := logFile.Write(append(data, '\n')); err != nil {
						t.Fatal(err)
					}
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	// Give accepted jobs real time to finish after faults; never alter lease clocks.
	storageDown.Store(false)
	if !restarted {
		for i := range procs {
			if procs[i].cmd == nil {
				startWorker(i)
			}
		}
	}
	drainUntil := time.Now().Add(4 * time.Minute)
	for time.Now().Before(drainUntil) {
		var unfinished int
		if err := p.QueryRow(ctx, "select count(*)from media.reservations r join feed.photo_uploads u on u.id=r.photo_id where r.unfinished and split_part(u.source_path,'/',1)=any($1::text[])", ids).Scan(&unfinished); err != nil {
			t.Fatal(err)
		}
		if unfinished == 0 {
			break
		}
		if time.Until(drainUntil) < time.Second {
			t.Fatalf("accepted jobs failed to recover: %d", unfinished)
		}
		time.Sleep(time.Second)
	}
	for _, post := range pending {
		if post.published.IsZero() {
			payload := map[string]any{"requested_id": post.post, "requested_photos": post.photos}
			for replay := 0; replay < 2; replay++ {
				raw, s, e := c.rpc(post.token, "publish_feed_post", payload)
				if e != nil || s != 200 {
					t.Fatalf("final publication/replay status=%d body=%s", s, raw)
				}
			}
			published++
			completion = append(completion, time.Since(post.accepted))
		}
	}
	var processed int
	if err := p.QueryRow(ctx, "select count(*)from feed.photo_uploads u join feed.post_drafts d on d.id=u.draft_id where d.owner_id=any($1::uuid[])and u.state='attached'", ids).Scan(&processed); err != nil || processed != accepted {
		t.Fatalf("accepted photo reconciliation: accepted=%d attached=%d error=%v", accepted, processed, err)
	}
	stop()
	feedWG.Wait()
	assertMediaAccounting(t, p)
	if feedErrors.Load() != 0 {
		t.Errorf("feed RPC errors during mixed load: %d", feedErrors.Load())
	}
	if readErrors != 0 {
		t.Errorf("chat, comments or social-feed errors during mixed load: %d", readErrors)
	}
	if minutes >= 60 {
		raw, s, e := c.rpc(tokens[127], "confirm_feed_photo", map[string]any{"requested_photo": expiringPhoto})
		if e != nil || s != 410 || !bytes.Contains(raw, []byte("photo_reservation_expired")) {
			t.Fatalf("real one-hour expiry: %d %s", s, raw)
		}
		if !scrollCleanupObserved || peakExpiredScrolls > 1000 || peakRecommendationConnections > 4 || peakMediaConnections > 4 {
			t.Errorf("maintenance or connection budget exceeded: cleanup=%t peak_expired=%d recommendation=%d media=%d",
				scrollCleanupObserved, peakExpiredScrolls, peakRecommendationConnections, peakMediaConnections)
		}
	}
	if withModel && (personalized.Load() == 0 || actions.Load() == 0 || actionErrors.Load() != 0 || len(interestLatency) == 0) {
		t.Fatalf("mixed recommendation coverage: personalized=%d actions=%d errors=%d probes=%d", personalized.Load(), actions.Load(), actionErrors.Load(), len(interestLatency))
	}
	if accepted == 0 || published == 0 || !storageRestored || !dbFault || !restarted {
		t.Fatal("soak did not cover required scenarios")
	}
	sort.Slice(completion, func(i, j int) bool { return completion[i] < completion[j] })
	feedMu.Lock()
	sort.Slice(feedLatency, func(i, j int) bool { return feedLatency[i] < feedLatency[j] })
	feedP95 := feedLatency[(len(feedLatency)-1)*95/100]
	feedP99 := feedLatency[(len(feedLatency)-1)*99/100]
	feedMu.Unlock()
	if minutes >= 60 && (feedP95 > 300*time.Millisecond || feedP99 > 800*time.Millisecond) {
		t.Errorf("soak For You page latency exceeded budget: p95=%s p99=%s", feedP95, feedP99)
	}
	summary := map[string]any{"wall_seconds": time.Since(start).Seconds(), "requested_minutes": minutes, "accepted_photos": accepted, "published_posts": published, "admission_busy": busy, "feed_requests": feedCount.Load(), "feed_errors": feedErrors.Load(), "feed_p95_ms": float64(feedP95.Microseconds()) / 1000, "publish_p95_ms": float64(completion[(len(completion)-1)*95/100].Microseconds()) / 1000, "source_bytes": sourceBytes.Load(), "hour_soak": minutes >= 60}
	summary["feed_p99_ms"] = float64(feedP99.Microseconds()) / 1000
	secondary := make(map[string]feedPerfSummary, len(readSurfaces))
	for _, surface := range readSurfaces {
		secondary[surface] = summarizeFeedTiming(readSamples[surface])
	}
	summary["secondary_http_ms"] = secondary
	summary["secondary_errors"] = readErrors
	summary["scroll_cleanup_observed"] = scrollCleanupObserved
	summary["peak_expired_scrolls"] = peakExpiredScrolls
	summary["peak_recommendation_connections"] = peakRecommendationConnections
	summary["peak_media_connections"] = peakMediaConnections
	summary["personalized_pages"] = personalized.Load()
	summary["actions"] = actions.Load()
	summary["action_errors"] = actionErrors.Load()
	summary["expected_deleted_actions"] = staleActions.Load()
	if len(interestLatency) > 0 {
		sort.Slice(interestLatency, func(i, j int) bool { return interestLatency[i] < interestLatency[j] })
		interestP95 := interestLatency[(len(interestLatency)-1)*95/100]
		summary["interest_p95_ms"] = float64(interestP95.Microseconds()) / 1000
		summary["interest_probes"] = len(interestLatency)
		if minutes >= 60 && interestP95 > 30*time.Second {
			t.Errorf("feedback p95 exceeded the 30 second budget: %s", interestP95)
		}
	}
	b, _ := json.MarshalIndent(summary, "", "  ")
	if err := os.WriteFile(filepath.Join(output, "summary.json"), b, 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("result=%s", b)
	t.Logf("private artifacts: %s", output)
}
