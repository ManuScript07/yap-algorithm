package tests

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"yap-algorithm/internal/adapters/photo"
	"yap-algorithm/internal/adapters/postgres"
	"yap-algorithm/internal/adapters/storage"
	"yap-algorithm/internal/application"
	"yap-algorithm/internal/domain"
)

// A loopback-only transport fault; shared Postgres stays available to the app.
// Closing both directions also interrupts existing pooled connections.
type mediaDatabaseProxy struct {
	listener net.Listener
	mu       sync.Mutex
	down     bool
	conns    map[net.Conn]bool
	workers  sync.WaitGroup
}

func newMediaDatabaseProxy(t *testing.T) *mediaDatabaseProxy {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &mediaDatabaseProxy{listener: listener, conns: map[net.Conn]bool{}}
	p.workers.Add(1)
	go func() {
		defer p.workers.Done()
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			p.workers.Add(1)
			go func() {
				defer p.workers.Done()
				defer client.Close()
				p.mu.Lock()
				down := p.down
				p.mu.Unlock()
				if down {
					return
				}
				server, err := net.DialTimeout("tcp", "127.0.0.1:54322", time.Second)
				if err != nil {
					return
				}
				defer server.Close()
				p.mu.Lock()
				if p.down {
					p.mu.Unlock()
					return
				}
				p.conns[client] = true
				p.conns[server] = true
				p.mu.Unlock()
				defer func() { p.mu.Lock(); delete(p.conns, client); delete(p.conns, server); p.mu.Unlock() }()
				sent := make(chan struct{})
				go func() { io.Copy(server, client); server.Close(); close(sent) }()
				io.Copy(client, server)
				client.Close()
				server.Close()
				<-sent
			}()
		}
	}()
	t.Cleanup(func() { p.setDown(true); listener.Close(); p.workers.Wait() })
	return p
}
func (p *mediaDatabaseProxy) setDown(down bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.down = down
	if down {
		for c := range p.conns {
			c.Close()
		}
	}
}

// The database has committed, but the caller did not receive its acknowledgement.
// The application's failure path must not turn a ready photo back into a job.
type lostMediaAcknowledgement struct{ application.MediaRepository }

func (r lostMediaAcknowledgement) CompletePhoto(ctx context.Context, j application.PhotoJob, p application.PreparedPhoto) (bool, error) {
	accepted, err := r.MediaRepository.CompletePhoto(ctx, j, p)
	if err != nil || !accepted {
		return accepted, err
	}
	return false, errors.New("injected acknowledgement loss after commit")
}

func TestSupabaseLocalMediaDBOutage(t *testing.T) {
	p := capacityAdmin(t)
	ctx := context.Background()
	var running int
	if err := p.QueryRow(ctx, "select count(*)from media.workers where expires_at>statement_timestamp()").Scan(&running); err != nil || running != 0 {
		t.Fatal("stop regular media workers before outage test", err)
	}
	f := sbUsers(t, p)
	ids := []string{f.author, f.viewer, f.stranger}
	c := soakHTTP{os.Getenv("YAP_SUPABASE_LOCAL_URL"), os.Getenv("YAP_SUPABASE_LOCAL_ANON_KEY"), &http.Client{Timeout: 20 * time.Second}}
	token := localMediaJWT(t, f.author)
	t.Cleanup(func() {
		sbExec(t, p, "delete from public.profiles where id=any($1::uuid[])", ids)
		for _, bucket := range []string{"feed-uploads", "feed-media"} {
			rows, err := p.Query(ctx, "select name from storage.objects where bucket_id=$1 and split_part(name,'/',1)=any($2::text[])", bucket, ids)
			if err != nil {
				t.Error(err)
				continue
			}
			var paths []string
			for rows.Next() {
				var path string
				rows.Scan(&path)
				paths = append(paths, path)
			}
			rows.Close()
			if len(paths) > 0 {
				body, _ := json.Marshal(map[string]any{"prefixes": paths})
				_, status, err := c.call("DELETE", "/storage/v1/object/"+bucket, os.Getenv("YAP_SUPABASE_LOCAL_SERVICE_KEY"), "application/json", body)
				if err != nil || status != 200 {
					t.Errorf("fixture Storage cleanup status=%d", status)
				}
			}
		}
		sbExec(t, p, "delete from media.cleanup_jobs where split_part(path,'/',1)=any($1::text[])", ids)
		assertMediaAccounting(t, p)
	})
	proxy := newMediaDatabaseProxy(t)
	dsn, err := url.Parse(os.Getenv("YAP_SUPABASE_MEDIA_DSN"))
	if err != nil {
		t.Fatal("invalid local media identity")
	}
	dsn.Host = proxy.listener.Addr().String()
	repo, err := postgres.OpenMediaRepository(ctx, dsn.String(), 2)
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Close()
	if err := repo.Register(ctx, 1); err != nil {
		t.Fatal(err)
	}
	defer repo.Unregister(ctx)
	objects, err := storage.NewMedia(c.base, c.key, os.Getenv("YAP_SUPABASE_MEDIA_TOKEN"))
	if err != nil {
		t.Fatal(err)
	}
	data := mediaImage(t, 640, 480, false)
	upload := func() (post, id string) {
		post, id = domain.UUID(), domain.UUID()
		body, status, err := c.rpc(token, "reserve_feed_photo", map[string]any{"requested_post": post, "requested_photo": id, "requested_bytes": len(data), "requested_mime": "image/jpeg"})
		if err != nil || status != 200 {
			t.Fatalf("reserve status=%d", status)
		}
		var intent struct {
			Path string `json:"path"`
		}
		if err := json.Unmarshal(body, &intent); err != nil {
			t.Fatal(err)
		}
		if _, status, err := c.call("POST", "/storage/v1/object/feed-uploads/"+intent.Path, token, "image/jpeg", data); err != nil || status != 200 {
			t.Fatalf("upload status=%d", status)
		}
		if _, status, err := c.rpc(token, "confirm_feed_photo", map[string]any{"requested_photo": id}); err != nil || status != 200 {
			t.Fatalf("confirm status=%d", status)
		}
		return
	}
	post, id := upload()
	old, found, err := repo.ClaimPhoto(ctx)
	if err != nil || !found || old.ID != id {
		t.Fatal("initial claim", err)
	}
	prepared, err := (photo.Processor{}).Prepare(ctx, data, "image/jpeg")
	if err != nil {
		t.Fatal(err)
	}
	if err := objects.Upload(ctx, old.ImagePath, prepared.Image); err != nil {
		t.Fatal(err)
	}
	if err := objects.Upload(ctx, old.ThumbnailPath, prepared.Thumbnail); err != nil {
		t.Fatal(err)
	}
	proxy.setDown(true)
	if accepted, err := repo.CompletePhoto(ctx, old, prepared); err == nil || accepted {
		t.Fatal("completion unexpectedly succeeded during DB outage")
	}
	if err := repo.Heartbeat(ctx); err == nil {
		t.Fatal("heartbeat ignored DB outage")
	}
	var state string
	if err := p.QueryRow(ctx, "select state from feed.photo_uploads where id=$1", id).Scan(&state); err != nil || state != "processing" {
		t.Fatal("failed completion lost durable work", err)
	}
	proxy.setDown(false)
	replacement, err := postgres.OpenMediaRepository(ctx, os.Getenv("YAP_SUPABASE_MEDIA_DSN"), 2)
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()
	if err := replacement.Register(ctx, 1); err != nil {
		t.Fatal(err)
	}
	defer replacement.Unregister(ctx)
	t.Log("DB transport restored; waiting for the real two-minute job lease, without editing timestamps")
	deadline := time.Now().Add(135 * time.Second)
	heartbeat := time.Now()
	progress := time.Now()
	var next application.PhotoJob
	for time.Now().Before(deadline) {
		if time.Since(heartbeat) > 5*time.Second {
			if err := replacement.Heartbeat(ctx); err != nil {
				t.Fatal(err)
			}
			heartbeat = time.Now()
		}
		job, ok, err := replacement.ClaimPhoto(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			next = job
			break
		}
		if time.Since(progress) > 30*time.Second {
			t.Log("waiting for expired lease recovery")
			progress = time.Now()
		}
		time.Sleep(250 * time.Millisecond)
	}
	if next.ID != old.ID || next.Lease == old.Lease || next.ImagePath == old.ImagePath {
		t.Fatal("durable job did not receive an isolated recovery lease")
	}
	if accepted, err := repo.CompletePhoto(ctx, old, prepared); err != nil || accepted {
		t.Fatal("stale result won after DB recovery", err)
	}
	if err := objects.Upload(ctx, next.ImagePath, prepared.Image); err != nil {
		t.Fatal(err)
	}
	if err := objects.Upload(ctx, next.ThumbnailPath, prepared.Thumbnail); err != nil {
		t.Fatal(err)
	}
	if accepted, err := replacement.CompletePhoto(ctx, next, prepared); err != nil || !accepted {
		t.Fatal("recovered result not accepted", err)
	}
	for i := 0; i < 2; i++ {
		if _, status, err := c.rpc(token, "publish_feed_post", map[string]any{"requested_id": post, "requested_photos": []string{id}}); err != nil || status != 200 {
			t.Fatalf("recovered publication status=%d", status)
		}
	}
	// A second real object exercises ambiguous completion: persistence succeeded,
	// the application observed an error, and its retry/failure path must be safe.
	secondPost, second := upload()
	processor := application.PhotoProcessor{Repository: lostMediaAcknowledgement{replacement}, Storage: objects, Preparer: photo.Processor{}}
	if worked, err := processor.Once(ctx); !worked || err == nil {
		t.Fatal("acknowledgement fault did not execute")
	}
	if err := p.QueryRow(ctx, "select state from feed.photo_uploads where id=$1", second).Scan(&state); err != nil || state != "ready" {
		t.Fatal("lost acknowledgement reverted committed photo", err)
	}
	if _, status, err := c.rpc(token, "publish_feed_post", map[string]any{"requested_id": secondPost, "requested_photos": []string{second}}); err != nil || status != 200 {
		t.Fatalf("ambiguous completion publication status=%d", status)
	}
	assertMediaAccounting(t, p)
}
