package tests

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"yap-algorithm/internal/adapters/photo"
	"yap-algorithm/internal/adapters/postgres"
	"yap-algorithm/internal/adapters/storage"
	"yap-algorithm/internal/application"
	"yap-algorithm/internal/domain"
)

func visionLocalJWT(t *testing.T) string {
	t.Helper()
	secret := os.Getenv("YAP_SUPABASE_LOCAL_JWT_SECRET")
	if secret == "" {
		t.Fatal("local JWT secret required")
	}
	encode := base64.RawURLEncoding.EncodeToString
	claims, _ := json.Marshal(map[string]any{"role": "yap_feed_vision_worker", "aud": "authenticated",
		"iat": time.Now().Unix(), "exp": time.Now().Add(time.Hour).Unix()})
	body := encode([]byte(`{"alg":"HS256","typ":"JWT"}`)) + "." + encode(claims)
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(body))
	return body + "." + encode(mac.Sum(nil))
}

func TestSupabaseLocalVision(t *testing.T) {
	p := stage5Pool(t)
	if os.Getenv("YAP_SUPABASE_MEDIA_TEST") != "1" || os.Getenv("YAP_VISION_TEST") != "1" {
		t.Skip("enable -WithMedia and YAP_VISION_TEST with the local model cache")
	}
	ctx := context.Background()
	var enabled, exposed bool
	err := p.QueryRow(ctx, `select enabled,
		has_function_privilege('authenticated','recsys.claim_vision_photo(uuid)','EXECUTE')
		or has_table_privilege('service_role','recsys.vision_embeddings','SELECT')
		or has_function_privilege('yap_feed_media_worker','recsys.complete_vision_photo(uuid,uuid,text,text,extensions.vector)','EXECUTE')
		from recsys.vision_settings`).Scan(&enabled, &exposed)
	if err != nil || enabled || exposed {
		t.Fatalf("vision pilot should be off and restricted: %v", err)
	}
	var count int
	if err := p.QueryRow(ctx, "select count(*) from recsys.vision_viewers").Scan(&count); err != nil || count != 0 {
		t.Fatal("local vision test requires an empty pilot")
	}
	f := sbUsers(t, p)
	svc := mediaHTTP{t: t, url: os.Getenv("YAP_SUPABASE_LOCAL_URL"), anon: os.Getenv("YAP_SUPABASE_LOCAL_ANON_KEY"),
		service: os.Getenv("YAP_SUPABASE_LOCAL_SERVICE_KEY"), client: &http.Client{Timeout: 15 * time.Second}}
	t.Cleanup(func() {
		sbExec(t, p, "delete from public.profiles where id=$1", f.author)
		for _, bucket := range []string{"feed-uploads", "feed-media"} {
			rows, err := p.Query(ctx, "select name from storage.objects where bucket_id=$1 and name like $2", bucket, f.author+"/%")
			if err != nil {
				t.Error(err)
				continue
			}
			var paths []string
			for rows.Next() {
				var path string
				if err := rows.Scan(&path); err != nil {
					t.Error(err)
				} else {
					paths = append(paths, path)
				}
			}
			rows.Close()
			if len(paths) > 0 {
				body, _ := json.Marshal(map[string]any{"prefixes": paths})
				if _, code := svc.request("DELETE", "/storage/v1/object/"+bucket, svc.service, "application/json", body); code != 200 {
					t.Errorf("fixture cleanup status %d", code)
				}
			}
		}
		sbExec(t, p, "delete from media.cleanup_jobs where path like $1", f.author+"/%")
	})
	sbExec(t, p, "insert into recsys.vision_viewers(slot,user_id) values(1,$1)", f.viewer)
	sbExec(t, p, "update recsys.vision_settings set enabled=true")
	t.Cleanup(func() { sbExec(t, p, "update recsys.vision_settings set enabled=false") })

	media, err := postgres.OpenMediaRepository(ctx, os.Getenv("YAP_SUPABASE_MEDIA_DSN"), 4)
	if err != nil {
		t.Fatal(err)
	}
	defer media.Close()
	objects, err := storage.NewMedia(svc.url, svc.anon, os.Getenv("YAP_SUPABASE_MEDIA_TOKEN"))
	if err != nil {
		t.Fatal(err)
	}
	processor := application.PhotoProcessor{Repository: media, Storage: objects, Preparer: photo.Processor{}}
	image := mediaImage(t, 384, 240, false)
	owner := localMediaJWT(t, f.author)
	type publication struct{ post, photo, path, original string }
	publish := func(audience string) publication {
		t.Helper()
		post, id := domain.UUID(), domain.UUID()
		reserve := stage5RPC(t, f.author, "reserve_feed_photo", map[string]any{
			"requested_post": post, "requested_photo": id, "requested_bytes": len(image), "requested_mime": "image/jpeg",
		}, 200)
		original := reserve["path"].(string)
		if _, status := svc.request("POST", "/storage/v1/object/feed-uploads/"+original, owner, "image/jpeg", image); status != 200 {
			t.Fatalf("fixture upload status %d", status)
		}
		stage5RPC(t, f.author, "confirm_feed_photo", map[string]string{"requested_photo": id}, 200)
		for pass := 0; pass < 20; pass++ {
			if worked, err := processor.Once(ctx); err != nil {
				t.Fatal(err)
			} else if !worked {
				break
			}
		}
		body := stage5RPC(t, f.author, "publish_feed_post", map[string]any{
			"requested_id": post, "requested_photos": []string{id}, "requested_audience": audience,
		}, 200)
		return publication{post, id, body["photos"].([]any)[0].(map[string]any)["path"].(string), original}
	}
	seed := publish("public")
	fenceTx, err := p.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer fenceTx.Rollback(ctx)
	if _, err := fenceTx.Exec(ctx, "select pg_advisory_xact_lock(78563402064)"); err != nil {
		t.Fatal(err)
	}
	target := publish("public")
	photoPublished := time.Now()
	if err := fenceTx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := p.QueryRow(ctx, "select recsys.reconcile_vision_jobs()").Scan(&count); err != nil || count != 1 {
		t.Fatalf("deferred public photo was not reconciled: %d %v", count, err)
	}
	private := publish("friends")
	like := stage5RPC(t, f.viewer, "get_feed_post", map[string]string{"requested_id": seed.post}, 200)
	stage5RPC(t, f.viewer, "apply_feed_action", map[string]any{
		"requested_id": domain.UUID(), "requested_delivery": like["delivery_id"], "requested_action": "like", "expected_version": 0,
	}, 200)
	fallback := stage5RPC(t, f.viewer, "get_for_you_feed", map[string]int{"requested_limit": 20}, 200)
	if len(fallback["items"].([]any)) == 0 {
		t.Fatal("media queue delayed the normal feed")
	}

	token := visionLocalJWT(t)
	read := func(bucket, path string) int {
		_, status := svc.request("GET", "/storage/v1/object/authenticated/"+bucket+"/"+path, token, "", nil)
		return status
	}
	if read("feed-media", target.path) == 200 || read("feed-uploads", target.original) == 200 {
		t.Fatal("vision token can read an unleased derivative or an original")
	}
	var wire []byte
	lease := domain.UUID()
	if err := p.QueryRow(ctx, "select recsys.claim_vision_photo($1)", lease).Scan(&wire); err != nil {
		t.Fatal(err)
	}
	var leased map[string]any
	if json.Unmarshal(wire, &leased) != nil || leased["id"] != seed.photo {
		t.Fatal("unexpected first vision job")
	}
	if read("feed-media", seed.path) != 200 || read("feed-media", private.path) == 200 || read("feed-uploads", seed.original) == 200 {
		t.Fatal("Storage did not enforce the vision job boundary")
	}
	var concurrentClaim []byte
	if err := p.QueryRow(ctx, "select recsys.claim_vision_photo($1)", domain.UUID()).Scan(&concurrentClaim); err != nil || concurrentClaim != nil {
		t.Fatal("second worker received a job while the global vision slot was occupied", err)
	}
	sbExec(t, p, "select recsys.retry_vision_photo($1,$2,false)", seed.photo, lease)
	var deferred bool
	if err := p.QueryRow(ctx, "select lease is null and available_at>clock_timestamp() from recsys.vision_jobs where photo_id=$1", seed.photo).Scan(&deferred); err != nil || !deferred {
		t.Fatal("transient failure did not release the lease with backoff", err)
	}
	sbExec(t, p, "update recsys.vision_jobs set available_at=clock_timestamp()-interval '1 minute' where photo_id=$1", seed.photo)
	lease = domain.UUID()
	if err := p.QueryRow(ctx, "select recsys.claim_vision_photo($1)", lease).Scan(&wire); err != nil || json.Unmarshal(wire, &leased) != nil || leased["id"] != seed.photo {
		t.Fatal("retry did not reclaim the original photo", err)
	}
	sbExec(t, p, "update recsys.vision_jobs set locked_until=clock_timestamp()-interval '1 second' where photo_id=$1", seed.photo)
	if read("feed-media", seed.path) == 200 {
		t.Fatal("expired vision lease can still read Storage")
	}
	var accepted bool
	err = p.QueryRow(ctx, "select recsys.complete_vision_photo($1,$2,$3,$4,array_prepend(1,array_fill(0,array[767]))::extensions.vector)",
		seed.photo, lease, leased["sha256"], leased["space"]).Scan(&accepted)
	if err != nil || accepted {
		t.Fatalf("expired lease was accepted: %v", err)
	}

	login := "vision_test_" + strings.ReplaceAll(domain.UUID(), "-", "")
	password := domain.UUID()
	sbExec(t, p, fmt.Sprintf("create role %s login password '%s' connection limit 1 in role yap_feed_vision_worker", login, password))
	t.Cleanup(func() { sbExec(t, p, "drop role "+login) })
	dsn, _ := url.Parse(os.Getenv("YAP_SUPABASE_LOCAL_ADMIN_DSN"))
	dsn.User = url.UserPassword(login, password)
	python := os.Getenv("YAP_VISION_PYTHON")
	if python == "" {
		t.Fatal("YAP_VISION_PYTHON must point to the local Python environment")
	}
	cache, _ := filepath.Abs("../.cache/huggingface")
	runCtx, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()
	cmd := exec.CommandContext(runCtx, python, "ml/vision_worker.py", "--max-jobs", "2")
	cmd.Dir = ".."
	for _, entry := range os.Environ() {
		name := strings.ToUpper(strings.SplitN(entry, "=", 2)[0])
		if !strings.HasPrefix(name, "YAP_SUPABASE_") && !strings.HasPrefix(name, "SUPABASE_") &&
			name != "DATABASE_URL" && name != "VISION_DATABASE_URL" && name != "HF_HOME" {
			cmd.Env = append(cmd.Env, entry)
		}
	}
	cmd.Env = append(cmd.Env, "VISION_DATABASE_URL="+dsn.String(), "SUPABASE_URL="+svc.url,
		"SUPABASE_ANON_KEY="+svc.anon, "SUPABASE_VISION_TOKEN="+token, "HF_HOME="+cache)
	out, err := cmd.CombinedOutput()
	if err != nil || !strings.Contains(string(out), `"encoded": 1`) || !strings.Contains(string(out), `"cached": 1`) {
		t.Fatalf("real vision worker or content cache failed: %v %s", err, out)
	}
	var indexed time.Time
	err = p.QueryRow(ctx, "select indexed_at from recsys.vision_embeddings where photo_id=$1 and extensions.vector_dims(embedding)=768", target.photo).Scan(&indexed)
	if err != nil {
		t.Fatal("prepared derivative did not produce a vector", err)
	}
	if err := p.QueryRow(ctx, "select count(*) from recsys.vision_embeddings where photo_id=$1", private.photo).Scan(&count); err != nil || count != 0 {
		t.Fatal("private photo was indexed")
	}
	_, rec := stage5Worker(t)
	ranker := application.Personalizer{Repository: rec.WithVision()}
	for pass := 0; pass < 30; pass++ {
		worked, err := ranker.Once(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !worked {
			break
		}
	}
	page := stage5RPC(t, f.viewer, "get_for_you_feed", map[string]int{"requested_limit": 20}, 200)
	if page["versions"].(map[string]any)["policy"] != "hybrid-v2-vision-siglip2-v1" {
		t.Fatal("vision snapshot was not published")
	}
	var present bool
	err = p.QueryRow(ctx, `select exists(select 1 from recsys.user_states u join recsys.feed_snapshot_items i
	  on i.snapshot_id=u.current_snapshot where u.user_id=$1 and i.post_id=$2 and 'vision'=any(i.sources))`,
		f.viewer, target.post).Scan(&present)
	if err != nil || !present {
		t.Fatal("visual candidate was not part of the personalized pool", err)
	}
	items, _ := json.Marshal(page["items"])
	if !strings.Contains(string(items), target.post) || strings.Contains(string(items), private.post) {
		t.Fatal("Supabase RPC did not return the expected authorized visual candidate")
	}
	t.Logf("photo publication to vector=%s, publication to observed pool=%s", indexed.Sub(photoPublished), time.Since(photoPublished))
	stage5RPC(t, f.viewer, "reset_recommendation_interests", map[string]string{"requested_id": domain.UUID()}, 200)
	fresh := stage5RPC(t, f.viewer, "get_for_you_feed", map[string]int{"requested_limit": 20}, 200)
	if fresh["versions"].(map[string]any)["policy"] == "hybrid-v2-vision-siglip2-v1" {
		t.Fatal("reset retained the old visual profile")
	}
}
