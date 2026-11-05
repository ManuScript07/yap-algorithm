package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"image"
	"image/color"
	"image/jpeg"
	"image/png"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"yap-algorithm/internal/adapters/photo"
	"yap-algorithm/internal/adapters/postgres"
	"yap-algorithm/internal/adapters/storage"
	"yap-algorithm/internal/application"
	"yap-algorithm/internal/domain"
)

type mediaHTTP struct {
	t                  *testing.T
	url, anon, service string
	client             *http.Client
}

func (c mediaHTTP) request(method, path, token, mime string, data []byte) ([]byte, int) {
	c.t.Helper()
	req, err := http.NewRequest(method, c.url+path, bytes.NewReader(data))
	if err != nil {
		c.t.Fatal(err)
	}
	req.Header.Set("apikey", c.anon)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", mime)
	req.Header.Set("x-upsert", "false")
	response, err := c.client.Do(req)
	if err != nil {
		c.t.Fatal("local Supabase request failed")
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 6*1024*1024))
	if err != nil {
		c.t.Fatal(err)
	}
	return body, response.StatusCode
}

func (c mediaHTTP) rpc(token, name string, payload any) (map[string]any, int) {
	body, _ := json.Marshal(payload)
	response, status := c.request("POST", "/rest/v1/rpc/"+name, token, "application/json", body)
	var out map[string]any
	json.Unmarshal(response, &out)
	return out, status
}

func mediaImage(t testing.TB, width, height int, pngFormat bool) []byte {
	t.Helper()
	im := image.NewRGBA(image.Rect(0, 0, width, height))
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			im.SetRGBA(x, y, color.RGBA{uint8(x), uint8(y), uint8(x + y), 255})
		}
	}
	var out bytes.Buffer
	var err error
	if pngFormat {
		err = png.Encode(&out, im)
	} else {
		err = jpeg.Encode(&out, im, &jpeg.Options{Quality: 85})
	}
	if err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func TestSupabaseLocalMedia(t *testing.T) {
	if os.Getenv("YAP_SUPABASE_MEDIA_TEST") != "1" {
		t.Skip("enable -WithMedia after media-local.ps1 -Action setup")
	}
	dsn := os.Getenv("YAP_SUPABASE_LOCAL_ADMIN_DSN")
	u, err := url.Parse(dsn)
	if err != nil || u.Hostname() != "127.0.0.1" || u.Port() != "54322" || os.Getenv("YAP_SUPABASE_LOCAL_URL") != "http://127.0.0.1:54321" {
		t.Fatal("media tests only accept explicit local YapChat Supabase")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal("cannot open local database")
	}
	t.Cleanup(admin.Close)
	repo, err := postgres.OpenMediaRepository(ctx, os.Getenv("YAP_SUPABASE_MEDIA_DSN"), 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(repo.Close)
	objects, err := storage.NewMedia(os.Getenv("YAP_SUPABASE_LOCAL_URL"), os.Getenv("YAP_SUPABASE_LOCAL_ANON_KEY"), os.Getenv("YAP_SUPABASE_MEDIA_TOKEN"))
	if err != nil {
		t.Fatal(err)
	}
	processor := application.PhotoProcessor{Repository: repo, Storage: objects, Preparer: photo.Processor{}}
	cleaner := application.MediaCleaner{Repository: repo, Storage: objects}
	c := mediaHTTP{t: t, url: os.Getenv("YAP_SUPABASE_LOCAL_URL"), anon: os.Getenv("YAP_SUPABASE_LOCAL_ANON_KEY"),
		service: os.Getenv("YAP_SUPABASE_LOCAL_SERVICE_KEY"), client: &http.Client{Timeout: 20 * time.Second}}
	var userIDs []string
	t.Cleanup(func() {
		for _, id := range userIDs {
			// Remove only this run's synthetic users/objects, always via Storage
			// API. The production lifecycle is asserted separately below.
			sbExec(t, admin, "delete from private.global_account_bans where target_user_id=$1", id)
			sbExec(t, admin, "delete from public.profiles where id=$1", id)
			for _, bucket := range []string{"feed-uploads", "feed-media"} {
				rows, err := admin.Query(ctx, "select name from storage.objects where bucket_id=$1 and name like $2", bucket, id+"/%")
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
				if len(paths) > 0 {
					body, _ := json.Marshal(map[string]any{"prefixes": paths})
					_, status := c.request("DELETE", "/storage/v1/object/"+bucket, c.service, "application/json", body)
					if status != 200 {
						t.Errorf("fixture Storage cleanup failed: %d", status)
					}
				}
			}
			sbExec(t, admin, "delete from media.cleanup_jobs where path like $1", id+"/%")
			sbExec(t, admin, "delete from auth.users where id=$1", id)
		}
	})
	newUser := func() (id, token string) {
		email, password := "media-"+domain.UUID()+"@example.com", domain.UUID()
		body, _ := json.Marshal(map[string]any{"email": email, "password": password, "email_confirm": true})
		response, status := c.request("POST", "/auth/v1/admin/users", c.service, "application/json", body)
		var created struct {
			ID string `json:"id"`
		}
		json.Unmarshal(response, &created)
		if status != 200 || created.ID == "" {
			t.Fatalf("Auth fixture creation: %d", status)
		}
		userIDs = append(userIDs, created.ID)
		body, _ = json.Marshal(map[string]string{"email": email, "password": password})
		response, status = c.request("POST", "/auth/v1/token?grant_type=password", c.anon, "application/json", body)
		var session struct {
			Token string `json:"access_token"`
		}
		json.Unmarshal(response, &session)
		if status != 200 || session.Token == "" {
			t.Fatalf("Auth fixture login: %d", status)
		}
		return created.ID, session.Token
	}
	owner, ownerToken := newUser()
	viewer, viewerToken := newUser()
	stranger, strangerToken := newUser()
	f := sbFixture{admin: admin, author: owner, viewer: viewer, stranger: stranger}
	reserve := func(post, id, mime string, size int) map[string]any {
		out, status := c.rpc(ownerToken, "reserve_feed_photo", map[string]any{"requested_post": post, "requested_photo": id, "requested_bytes": size, "requested_mime": mime})
		if status != 200 {
			t.Fatalf("reserve photo: %d %v", status, out)
		}
		return out
	}
	upload := func(post string, data []byte, mime string) string {
		id := domain.UUID()
		intent := reserve(post, id, mime, len(data))
		_, status := c.request("POST", "/storage/v1/object/feed-uploads/"+intent["path"].(string), ownerToken, mime, data)
		if status != 200 {
			t.Fatalf("upload: %d", status)
		}
		if result, status := c.rpc(ownerToken, "confirm_feed_photo", map[string]string{"requested_photo": id}); status != 200 || result["state"] != "queued" {
			t.Fatalf("confirm: %d %v", status, result)
		}
		return id
	}
	drain := func() {
		for i := 0; i < 50; i++ {
			worked, err := processor.Once(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if !worked {
				return
			}
		}
		t.Fatal("media queue did not drain")
	}
	var publicPost, privatePost, publicPath, privatePath string
	jpegData := mediaImage(t, 320, 160, false)
	pngData := mediaImage(t, 160, 320, true)
	t.Run("actual_storage_prepare_publish_and_client_contract", func(t *testing.T) {
		publicPost = domain.UUID()
		photos := []string{upload(publicPost, jpegData, "image/jpeg"), upload(publicPost, pngData, "image/png")}
		out, status := c.rpc(ownerToken, "publish_feed_post", map[string]any{"requested_id": publicPost, "requested_photos": photos})
		if status < 300 || out["message"] != "photos_not_ready" {
			t.Fatal("unprocessed photos were published")
		}
		cmd := exec.Command("go", "run", "./cmd/media-worker", "-once")
		cmd.Dir = ".."
		for _, entry := range os.Environ() {
			name := strings.SplitN(entry, "=", 2)[0]
			if !strings.HasPrefix(name, "YAP_SUPABASE_") && name != "MEDIA_DATABASE_URL" && name != "MEDIA_STORAGE_TOKEN" && name != "SUPABASE_URL" && name != "SUPABASE_PUBLISHABLE_KEY" {
				cmd.Env = append(cmd.Env, entry)
			}
		}
		cmd.Env = append(cmd.Env, "MEDIA_DATABASE_URL="+os.Getenv("YAP_SUPABASE_MEDIA_DSN"), "MEDIA_STORAGE_TOKEN="+os.Getenv("YAP_SUPABASE_MEDIA_TOKEN"),
			"SUPABASE_URL="+c.url, "SUPABASE_PUBLISHABLE_KEY="+c.anon)
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("separate media worker: %v %s", err, output)
		}
		payload := map[string]any{"requested_id": publicPost, "requested_photos": []string{photos[1], photos[0]},
			"requested_location": map[string]float64{"latitude": 55.75, "longitude": 37.61}}
		out, status = c.rpc(ownerToken, "publish_feed_post", payload)
		if status != 200 || out["text"] != "" || len(out["photos"].([]any)) != 2 {
			t.Fatalf("photo-only publication: %d %v", status, out)
		}
		first := out["photos"].([]any)[0].(map[string]any)
		if first["id"] != photos[1] || first["width"] != float64(160) {
			t.Fatal("requested photo order or dimensions lost")
		}
		publicPath = first["path"].(string)
		if _, status := c.rpc(ownerToken, "publish_feed_post", payload); status != 200 {
			t.Fatal("publication retry not idempotent")
		}
		if body, status := c.request("GET", "/storage/v1/object/authenticated/feed-media/"+publicPath, viewerToken, "", nil); status != 200 || !bytes.HasPrefix(body, []byte{255, 216}) {
			t.Fatalf("authorized prepared image: %d", status)
		}
		var indexed int
		admin.QueryRow(ctx, "select count(*)from recsys.index_jobs where post_id=$1", publicPost).Scan(&indexed)
		if indexed != 0 {
			t.Fatal("photo-only post created a fake text embedding job")
		}
	})
	t.Run("originals_signing_listing_upsert_and_foreign_paths_are_closed", func(t *testing.T) {
		var source string
		admin.QueryRow(ctx, "select source_path from feed.photo_uploads where draft_id=$1 order by slot limit 1", publicPost).Scan(&source)
		for _, token := range []string{ownerToken, viewerToken, c.anon} {
			if _, status := c.request("GET", "/storage/v1/object/authenticated/feed-uploads/"+source, token, "", nil); status == 200 {
				t.Fatal("original readable")
			}
		}
		if _, status := c.request("GET", "/storage/v1/object/public/feed-media/"+publicPath, c.anon, "", nil); status == 200 {
			t.Fatal("private bucket readable through public endpoint")
		}
		for _, path := range []string{"/storage/v1/object/sign/feed-media/" + publicPath, "/storage/v1/object/list/feed-media"} {
			body := []byte(`{"expiresIn":3600,"prefix":""}`)
			response, status := c.request("POST", path, viewerToken, "application/json", body)
			if strings.Contains(path, "/sign/") && status < 300 {
				t.Fatal("client minted a bypass link")
			}
			if strings.Contains(path, "/list/") && status == 200 && string(bytes.TrimSpace(response)) != "[]" {
				t.Fatal("bucket contents enumerated")
			}
		}
		if _, status := c.request("POST", "/storage/v1/object/feed-media/"+publicPath, ownerToken, "image/jpeg", jpegData); status < 300 {
			t.Fatal("client overwrote prepared image")
		}
		post, id := domain.UUID(), domain.UUID()
		intent := reserve(post, id, "image/jpeg", len(jpegData))
		if _, status := c.request("POST", "/storage/v1/object/upload/sign/feed-uploads/"+intent["path"].(string), ownerToken, "application/json", []byte(`{}`)); status < 300 {
			t.Fatal("signed upload could outlive its reservation")
		}
		if _, status := c.request("POST", "/storage/v1/object/feed-uploads/"+intent["path"].(string), strangerToken, "image/jpeg", jpegData); status < 300 {
			t.Fatal("foreign upload accepted")
		}
		if _, status := c.rpc(strangerToken, "confirm_feed_photo", map[string]string{"requested_photo": id}); status < 300 {
			t.Fatal("foreign confirmation accepted")
		}
		c.rpc(ownerToken, "cancel_feed_draft", map[string]string{"requested_post": post})
	})
	t.Run("current_friendship_block_ban_and_deletion_on_download", func(t *testing.T) {
		privatePost = domain.UUID()
		id := upload(privatePost, jpegData, "image/jpeg")
		drain()
		out, status := c.rpc(ownerToken, "publish_feed_post", map[string]any{"requested_id": privatePost, "requested_audience": "friends", "requested_photos": []string{id}})
		if status != 200 {
			t.Fatal(out)
		}
		privatePath = out["photos"].([]any)[0].(map[string]any)["path"].(string)
		path := "/storage/v1/object/authenticated/feed-media/" + privatePath
		if _, status := c.request("GET", path, viewerToken, "", nil); status == 200 {
			t.Fatal("nonfriend read friends-only media")
		}
		sbExec(t, admin, "insert into public.friendships(user_one_id,user_two_id)values(least($1::uuid,$2::uuid),greatest($1::uuid,$2::uuid))", owner, viewer)
		if _, status := c.request("GET", path, viewerToken, "", nil); status != 200 {
			t.Fatalf("friend download: %d", status)
		}
		sbExec(t, admin, "delete from public.friendships where user_one_id=least($1::uuid,$2::uuid)and user_two_id=greatest($1::uuid,$2::uuid)", owner, viewer)
		if _, status := c.request("GET", path, viewerToken, "", nil); status == 200 {
			t.Fatal("friend removal did not revoke download")
		}
		sbExec(t, admin, "insert into public.user_blocks(blocker_user_id,blocked_user_id)values($1,$2)", owner, viewer)
		if _, status := c.request("GET", "/storage/v1/object/authenticated/feed-media/"+publicPath, viewerToken, "", nil); status == 200 {
			t.Fatal("block did not revoke public-post media")
		}
		sbExec(t, admin, "delete from public.user_blocks where blocker_user_id=$1 and blocked_user_id=$2", owner, viewer)
		sbExec(t, admin, "insert into private.global_account_bans(target_user_id,note)values($1,'media test')", owner)
		if _, status := c.request("GET", "/storage/v1/object/authenticated/feed-media/"+publicPath, viewerToken, "", nil); status == 200 {
			t.Fatal("global author ban did not revoke media")
		}
		sbExec(t, admin, "delete from private.global_account_bans where target_user_id=$1", owner)
		if _, status := c.rpc(ownerToken, "delete_feed_post", map[string]string{"requested_id": privatePost}); status != 204 && status != 200 {
			t.Fatal("post deletion failed")
		}
		if _, status := c.request("GET", path, ownerToken, "", nil); status == 200 {
			t.Fatal("deleted post media readable even by author")
		}
	})
	t.Run("geotag_is_optional_and_precise_permission_is_live", func(t *testing.T) {
		sbExec(t, admin, "insert into public.user_locations(user_id,latitude,longitude)values($1,55.76,37.62)on conflict(user_id)do update set latitude=excluded.latitude,longitude=excluded.longitude", viewer)
		out, status := c.rpc(viewerToken, "get_feed_post", map[string]string{"requested_id": publicPost})
		if status != 200 || out["location"].(map[string]any)["nearby"] != true || out["location"].(map[string]any)["latitude"] != nil {
			t.Fatal("nearby marker leaked precise location")
		}
		sbExec(t, admin, "insert into public.friendships(user_one_id,user_two_id)values(least($1::uuid,$2::uuid),greatest($1::uuid,$2::uuid))", owner, viewer)
		out, _ = c.rpc(viewerToken, "get_feed_post", map[string]string{"requested_id": publicPost})
		if out["location"].(map[string]any)["latitude"] != float64(55.75) {
			t.Fatal("permitted friend did not receive original post location")
		}
		sbExec(t, admin, "insert into private.precise_location_exclusions(owner_user_id,viewer_user_id)values($1,$2)", owner, viewer)
		out, _ = c.rpc(viewerToken, "get_feed_post", map[string]string{"requested_id": publicPost})
		if out["location"].(map[string]any)["latitude"] != nil {
			t.Fatal("precise-location exclusion ignored")
		}
		out, _ = c.rpc(strangerToken, "get_feed_post", map[string]string{"requested_id": publicPost})
		if out["location"] != nil {
			t.Fatal("far/unknown location disclosed")
		}
		sbExec(t, admin, "update public.user_locations set updated_at=statement_timestamp()-interval '13 hours'where user_id=$1", viewer)
		out, _ = c.rpc(viewerToken, "get_feed_post", map[string]string{"requested_id": publicPost})
		if out["location"] != nil {
			t.Fatal("stale viewer location produced a nearby marker")
		}
	})
	t.Run("photo_feedback_retrieves_author_without_text_embedding", func(t *testing.T) {
		sbExec(t, admin, "delete from public.friendships where user_one_id=least($1::uuid,$2::uuid)and user_two_id=greatest($1::uuid,$2::uuid)", owner, stranger)
		out, _ := c.rpc(strangerToken, "get_feed_post", map[string]string{"requested_id": publicPost})
		result, status := c.rpc(strangerToken, "apply_feed_action", map[string]any{"requested_id": domain.UUID(), "requested_delivery": out["delivery_id"], "requested_action": "like", "expected_version": 0})
		if status != 200 || result["liked"] != true {
			t.Fatal(result)
		}
		sbExec(t, admin, "update recsys.refresh_jobs set available_at=statement_timestamp()where user_id=$1", stranger)
		var lease string
		admin.QueryRow(ctx, "select lease::text from recsys.claim_refresh_job($1::uuid)", domain.UUID()).Scan(&lease)
		// Claim may pick another active user first; target this fixture's lease
		// explicitly for a bounded source-level assertion.
		lease = domain.UUID()
		sbExec(t, admin, "update recsys.refresh_jobs set lease=$2::uuid,locked_until=statement_timestamp()+interval '1 minute'where user_id=$1", stranger, lease)
		var raw []byte
		if err := admin.QueryRow(ctx, "select recsys.load_refresh_candidates($1::uuid,$2::uuid,1,$3,'[]')", stranger, lease, sbModel).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		if !bytes.Contains(raw, []byte("author_affinity")) {
			t.Fatal("photo like had no effect on candidate retrieval")
		}
	})
	t.Run("parallel_reservations_max_three_and_invalid_uploads", func(t *testing.T) {
		post := domain.UUID()
		var group sync.WaitGroup
		results := make(chan int, 4)
		for i := 0; i < 4; i++ {
			group.Add(1)
			go func() {
				defer group.Done()
				_, status := c.rpc(ownerToken, "reserve_feed_photo", map[string]any{"requested_post": post, "requested_photo": domain.UUID(), "requested_bytes": len(jpegData), "requested_mime": "image/jpeg"})
				results <- status
			}()
		}
		group.Wait()
		close(results)
		successes := 0
		for status := range results {
			if status == 200 {
				successes++
			}
		}
		if successes != 3 {
			t.Fatalf("parallel reservation successes: %d", successes)
		}
		c.rpc(ownerToken, "cancel_feed_draft", map[string]string{"requested_post": post})
		// Flutter's standard upload can use multipart: framing bytes are not
		// part of the reserved file size. Validate the final measured file too.
		post = domain.UUID()
		var multipartBody bytes.Buffer
		writer := multipart.NewWriter(&multipartBody)
		headers := textproto.MIMEHeader{}
		headers.Set("Content-Disposition", `form-data; name="file"; filename="photo.jpg"`)
		headers.Set("Content-Type", "image/jpeg")
		part, err := writer.CreatePart(headers)
		if err != nil {
			t.Fatal(err)
		}
		part.Write(jpegData)
		writer.Close()
		for _, size := range []int{len(jpegData), 1} {
			intent := reserve(post, domain.UUID(), "image/jpeg", size)
			_, status := c.request("POST", "/storage/v1/object/feed-uploads/"+intent["path"].(string), ownerToken, writer.FormDataContentType(), multipartBody.Bytes())
			if size == len(jpegData) && status != 200 {
				t.Fatalf("valid multipart rejected: %d", status)
			}
			if size == 1 && status < 300 {
				t.Fatal("underdeclared multipart accepted")
			}
		}
		c.rpc(ownerToken, "cancel_feed_draft", map[string]string{"requested_post": post})
		post = domain.UUID()
		bad := append([]byte{}, jpegData[:len(jpegData)/2]...)
		id := upload(post, bad, "image/jpeg")
		drain()
		var state string
		admin.QueryRow(ctx, "select state from feed.photo_uploads where id=$1", id).Scan(&state)
		if state != "rejected" {
			t.Fatal("corrupt image did not become terminally rejected")
		}
		if out, status := c.rpc(ownerToken, "publish_feed_post", map[string]any{"requested_id": post, "requested_photos": []string{id}}); status < 300 {
			t.Fatal("rejected photo published", out)
		}
		c.rpc(ownerToken, "cancel_feed_draft", map[string]string{"requested_post": post})
		post, id = domain.UUID(), domain.UUID()
		intent := reserve(post, id, "image/jpeg", application.MaxPhotoBytes)
		if _, status := c.request("POST", "/storage/v1/object/feed-uploads/"+intent["path"].(string), ownerToken, "image/jpeg", make([]byte, application.MaxPhotoBytes+1)); status < 300 {
			t.Fatal("bucket accepted more than 5 MiB")
		}
		c.rpc(ownerToken, "cancel_feed_draft", map[string]string{"requested_post": post})
	})
	t.Run("cleanup_is_physical_idempotent_and_cannot_ack_existing_object", func(t *testing.T) {
		var before int
		admin.QueryRow(ctx, "select count(*)from storage.objects where bucket_id='feed-media'and name=$1", privatePath).Scan(&before)
		if before != 1 {
			t.Fatal("deletion must go through durable cleanup, not raw SQL")
		}
		sbExec(t, admin, "update media.cleanup_jobs set available_at=statement_timestamp()where path like $1", owner+"/%")
		for i := 0; i < 100; i++ {
			worked, err := cleaner.Once(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if !worked {
				break
			}
		}
		var after int
		admin.QueryRow(ctx, "select count(*)from storage.objects where bucket_id='feed-media'and name=$1", privatePath).Scan(&after)
		if after != 0 {
			t.Fatal("deleted image still exists in actual Storage")
		}
		if _, status := c.request("GET", "/storage/v1/object/authenticated/feed-media/"+publicPath, strangerToken, "", nil); status != 200 {
			t.Fatal("cleanup removed a live post image")
		}
		lease := domain.UUID()
		sbExec(t, admin, "insert into media.cleanup_jobs(bucket,path,lease,locked_until)values('feed-media',$1,$2::uuid,statement_timestamp()+interval '1 minute')on conflict(bucket,path)do update set lease=excluded.lease,locked_until=excluded.locked_until", publicPath, lease)
		if err := repo.CompleteCleanup(ctx, application.MediaCleanupJob{Bucket: "feed-media", Path: publicPath, Lease: lease}, true); err == nil {
			t.Fatal("false physical deletion acknowledgement accepted")
		}
		var pending int
		admin.QueryRow(ctx, "select count(*)from media.cleanup_jobs where bucket='feed-media'and path=$1", publicPath).Scan(&pending)
		if pending != 1 {
			t.Fatal("cleanup acknowledged an existing object")
		}
	})
	t.Run("expired_leases_reservations_and_metadata_mismatches", func(t *testing.T) {
		post, id := domain.UUID(), domain.UUID()
		intent := reserve(post, id, "image/png", len(jpegData))
		if _, status := c.request("POST", "/storage/v1/object/feed-uploads/"+intent["path"].(string), ownerToken, "image/jpeg", jpegData); status < 300 {
			t.Fatal("Storage accepted mismatched MIME")
		}
		if _, status := c.rpc(ownerToken, "confirm_feed_photo", map[string]string{"requested_photo": id}); status < 300 {
			t.Fatal("declared MIME was trusted over actual Storage metadata")
		}
		c.rpc(ownerToken, "cancel_feed_draft", map[string]string{"requested_post": post})
		post, id = domain.UUID(), domain.UUID()
		intent = reserve(post, id, "image/jpeg", 1)
		if _, status := c.request("POST", "/storage/v1/object/feed-uploads/"+intent["path"].(string), ownerToken, "image/jpeg", jpegData); status < 300 {
			t.Fatal("Storage accepted an underdeclared size")
		}
		var stored bool
		if err := admin.QueryRow(ctx, "select exists(select 1 from storage.objects where bucket_id='feed-uploads'and name=$1)", intent["path"]).Scan(&stored); err != nil || stored {
			t.Fatal("mismatch consumed persistent Storage outside its reservation", err)
		}
		// Storage may finalize through its internal database role after RLS.
		// The actual-byte guard must still reject that privileged finalization.
		_, err := admin.Exec(ctx, `insert into storage.objects(bucket_id,name,owner_id,metadata)values('feed-uploads',$1,$2,'{"size":2,"mimetype":"image/jpeg"}')`, intent["path"], owner)
		sbErrorCode(t, err, "22023")
		c.rpc(ownerToken, "cancel_feed_draft", map[string]string{"requested_post": post})
		post = domain.UUID()
		id = upload(post, jpegData, "image/jpeg")
		job, found, err := repo.ClaimPhoto(ctx)
		if err != nil || !found || job.ID != id {
			t.Fatal("cannot claim lease fixture", err)
		}
		prepared, err := (photo.Processor{}).Prepare(ctx, jpegData, "image/jpeg")
		if err != nil {
			t.Fatal(err)
		}
		// An interrupted attempt has a physical half-upload but no published
		// metadata. Its path was durably registered before that upload began.
		if err := objects.Upload(ctx, job.ImagePath, prepared.Image); err != nil {
			t.Fatal(err)
		}
		sbExec(t, admin, "update feed.photo_uploads set locked_until=statement_timestamp()-interval '1 second'where id=$1", id)
		if err := objects.Upload(ctx, job.ThumbnailPath, prepared.Thumbnail); err == nil {
			t.Fatal("expired lease uploaded an output")
		}
		if accepted, err := repo.CompletePhoto(ctx, job, prepared); err != nil || accepted {
			t.Fatal("expired lease published", err)
		}
		next, found, err := repo.ClaimPhoto(ctx)
		if err != nil || !found || next.ID != job.ID || next.Lease == job.Lease || next.ImagePath == job.ImagePath {
			t.Fatal("retry did not get isolated output paths", err)
		}
		if err := repo.FailPhoto(ctx, next, false); err != nil {
			t.Fatal(err)
		}
		c.rpc(ownerToken, "cancel_feed_draft", map[string]string{"requested_post": post})
		sbExec(t, admin, "update media.cleanup_jobs set available_at=statement_timestamp()where path=$1", job.ImagePath)
		if worked, err := cleaner.Once(ctx); err != nil || !worked {
			t.Fatal("half-upload cleanup failed", err)
		}
		var remains bool
		admin.QueryRow(ctx, "select exists(select 1 from storage.objects where bucket_id='feed-media'and name=$1)", job.ImagePath).Scan(&remains)
		if remains {
			t.Fatal("interrupted upload became an orphan")
		}
		post, id = domain.UUID(), domain.UUID()
		intent = reserve(post, id, "image/jpeg", len(jpegData))
		sbExec(t, admin, "update feed.post_drafts set expires_at=statement_timestamp()-interval '1 second'where id=$1", post)
		if _, status := c.request("POST", "/storage/v1/object/feed-uploads/"+intent["path"].(string), ownerToken, "image/jpeg", jpegData); status < 300 {
			t.Fatal("expired draft accepted a late original")
		}
	})
	t.Run("deleted_post_action_is_an_expected_race_not_a_serving_error", func(t *testing.T) {
		post := domain.UUID()
		if out, status := c.rpc(viewerToken, "create_feed_post", map[string]any{"requested_id": post, "requested_text": "Deletion race fixture"}); status != 200 {
			t.Fatalf("create stale-action fixture: %d %v", status, out)
		}
		out, status := c.rpc(ownerToken, "get_feed_post", map[string]any{"requested_id": post})
		if status != 200 {
			t.Fatalf("get stale-action delivery: %d", status)
		}
		delivery, okay := out["delivery_id"].(string)
		if !okay {
			t.Fatal("missing direct delivery")
		}
		if _, err := f.rpc(viewer, "select to_jsonb(public.delete_feed_post($1))", post); err != nil {
			t.Fatal(err)
		}
		body, status, err := (soakHTTP{c.url, c.anon, c.client}).rpc(ownerToken, "apply_feed_action", map[string]any{"requested_id": domain.UUID(), "requested_delivery": delivery, "requested_action": "like", "expected_version": 0})
		if err != nil || !soakDeletedAction(ctx, admin, owner, delivery, []string{viewer}, status, body) {
			t.Fatalf("deleted action not classified: status=%d error=%v", status, err)
		}
		if soakDeletedAction(ctx, admin, stranger, delivery, []string{viewer}, status, body) || soakDeletedAction(ctx, admin, owner, delivery, []string{owner}, status, body) {
			t.Fatal("foreign errors were swallowed as fixture races")
		}
		var applied bool
		if err := admin.QueryRow(ctx, "select exists(select 1 from feed.post_likes where user_id=$1 and post_id=$2)", owner, post).Scan(&applied); err != nil || applied {
			t.Fatal("deleted post accepted an action", err)
		}
	})
	t.Run("captions_remain_editable_and_empty_posts_rejected", func(t *testing.T) {
		if _, err := f.rpc(owner, "select public.edit_feed_post($1,1,'caption')", publicPost); err != nil {
			t.Fatal(err)
		}
		if _, err := f.rpc(owner, "select public.edit_feed_post($1,2,'')", publicPost); err != nil {
			t.Fatal(err)
		}
		out, status := c.rpc(ownerToken, "publish_feed_post", map[string]string{"requested_id": domain.UUID()})
		if status < 300 || out["message"] != "invalid_post" {
			t.Fatal("empty post accepted")
		}
	})
	t.Run("deleted_or_banned_viewer_cannot_use_a_still_valid_token", func(t *testing.T) {
		reader, token := newUser()
		path := "/storage/v1/object/authenticated/feed-media/" + publicPath
		if _, status := c.request("GET", path, token, "", nil); status != 200 {
			t.Fatal("active reader fixture cannot read public media")
		}
		sbExec(t, admin, "insert into private.global_account_bans(target_user_id,note)values($1,'media reader test')", reader)
		if _, status := c.request("GET", path, token, "", nil); status == 200 {
			t.Fatal("banned viewer used an existing JWT")
		}
		sbExec(t, admin, "delete from private.global_account_bans where target_user_id=$1", reader)
		sbExec(t, admin, "delete from public.profiles where id=$1", reader)
		if _, status := c.request("GET", path, token, "", nil); status == 200 {
			t.Fatal("deleted profile used an existing JWT")
		}
	})
	if os.Getenv("YAP_SUPABASE_LOAD_TEST") == "1" {
		t.Run("media_load", func(t *testing.T) {
			large := mediaImage(t, 4000, 3000, false)
			posts := make(map[string][]string)
			for draft := 0; draft < 4; draft++ {
				post := domain.UUID()
				for i := 0; i < 3; i++ {
					posts[post] = append(posts[post], upload(post, large, "image/jpeg"))
				}
			}
			var readers []sbFixture
			for i := 0; i < 4; i++ {
				readers = append(readers, sbUsers(t, admin))
			}
			started := time.Now()
			var group sync.WaitGroup
			var photoDuration time.Duration
			for worker := 0; worker < 2; worker++ {
				group.Add(1)
				go func() {
					defer group.Done()
					for {
						worked, err := processor.Once(ctx)
						if err != nil {
							t.Error(err)
							return
						}
						if !worked {
							return
						}
					}
				}()
			}
			photosDone := make(chan struct{})
			go func() { group.Wait(); photoDuration = time.Since(started); close(photosDone) }()
			durations := make(chan time.Duration, 500)
			failures := make(chan error, 500)
			work := make(chan int)
			var readGroup sync.WaitGroup
			for worker := 0; worker < 8; worker++ {
				readGroup.Add(1)
				go func() {
					defer readGroup.Done()
					for index := range work {
						fixture := readers[(index%12)/3]
						reader := []string{fixture.author, fixture.viewer, fixture.stranger}[index%3]
						at := time.Now()
						_, err := fixture.rpc(reader, "select public.get_for_you_feed(null,-1,20)")
						durations <- time.Since(at)
						if err != nil {
							failures <- err
						}
					}
				}()
			}
			for i := 0; i < 500; i++ {
				work <- i
			}
			close(work)
			readGroup.Wait()
			close(durations)
			close(failures)
			for err := range failures {
				t.Error(err)
			}
			values := make([]time.Duration, 0, 500)
			for elapsed := range durations {
				values = append(values, elapsed)
			}
			sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
			<-photosDone
			for post, ids := range posts {
				if out, status := c.rpc(ownerToken, "publish_feed_post", map[string]any{"requested_id": post, "requested_photos": ids}); status != 200 {
					t.Fatalf("load publication failed: %d %v", status, out)
				}
			}
			t.Logf("12 real 12MP photos, actual Storage + processing, concurrency 2: %s; simultaneous 500 SQL feed RPC reads: p95=%s p99=%s. Reads exclude HTTP/Auth.", photoDuration, values[474], values[494])
		})
	}
}
