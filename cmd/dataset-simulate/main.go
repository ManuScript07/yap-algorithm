package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"image"
	_ "image/jpeg"
	"math/rand"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"golang.org/x/image/draw"
	"yap-algorithm/internal/dataset"
	"yap-algorithm/internal/datasetsim"
	"yap-algorithm/internal/domain"
)

type object = map[string]any
type account struct{ ID, Token string }

type runner struct {
	admin, exporter *pgx.Conn
	api             *datasetsim.Client
	users           []account
	cohort, output  string
	seed            int64
	photo           []byte
	mime, toolHash  string
	photoHashes     map[string]string
}

func (r *runner) user(ctx context.Context) (account, error) {
	email, password := "dataset-"+domain.UUID()+"@example.test", "Aa!"+domain.UUID()
	var created struct{ ID string }
	if err := r.api.JSON(ctx, "POST", "/auth/v1/admin/users", r.api.Service, object{
		"email": email, "password": password, "email_confirm": true, "user_metadata": object{"name": "Dataset fixture"},
	}, &created); err != nil {
		return account{}, err
	}
	if created.ID == "" {
		return account{}, errors.New("fixture has no Auth identifier")
	}
	r.users = append(r.users, account{ID: created.ID})
	var login struct {
		Token string `json:"access_token"`
	}
	err := r.api.JSON(ctx, "POST", "/auth/v1/token?grant_type=password", r.api.Anon,
		object{"email": email, "password": password}, &login)
	if err != nil || login.Token == "" {
		return account{}, errors.Join(err, errors.New("fixture could not sign in"))
	}
	return account{created.ID, login.Token}, nil
}

func (r *runner) clean() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var errs []error
	for _, a := range r.users {
		// Cancel owned media first via account cascade, then remove objects
		// through Storage rather than mutating its metadata tables.
		if err := r.deleteProfile(ctx, a.ID); err != nil {
			errs = append(errs, err)
			continue
		}
		for _, bucket := range []string{"feed-uploads", "feed-media"} {
			rows, err := r.admin.Query(ctx, "select name from storage.objects where bucket_id=$1 and name like $2", bucket, a.ID+"/%")
			if err != nil {
				errs = append(errs, err)
				continue
			}
			keys, err := pgx.CollectRows(rows, pgx.RowTo[string])
			if err != nil {
				errs = append(errs, err)
			} else if len(keys) > 0 {
				errs = append(errs, r.api.DeleteObjects(ctx, bucket, keys))
			}
		}
		errs = append(errs, r.api.JSON(ctx, "DELETE", "/auth/v1/admin/users/"+a.ID, r.api.Service, nil, nil))
	}
	if err := errors.Join(errs...); err != nil {
		return err
	}
	_, err := r.admin.Exec(ctx, "delete from recsys_dataset.cohorts where id=$1::uuid", r.cohort)
	return err
}

func (r *runner) deleteProfile(ctx context.Context, id string) error {
	var err error
	for attempt := 0; attempt < 5; attempt++ {
		_, err = r.admin.Exec(ctx, "delete from public.profiles where id=$1::uuid", id)
		var db *pgconn.PgError
		if !errors.As(err, &db) || (db.Code != "40P01" && db.Code != "40001") {
			return err
		}
		if waitErr := pause(ctx, time.Duration(attempt+1)*150*time.Millisecond); waitErr != nil {
			return errors.Join(err, waitErr)
		}
	}
	return err
}

func (r *runner) clock(ctx context.Context) (time.Time, error) {
	var now time.Time
	err := r.admin.QueryRow(ctx, "select clock_timestamp()").Scan(&now)
	return now.UTC(), err
}

func pause(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (r *runner) publish(ctx context.Context, author account, text, language string, photo bool) (string, error) {
	id := domain.UUID()
	photos := []string{}
	if photo {
		photoID := domain.UUID()
		var intent struct{ Path string }
		if err := r.api.RPC(ctx, "reserve_feed_photo", author.Token, object{"requested_post": id, "requested_photo": photoID,
			"requested_bytes": len(r.photo), "requested_mime": r.mime}, &intent); err != nil {
			return "", err
		}
		if err := r.api.Upload(ctx, intent.Path, r.mime, author.ID, author.Token, r.photo); err != nil {
			return "", err
		}
		if err := r.api.RPC(ctx, "confirm_feed_photo", author.Token, object{"requested_photo": photoID}, nil); err != nil {
			return "", err
		}
		ready, cancel := context.WithTimeout(ctx, 35*time.Second)
		defer cancel()
		for {
			var state string
			if err := r.admin.QueryRow(ready, "select state from feed.photo_uploads where id=$1", photoID).Scan(&state); err != nil {
				return "", err
			}
			if state == "ready" {
				break
			}
			if state == "rejected" {
				return "", errors.New("fixture photo was rejected")
			}
			if err := pause(ready, 750*time.Millisecond); err != nil {
				return "", errors.New("fixture photo did not become ready; start the local media worker")
			}
		}
		photos = append(photos, photoID)
	}
	err := r.api.RPC(ctx, "publish_feed_post", author.Token, object{"requested_id": id, "requested_text": text,
		"requested_language": language, "requested_audience": "public", "requested_photos": photos,
		"requested_location": object{"latitude": 55.75, "longitude": 37.61}}, nil)
	if err == nil && photo {
		err = r.fingerprint(ctx, photos[0], author)
	}
	return id, err
}

func (r *runner) fingerprint(ctx context.Context, id string, author account) error {
	var checksum, key string
	if err := r.admin.QueryRow(ctx, "select sha256,image_path from feed.photo_uploads where id=$1", id).Scan(&checksum, &key); err != nil {
		return err
	}
	raw, err := r.api.PreparedPhoto(ctx, key, author.ID, author.Token)
	if err != nil {
		return err
	}
	if fmt.Sprintf("%x", sha256.Sum256(raw)) != checksum {
		return errors.New("prepared photo checksum does not match Storage")
	}
	source, _, err := image.Decode(bytes.NewReader(raw))
	if err != nil {
		return err
	}
	tiny := image.NewGray(image.Rect(0, 0, 9, 8))
	draw.ApproxBiLinear.Scale(tiny, tiny.Bounds(), source, source.Bounds(), draw.Src, nil)
	var bits uint64
	for y := 0; y < 8; y++ {
		for x := 0; x < 8; x++ {
			bits <<= 1
			if tiny.GrayAt(x, y).Y > tiny.GrayAt(x+1, y).Y {
				bits++
			}
		}
	}
	r.photoHashes[checksum] = fmt.Sprintf("%016x", bits)
	return nil
}

type feedPage struct {
	Signature map[string]string `json:"versions"`
	Items     []struct {
		Delivery string `json:"delivery_id"`
		Post     struct{ ID string }
	}
}

func (r *runner) receipts(ctx context.Context, viewer account, rpc string, wanted []string, prepared bool) (map[string]string, error) {
	wait, cancel := context.WithTimeout(ctx, 40*time.Second)
	defer cancel()
	for {
		var page feedPage
		if err := r.api.RPC(wait, rpc, viewer.Token, object{"requested_limit": 30}, &page); err != nil {
			return nil, err
		}
		found := make(map[string]string)
		for _, item := range page.Items {
			if slices.Contains(wanted, item.Post.ID) && item.Delivery != "" {
				found[item.Post.ID] = item.Delivery
			}
		}
		if len(found) == len(wanted) && (!prepared || page.Signature["features"] == "feed-features-v2") {
			return found, nil
		}
		if err := pause(wait, 2*time.Second); err != nil {
			return nil, fmt.Errorf("%s: fixture posts did not reach the page; start the local worker", rpc)
		}
	}
}

var examples = [3][4]string{
	{"Собрал карманный приёмник и впервые поймал дальнюю станцию.", "The sourdough crust cracked loudly when it cooled.",
		"Городская команда тренируется к вечернему заплыву.", "The library repaired its old clock before opening."},
	{"После дождя тропинка у водопада стала очень скользкой.", "A thrift-store guitar needed new strings and a bridge adjustment.",
		"Нашёл в трамвае забытую книгу и передал водителю.", "The baker is testing a new flour for sandwich loaves."},
	{"Тюльпаны на солнечном балконе раскрылись раньше обычного.", "Our chess club is trying a faster time control tonight.",
		"Поменял цепь на велосипеде перед первой поездкой.", "A rescue dog finally stopped barking at the vacuum."},
}

func (r *runner) phase(ctx context.Context, number int, author, viewer account, rng *rand.Rand) error {
	if err := r.api.RPC(ctx, "set_feed_author_followed", viewer.Token, object{"requested_id": domain.UUID(),
		"requested_author": author.ID, "requested_following": true, "expected_version": 0}, nil); err != nil {
		return err
	}
	if err := r.api.RPC(ctx, "update_my_location_with_metadata", viewer.Token, object{
		"new_latitude": 55.75, "new_longitude": 37.61}, nil); err != nil {
		return err
	}
	posts := make([]string, 4)
	for i, text := range examples[number] {
		lang := []string{"ru", "en"}[i%2]
		withPhoto := len(r.photo) > 0 && number == 1 && i == 0
		if withPhoto {
			text = ""
		}
		id, err := r.publish(ctx, author, text, lang, withPhoto)
		if err != nil {
			return err
		}
		posts[i] = id
	}
	friends, err := r.receipts(ctx, viewer, "get_friends_feed", posts[2:3], false)
	if err != nil {
		return err
	}
	nearby, err := r.receipts(ctx, viewer, "get_nearby_feed", posts[3:4], false)
	if err != nil {
		return err
	}
	foryou, err := r.receipts(ctx, viewer, "get_for_you_feed", posts[:2], true)
	if err != nil {
		return err
	}
	if err := pause(ctx, 1100*time.Millisecond); err != nil {
		return err
	}
	impression := func(ids ...string) error {
		batch := make([]object, 0, len(ids))
		for _, id := range ids {
			batch = append(batch, object{"event_id": domain.UUID(), "delivery_id": id, "kind": "impression",
				"visible_ms": 1000, "visible_fraction": 0.8})
		}
		var reply struct{ Accepted int }
		if err := r.api.RPC(ctx, "record_feed_events", viewer.Token, object{"requested_events": batch}, &reply); err != nil {
			return err
		}
		if reply.Accepted != len(batch) {
			return errors.New("fixture exposure was not accepted")
		}
		return nil
	}
	if err := impression(foryou[posts[0]], foryou[posts[1]]); err != nil {
		return err
	}
	if err := impression(friends[posts[2]], nearby[posts[3]]); err != nil {
		return err
	}
	action := func(receipt, name string, version int) error {
		return r.api.RPC(ctx, "apply_feed_action", viewer.Token, object{"requested_id": domain.UUID(),
			"requested_delivery": receipt, "requested_action": name, "expected_version": version}, nil)
	}
	liked := rng.Intn(2)
	if err := action(foryou[posts[liked]], "like", 0); err != nil {
		return err
	}
	if number == 0 {
		if err := action(foryou[posts[liked]], "unlike", 1); err != nil {
			return err
		}
	}
	if err := action(friends[posts[2]], "like", 0); err != nil {
		return err
	}
	if err := r.api.RPC(ctx, "create_feed_comment", viewer.Token, object{"requested_id": domain.UUID(),
		"requested_post": posts[3], "requested_text": "Synthetic comment"}, nil); err != nil {
		return err
	}
	return r.api.RPC(ctx, "record_feed_events", viewer.Token, object{"requested_events": []object{
		{"event_id": domain.UUID(), "delivery_id": nearby[posts[3]], "kind": "open_post"},
	}}, nil)
}

func (r *runner) run(ctx context.Context) error {
	if err := os.Mkdir(r.output, 0700); err != nil {
		return err
	}
	var authors, viewers [3]account
	for i := range authors {
		var err error
		authors[i], err = r.user(ctx)
		if err != nil {
			return err
		}
		viewers[i], err = r.user(ctx)
		if err != nil {
			return err
		}
	}
	ids := make([]string, len(r.users))
	for i, user := range r.users {
		ids[i] = user.ID
	}
	source := sha256.Sum256([]byte(fmt.Sprintf("%s:%d:%x", r.toolHash, r.seed, sha256.Sum256(r.photo))))
	if _, err := r.admin.Exec(ctx, "insert into recsys_dataset.cohorts(id,origin,seed,source_hash)values($1,'synthetic',$2,$3)", r.cohort, r.seed, fmt.Sprintf("%x", source)); err != nil {
		return err
	}
	if _, err := r.admin.Exec(ctx, "insert into recsys_dataset.members(cohort_id,user_id) select $1,unnest($2::uuid[])", r.cohort, ids); err != nil {
		return err
	}
	const horizon = 3 * time.Second
	var signature []string
	splits := object{}
	rng := rand.New(rand.NewSource(r.seed))
	for i, name := range []string{"train", "validation", "test"} {
		start, err := r.clock(ctx)
		if err != nil {
			return err
		}
		if err := r.phase(ctx, i, authors[i], viewers[i], rng); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		if err := pause(ctx, horizon+250*time.Millisecond); err != nil {
			return err
		}
		capture := "capture-" + name
		manifest, err := dataset.Capture(ctx, r.exporter, dataset.Options{Cohort: r.cohort, Output: filepath.Join(r.output, capture),
			PageSize: 20, MaxRows: 1000, Timeout: 10 * time.Second, BinaryHash: r.toolHash})
		if err != nil {
			return err
		}
		var snapshot struct {
			Settled *time.Time `json:"settled_through"`
		}
		if err := json.Unmarshal(manifest.Database, &snapshot); err != nil || snapshot.Settled == nil {
			return errors.New("capture has an unknown time fence; retry the simulator")
		}
		if i == 0 {
			// Read the producing versions from the actual exposure, never a
			// hard-coded model/space written into an exported dataset.
			raw, err := os.ReadFile(filepath.Join(r.output, capture, "facts.jsonl"))
			if err != nil {
				return err
			}
			for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
				var row struct {
					Kind, Surface string
					Context       map[string]json.RawMessage
				}
				if err := json.Unmarshal([]byte(line), &row); err != nil {
					return err
				}
				if row.Kind == "impression" && row.Surface == "for_you" {
					for _, key := range []string{"model", "feature_version", "policy_version", "embedding_space", "embedding_model"} {
						var value string
						if json.Unmarshal(row.Context[key], &value) != nil || value == "" {
							return errors.New("prepared exposure has incomplete versions")
						}
						signature = append(signature, value)
					}
					break
				}
			}
			if len(signature) != 5 {
				return errors.New("prepared exposure missing from capture")
			}
		}
		splits[name] = object{"from": start, "available_by": manifest.CompletedAt, "capture": capture}
		fmt.Printf("%s: %d captured facts\n", name, manifest.Rows)
		if i < 2 {
			if err := pause(ctx, horizon+100*time.Millisecond); err != nil {
				return err
			}
		}
	}
	raw, err := json.MarshalIndent(object{"origin": "synthetic", "seed": r.seed, "snapshot_signature": signature,
		"label_window_seconds": int(horizon.Seconds()), "embargo_seconds": int(horizon.Seconds()), "splits": splits,
		"photo_fingerprints": r.photoHashes}, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(r.output, "build.json"), raw, 0600)
}

func run(ctx context.Context) (err error) {
	output := flag.String("output", "", "new private output directory")
	seed := flag.Int64("seed", 42, "seed for fixture behavior")
	photo := flag.String("photo", "", "optional licensed PNG/JPEG for a photo-only publication")
	flag.Parse()
	if *output == "" || *seed < 0 || os.Getenv("YAP_SUPABASE_LOCAL_URL") != "http://127.0.0.1:54321" ||
		os.Getenv("YAP_SUPABASE_LOCAL_ANON_KEY") == "" || os.Getenv("YAP_SUPABASE_LOCAL_SERVICE_KEY") == "" {
		return errors.New("explicit local Supabase configuration and output directory required")
	}
	adminURL, err := url.Parse(os.Getenv("YAP_SUPABASE_LOCAL_ADMIN_DSN"))
	if err != nil || adminURL.Host != "127.0.0.1:54322" || adminURL.Path != "/postgres" {
		return errors.New("explicit local administrative DSN required")
	}
	adminURL.RawQuery = "sslmode=disable"
	admin, err := pgx.Connect(ctx, adminURL.String())
	if err != nil {
		return errors.New("cannot open local administrative connection")
	}
	defer admin.Close(context.Background())
	cfg, err := dataset.ConnectionConfig(os.Getenv("YAP_DATASET_DSN"), false)
	if err != nil {
		return err
	}
	exporter, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return errors.New("cannot open restricted dataset connection")
	}
	defer exporter.Close(context.Background())
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	binary, err := os.ReadFile(executable)
	if err != nil {
		return err
	}
	r := runner{admin: admin, exporter: exporter, api: datasetsim.NewClient(os.Getenv("YAP_SUPABASE_LOCAL_ANON_KEY"),
		os.Getenv("YAP_SUPABASE_LOCAL_SERVICE_KEY")), cohort: domain.UUID(), output: *output, seed: *seed,
		toolHash: fmt.Sprintf("%x", sha256.Sum256(binary)), photoHashes: map[string]string{}}
	if *photo != "" {
		info, err := os.Stat(*photo)
		if err != nil || !info.Mode().IsRegular() || info.Size() > 5<<20 {
			return errors.New("photo must be a local file up to 5 MiB")
		}
		r.photo, err = os.ReadFile(*photo)
		if err != nil {
			return err
		}
		switch strings.ToLower(filepath.Ext(*photo)) {
		case ".jpg", ".jpeg":
			r.mime = "image/jpeg"
		case ".png":
			r.mime = "image/png"
		default:
			return errors.New("fixture photo must be JPEG or PNG")
		}
	}
	defer func() {
		if cleanErr := r.clean(); cleanErr != nil {
			err = errors.Join(err, fmt.Errorf("local fixture cleanup failed for cohort %s: %w", r.cohort, cleanErr))
		}
	}()
	return r.run(ctx)
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	ctx, timeout := context.WithTimeout(ctx, 4*time.Minute)
	defer timeout()
	if err := run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
