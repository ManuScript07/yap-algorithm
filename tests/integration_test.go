package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
	"yap-algorithm/internal/api"
	"yap-algorithm/internal/cursor"
	"yap-algorithm/internal/domain"
	"yap-algorithm/internal/store"
	"yap-algorithm/internal/worker"
)

const fixtureModel = "fixture-topics-v1"

func admin(t *testing.T, sql string) string {
	t.Helper()
	cmd := exec.Command("docker", "exec", "-i", "yap_algorithm_db", "psql", "-U", "supabase_admin", "-d", "yap_algorithm_test", "-v", "ON_ERROR_STOP=1", "-qAt")
	cmd.Stdin = strings.NewReader(sql)
	out, e := cmd.CombinedOutput()
	if e != nil {
		t.Fatalf("isolated DB setup: %s: %v", out, e)
	}
	return strings.TrimSpace(string(out))
}
func stores(t *testing.T) (*store.Store, *store.Store) {
	t.Helper()
	if os.Getenv("YAP_INTEGRATION") != "1" {
		t.Skip("set YAP_INTEGRATION=1; requires isolated yap_algorithm_db")
	}
	ctx := context.Background()
	s, e := store.Open(ctx, "postgres://yap_local_api:yap-local-api@127.0.0.1:55432/yap_algorithm_test?sslmode=disable", fixtureModel)
	if e != nil {
		t.Fatal(e)
	}
	if !s.Fixture(ctx) {
		t.Fatal("refusing non-fixture database")
	}
	w, e := store.Open(ctx, "postgres://yap_local_worker:yap-local-worker@127.0.0.1:55432/yap_algorithm_test?sslmode=disable", fixtureModel)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { s.Pool.Close(); w.Pool.Close() })
	return s, w
}

type fakeEmbedding struct{}

func (fakeEmbedding) Embed(_ context.Context, text string) (string, error) {
	v := make([]float64, domain.Dimensions)
	i := 0
	if strings.Contains(text, "music") {
		i = 1
	}
	if strings.Contains(text, "sport") {
		i = 2
	}
	v[i] = 1
	return domain.Vector(v)
}
func drain(t *testing.T, s *store.Store) {
	t.Helper()
	w := worker.Worker{Store: s, Model: fakeEmbedding{}}
	for i := 0; i < 500; i++ {
		ok, e := w.Once(context.Background())
		if e != nil {
			t.Fatal(e)
		}
		if !ok {
			return
		}
	}
	t.Fatal("worker did not drain")
}
func post(t *testing.T, s *store.Store, u, text, audience string, loc *domain.Location) domain.Post {
	t.Helper()
	p, e := s.CreatePost(context.Background(), u, domain.NewPost{Text: text, Language: "en", Audience: audience, Photos: []string{}, Location: loc})
	if e != nil {
		t.Fatal(e)
	}
	return p
}
func has(p store.Page, id string) bool {
	for _, i := range p.Items {
		if i.Post.ID == id {
			return true
		}
	}
	return false
}
func page(t *testing.T, s *store.Store, u, tab string) store.Page {
	t.Helper()
	ctx := context.Background()
	id, e := s.Build(ctx, u, tab, nil)
	if e != nil {
		t.Fatal(e)
	}
	p, _, e := s.Page(ctx, u, tab, id, 0, 30)
	if e != nil {
		t.Fatal(e)
	}
	return p
}

func TestPipelineIntegration(t *testing.T) {
	s, w := stores(t)
	ctx := context.Background()
	a, b, c, d := domain.UUID(), domain.UUID(), domain.UUID(), domain.UUID()
	admin(t, fmt.Sprintf("insert into public.profiles(id)values('%s'),('%s'),('%s'),('%s');insert into public.friendships values(least('%s'::uuid,'%s'::uuid),greatest('%s'::uuid,'%s'::uuid));insert into public.user_locations values('%s',55.75,37.61,now());insert into private.search_privacy_settings values('%s',true);", a, b, c, d, a, b, a, b, a, b))
	friend := post(t, s, b, "music with friends", "friends", &domain.Location{Latitude: 55.76, Longitude: 37.61})
	pub := post(t, s, c, "sport and exercise", "public", &domain.Location{Latitude: 55.77, Longitude: 37.61})
	distant := post(t, s, d, "Go and programming", "public", &domain.Location{Latitude: 60, Longitude: 37.61})
	drain(t, w)
	t.Run("idempotent_post_creation", func(t *testing.T) {
		in := domain.NewPost{ID: distant.ID, Text: distant.Text, Audience: distant.Audience, Language: distant.Language, Photos: distant.Photos, Location: distant.Location}
		again, e := s.CreatePost(ctx, d, in)
		if e != nil || again.ID != distant.ID {
			t.Fatal("post retry not idempotent", e)
		}
		in.Text = "different content"
		if _, e = s.CreatePost(ctx, d, in); !errors.Is(e, domain.ErrInvalidated) {
			t.Fatal("conflicting create accepted", e)
		}
	})
	t.Run("visibility_and_location", func(t *testing.T) {
		if _, e := s.GetPost(ctx, c, friend.ID); !errors.Is(e, domain.ErrNotFound) {
			t.Fatal("private post leaked", e)
		}
		p, e := s.GetPost(ctx, a, friend.ID)
		if e != nil || p.Location == nil {
			t.Fatal("permitted friend coordinate missing", e)
		}
		p, e = s.GetPost(ctx, a, pub.ID)
		if e != nil || p.Location != nil {
			t.Fatal("public coordinate leaked", e)
		}
		admin(t, fmt.Sprintf("insert into private.precise_location_exclusions values('%s','%s');", b, a))
		p, e = s.GetPost(ctx, a, friend.ID)
		if e != nil || p.Location != nil {
			t.Fatal("exclusion ignored", e)
		}
		near := page(t, s, a, "nearby")
		if !has(near, friend.ID) || !has(near, pub.ID) || has(near, distant.ID) {
			t.Fatal("50km publication geography incorrect")
		}
	})
	t.Run("following_is_not_mutual_friendship", func(t *testing.T) {
		if e := s.Follow(ctx, a, c, true); e != nil {
			t.Fatal(e)
		}
		p := page(t, s, a, "friends")
		if !has(p, friend.ID) || has(p, pub.ID) {
			t.Fatal("follow and friendship mixed")
		}
	})
	t.Run("all_tabs_feedback_and_deduplication", func(t *testing.T) {
		p := page(t, s, a, "friends")
		var item store.FeedItem
		for _, it := range p.Items {
			if it.Post.ID == friend.ID {
				item = it
			}
		}
		impression := domain.Event{ID: domain.UUID(), SessionID: p.SessionID, PostID: friend.ID, Position: item.Position, Kind: "impression", VisibleMS: 1000, VisibleFraction: .9}
		if e := s.Events(ctx, a, []domain.Event{impression, impression}); e != nil {
			t.Fatal(e)
		}
		dwell := domain.Event{ID: domain.UUID(), SessionID: p.SessionID, PostID: friend.ID, Position: item.Position, Kind: "dwell", ActiveMS: 5000}
		admin(t, fmt.Sprintf("update recsys.session_items set served_at=now()-interval '6 seconds'where session_id='%s'and position=%d;", p.SessionID, item.Position))
		if e := s.Events(ctx, a, []domain.Event{dwell}); e != nil {
			t.Fatal(e)
		}
		drain(t, w)
		count := admin(t, fmt.Sprintf("select count(*)from recsys.interests where user_id='%s';", a))
		if count == "0" {
			t.Fatal("friends feedback did not build shared interests")
		}
		count = admin(t, fmt.Sprintf("select impressions from recsys.post_stats where post_id='%s';", friend.ID))
		if count != "1" {
			t.Fatal("replayed impression counted", count)
		}
		bad := impression
		bad.ID = domain.UUID()
		bad.PostID = distant.ID
		if e := s.Events(ctx, a, []domain.Event{bad}); e == nil {
			t.Fatal("unserved impression accepted")
		}
		if e := s.Events(ctx, c, []domain.Event{impression}); e == nil {
			t.Fatal("foreign session accepted")
		}
		admin(t, fmt.Sprintf("update recsys.jobs set done_at=null,lease=null,locked_until=null where kind='event'and target_id='%s';", impression.ID))
		drain(t, w)
		if got := admin(t, fmt.Sprintf("select impressions from recsys.post_stats where post_id='%s';", friend.ID)); got != "1" {
			t.Fatal("worker replay double count", got)
		}
	})
	t.Run("like_unlike_no_toggle_inflation", func(t *testing.T) {
		for _, kind := range []string{"like", "like", "unlike"} {
			if e := s.Action(ctx, a, pub.ID, kind, "", 0); e != nil {
				t.Fatal(e)
			}
		}
		drain(t, w)
		got := admin(t, fmt.Sprintf("select likes from recsys.post_stats where post_id='%s';select weight from recsys.evidence where user_id='%s'and post_id='%s'and kind='like';", pub.ID, a, pub.ID))
		if got != "0\n0" {
			t.Fatal("unlike did not cancel evidence", got)
		}
	})
	t.Run("nearby_actions_and_comment_reports", func(t *testing.T) {
		p := page(t, s, a, "nearby")
		var pos int
		for _, item := range p.Items {
			if item.Post.ID == pub.ID {
				pos = item.Position
			}
		}
		if e := s.Action(ctx, a, pub.ID, "like", p.SessionID, pos); e != nil {
			t.Fatal(e)
		}
		root, e := s.Comment(ctx, a, pub.ID, store.Comment{ID: domain.UUID(), Text: "A reply"})
		if e != nil {
			t.Fatal(e)
		}
		if e = s.CommentLike(ctx, b, root.ID, true); e != nil {
			t.Fatal(e)
		}
		reply := store.Comment{ID: domain.UUID(), ParentID: &root.ID, Text: "Nested reply"}
		if _, e = s.Comment(ctx, b, pub.ID, reply); e != nil {
			t.Fatal(e)
		}
		if e = s.Report(ctx, a, root.ID, "spam", true); e != nil {
			t.Fatal(e)
		}
		drain(t, w)
		if got := admin(t, fmt.Sprintf("select count(*)from feed.events where user_id='%s'and post_id='%s'and surface='nearby'and kind='like';", a, pub.ID)); got != "1" {
			t.Fatal("nearby source lost", got)
		}
		if got := admin(t, fmt.Sprintf("select count(*)from recsys.interests where user_id='%s';", a)); got == "0" {
			t.Fatal("nearby feedback did not update interests")
		}
		if got := admin(t, fmt.Sprintf("select count(*)from recsys.evidence where user_id='%s'and kind='report';", a)); got != "0" {
			t.Fatal("comment report trained disinterest in parent post", got)
		}
	})
	t.Run("reset_does_not_replay_queued_history", func(t *testing.T) {
		if e := s.Action(ctx, a, distant.ID, "like", "", 0); e != nil {
			t.Fatal(e)
		}
		if e := s.Reset(ctx, a); e != nil {
			t.Fatal(e)
		}
		drain(t, w)
		if got := admin(t, fmt.Sprintf("select count(*)from recsys.interests where user_id='%s';", a)); got != "0" {
			t.Fatal("old pending event undid reset", got)
		}
	})
	t.Run("hide_affects_only_for_you", func(t *testing.T) {
		old := page(t, s, a, "for_you")
		if e := s.HideAuthor(ctx, a, b, true); e != nil {
			t.Fatal(e)
		}
		if _, _, e := s.Page(ctx, a, "for_you", old.SessionID, 0, 20); !errors.Is(e, domain.ErrInvalidated) {
			t.Fatal("old preferences session remained active", e)
		}
		if has(page(t, s, a, "for_you"), friend.ID) || !has(page(t, s, a, "friends"), friend.ID) || !has(page(t, s, a, "nearby"), friend.ID) {
			t.Fatal("hide scope incorrect")
		}
	})
	t.Run("blocks_and_bans_rechecked_on_cached_page", func(t *testing.T) {
		p := page(t, s, a, "nearby")
		admin(t, fmt.Sprintf("insert into public.user_blocks values('%s','%s');", c, a))
		got, _, e := s.Page(ctx, a, "nearby", p.SessionID, 0, 30)
		if e != nil || has(got, pub.ID) {
			t.Fatal("reverse block bypassed", e)
		}
		admin(t, fmt.Sprintf("delete from public.user_blocks where blocker_user_id='%s'and blocked_user_id='%s';insert into private.fixture_account_status values('%s',true,false);", c, a, a))
		if _, _, e = s.Page(ctx, a, "nearby", p.SessionID, 0, 30); !errors.Is(e, domain.ErrForbidden) {
			t.Fatal("active account check bypassed", e)
		}
		admin(t, fmt.Sprintf("delete from private.fixture_account_status where user_id='%s';", a))
	})
	t.Run("edit_invalidates_embedding_and_conflicts", func(t *testing.T) {
		if e := s.EditPost(ctx, d, distant.ID, "music instead", 1); e != nil {
			t.Fatal(e)
		}
		if e := s.EditPost(ctx, d, distant.ID, "stale", 1); !errors.Is(e, domain.ErrInvalidated) {
			t.Fatal("stale edit accepted", e)
		}
		drain(t, w)
		if got := admin(t, fmt.Sprintf("select post_version from recsys.post_embeddings where post_id='%s';", distant.ID)); got != "2" {
			t.Fatal("embedding not refreshed", got)
		}
	})
	t.Run("old_feedback_does_not_learn_edited_meaning", func(t *testing.T) {
		if e := s.Reset(ctx, a); e != nil {
			t.Fatal(e)
		}
		p := post(t, s, d, "music before edit", "public", nil)
		drain(t, w)
		if e := s.Action(ctx, a, p.ID, "like", "", 0); e != nil {
			t.Fatal(e)
		}
		if e := s.EditPost(ctx, d, p.ID, "sport after edit", 1); e != nil {
			t.Fatal(e)
		}
		drain(t, w)
		if got := admin(t, fmt.Sprintf("select count(*)from recsys.interests where user_id='%s';", a)); got != "0" {
			t.Fatal("old like learned edited text", got)
		}
	})
	t.Run("deletion_and_friend_removal_rechecked", func(t *testing.T) {
		p := page(t, s, a, "nearby")
		if e := s.DeletePost(ctx, c, pub.ID); e != nil {
			t.Fatal(e)
		}
		got, _, e := s.Page(ctx, a, "nearby", p.SessionID, 0, 30)
		if e != nil || has(got, pub.ID) {
			t.Fatal("deleted post in session", e)
		}
		friends := page(t, s, a, "friends")
		admin(t, fmt.Sprintf("delete from public.friendships where user_one_id=least('%s'::uuid,'%s'::uuid)and user_two_id=greatest('%s'::uuid,'%s'::uuid);", a, b, a, b))
		got, _, e = s.Page(ctx, a, "friends", friends.SessionID, 0, 30)
		if e != nil || has(got, friend.ID) {
			t.Fatal("removed friendship bypassed", e)
		}
	})
	t.Run("http_auth_and_cursor_contract", func(t *testing.T) {
		app := &api.API{Store: s, Auth: api.DevAuth{}, Cursor: cursor.Codec{Key: []byte("local integration cursor secret 123456")}}
		handler := app.Handler()
		r := httptest.NewRequest("GET", "/v1/feed?limit=31", nil)
		r.Header.Set("Authorization", "Bearer dev:"+a)
		res := httptest.NewRecorder()
		handler.ServeHTTP(res, r)
		if res.Code != 400 {
			t.Fatal(res.Code, res.Body.String())
		}
		r = httptest.NewRequest("GET", "/v1/feed", nil)
		res = httptest.NewRecorder()
		handler.ServeHTTP(res, r)
		if res.Code != 401 {
			t.Fatal("missing authentication", res.Code)
		}
		body, _ := json.Marshal(map[string]any{"text": "hello", "audience": "public", "language": "en", "photos": []string{}, "user_id": b})
		r = httptest.NewRequest("POST", "/v1/posts", bytes.NewReader(body))
		r.Header.Set("Authorization", "Bearer dev:"+a)
		res = httptest.NewRecorder()
		handler.ServeHTTP(res, r)
		if res.Code != 400 {
			t.Fatal("identity injection field accepted", res.Code)
		}
	})
	t.Run("rls_subject_is_transaction_local", func(t *testing.T) {
		if e := s.WithUser(ctx, a, func(tx pgx.Tx) error { return nil }); e != nil {
			t.Fatal(e)
		}
		var subject *string
		if e := s.Pool.QueryRow(ctx, "select nullif(current_setting('yap.viewer_id',true),'')").Scan(&subject); e != nil || subject != nil {
			t.Fatal("identity leaked into pooled connection", e)
		}
	})
	t.Run("account_cascade_does_not_create_feedback", func(t *testing.T) {
		cleanupUser := domain.UUID()
		admin(t, fmt.Sprintf("insert into public.profiles(id)values('%s');", cleanupUser))
		p := post(t, s, b, "public cleanup case", "public", nil)
		if e := s.Action(ctx, cleanupUser, p.ID, "like", "", 0); e != nil {
			t.Fatal(e)
		}
		admin(t, fmt.Sprintf("delete from public.profiles where id='%s';delete from public.profiles where id='%s';", cleanupUser, d))
		if got := admin(t, fmt.Sprintf("select count(*)from feed.posts where author_id='%s';", d)); got != "0" {
			t.Fatal("account cleanup left posts", got)
		}
	})
}

func TestTenThousandPostCorpus(t *testing.T) {
	if os.Getenv("YAP_LOAD") != "1" {
		t.Skip("set YAP_LOAD=1 for isolated 10k ingestion/retrieval check")
	}
	s, _ := stores(t)
	u, a := domain.UUID(), domain.UUID()
	admin(t, fmt.Sprintf("insert into public.profiles(id)values('%s'),('%s');", u, a))
	t.Cleanup(func() {
		admin(t, fmt.Sprintf("delete from recsys.jobs where kind='post'and target_id in(select id from feed.posts where author_id='%s');delete from public.profiles where id in('%s','%s');", a, u, a))
	})
	started := time.Now()
	admin(t, fmt.Sprintf("insert into feed.posts(id,author_id,text,audience,language,created_at)select gen_random_uuid(),'%s','load corpus post '||i,'public','en',now()-(i%%1440)*interval '1 minute'from generate_series(1,10001)i;analyze feed.posts;", a))
	ingest := time.Since(started)
	latencies := []time.Duration{}
	for i := 0; i < 5; i++ {
		start := time.Now()
		p := page(t, s, u, "for_you")
		if len(p.Items) != 30 {
			t.Fatal("underfilled page", len(p.Items))
		}
		latencies = append(latencies, time.Since(start))
	}
	t.Logf("10001 posts + outbox inserted in %s; 5 build+page samples: %v; embedding throughput measured separately", ingest, latencies)
	vector := "[1," + strings.Repeat("0,", 382) + "0]"
	admin(t, fmt.Sprintf("insert into recsys.post_embeddings(post_id,post_version,model,embedding)select id,version,'%s','%s'::extensions.vector from feed.posts where author_id='%s';insert into recsys.interests values('%s',0,false,'%s','%s'::extensions.vector,10,now());analyze recsys.post_embeddings;", fixtureModel, vector, a, u, fixtureModel, vector))
	semanticLatencies := []float64{}
	for i := 0; i < 5; i++ {
		start := time.Now()
		p := page(t, s, u, "for_you")
		if len(p.Items) != 30 {
			t.Fatal("underfilled vector page")
		}
		semanticLatencies = append(semanticLatencies, float64(time.Since(start).Microseconds())/1000)
	}
	report := map[string]any{"inserted_posts": 10001, "insert_with_outbox_ms": float64(ingest.Microseconds()) / 1000, "semantic_build_page_ms": semanticLatencies, "note": "Technical vectors exercise pgvector and reranking; real E5 throughput is measured separately."}
	data, _ := json.MarshalIndent(report, "", "  ")
	if e := os.MkdirAll("../output", 0755); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile("../output/load-check.json", data, 0644); e != nil {
		t.Fatal(e)
	}
	t.Logf("pgvector + 384-dimensional reranking build/page samples (ms): %v", semanticLatencies)
}
