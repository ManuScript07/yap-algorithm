package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"yap-algorithm/internal/adapters/postgres"
	"yap-algorithm/internal/application"
	"yap-algorithm/internal/domain"
)

func TestSupabaseLocalSocial(t *testing.T) {
	if os.Getenv("YAP_SUPABASE_INTEGRATION") != "1" {
		t.Skip("enable local Supabase integration")
	}
	cfg, err := pgxpool.ParseConfig(os.Getenv("YAP_SUPABASE_LOCAL_ADMIN_DSN"))
	if err != nil || !localSupabaseTarget(t).acceptsPool(cfg) || os.Getenv("YAP_SUPABASE_LOCAL_URL") != localSupabaseTarget(t).apiURL {
		t.Fatal("explicit local Supabase required")
	}
	cfg.MaxConns = 16
	p, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	ctx := context.Background()
	f := sbUsers(t, p)
	c := soakHTTP{os.Getenv("YAP_SUPABASE_LOCAL_URL"), os.Getenv("YAP_SUPABASE_LOCAL_ANON_KEY"), &http.Client{Timeout: 15 * time.Second}}
	tokens := map[string]string{}
	for _, id := range []string{f.author, f.viewer, f.stranger} {
		tokens[id] = localMediaJWT(t, id)
	}
	rpc := func(user, name string, args any) map[string]any {
		t.Helper()
		raw, status, err := c.rpc(tokens[user], name, args)
		if err != nil || status != 200 {
			t.Fatalf("%s status=%d response=%s error=%v", name, status, raw, err)
		}
		var out map[string]any
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	post := f.create(t, "Original post about Go and games", "public")
	comment := func(user, post, text string, reply any) map[string]any {
		return rpc(user, "create_feed_comment", map[string]any{"requested_id": domain.UUID(), "requested_post": post, "requested_text": text, "requested_reply_to": reply})
	}
	page := func(user, post string, root any, order string, session any, after, limit int) map[string]any {
		return rpc(user, "get_feed_comments", map[string]any{"requested_post": post, "requested_root": root, "requested_sort": order, "requested_session": session, "after_position": after, "requested_limit": limit})
	}
	expect := func(user, name string, args any, status int, message string) {
		t.Helper()
		raw, s, err := c.rpc(tokens[user], name, args)
		if err != nil || s != status || !bytes.Contains(raw, []byte(message)) {
			t.Fatalf("expected %s %d %s; got %d %s %v", name, status, message, s, raw, err)
		}
	}
	t.Run("limits_reply_context_edit_delete_and_anonymization", func(t *testing.T) {
		root := comment(f.viewer, post, strings.Repeat("я", 200), nil)
		id := root["id"].(string)
		expect(f.viewer, "create_feed_comment", map[string]any{"requested_id": domain.UUID(), "requested_post": post, "requested_text": strings.Repeat("я", 201)}, 400, "invalid_comment")
		expect(f.viewer, "create_feed_comment", map[string]any{"requested_id": domain.UUID(), "requested_post": post, "requested_text": "   "}, 400, "invalid_comment")
		reply := comment(f.stranger, post, "Reply", id)
		second := comment(f.author, post, "Reply to reply", reply["id"])
		if second["root_id"] != id || second["reply_to"].(map[string]any)["user_id"] != f.stranger {
			t.Fatal("reply did not stay flat or lost addressee")
		}
		other := f.create(t, "Other post", "public")
		expect(f.viewer, "create_feed_comment", map[string]any{"requested_id": domain.UUID(), "requested_post": other, "requested_text": "wrong context", "requested_reply_to": id}, 400, "invalid_comment_thread")
		editID := domain.UUID()
		edit := map[string]any{"requested_id": editID, "requested_comment": id, "expected_version": 1, "requested_text": "Edited"}
		rpc(f.viewer, "edit_feed_comment", edit)
		replay := rpc(f.viewer, "edit_feed_comment", edit)
		if replay["replayed"] != true {
			t.Fatal("edit not idempotent")
		}
		expect(f.viewer, "edit_feed_comment", map[string]any{"requested_id": domain.UUID(), "requested_comment": id, "expected_version": 1, "requested_text": "stale"}, 409, "comment_version_conflict")
		expect(f.stranger, "delete_feed_comment", map[string]any{"requested_id": domain.UUID(), "requested_comment": id, "expected_version": 2}, 404, "comment_not_found")
		rpc(f.viewer, "delete_feed_comment", map[string]any{"requested_id": domain.UUID(), "requested_comment": id, "expected_version": 2})
		got := page(f.author, post, nil, "old", nil, -1, 30)["items"].([]any)
		if len(got) != 1 || got[0].(map[string]any)["text"] != nil || got[0].(map[string]any)["state"] != "deleted" {
			t.Fatal("deleted root not projected as tombstone")
		}
		got = page(f.author, post, id, "old", nil, -1, 30)["items"].([]any)
		if len(got) != 2 {
			t.Fatal("deletion removed thread replies")
		}
		ephemeral := sbUsers(t, p)
		tokens[ephemeral.author] = localMediaJWT(t, ephemeral.author)
		comment(ephemeral.author, post, "Private data to erase", id)
		sbExec(t, p, "delete from public.profiles where id=$1", ephemeral.author)
		var erased bool
		if err := p.QueryRow(ctx, "select text=''and author_id is null and deleted_at is not null from feed.comments where post_id=$1 and creation_request->>1='Private data to erase'", post).Scan(&erased); err != nil || !erased {
			t.Fatal("account deletion did not anonymize comment", err)
		}
	})
	t.Run("popular_pages_are_replayable_and_do_not_skip_after_likes_move", func(t *testing.T) {
		target := f.create(t, "Paged conversation", "public")
		var ids []string
		for i := 0; i < 40; i++ {
			id := domain.UUID()
			ids = append(ids, id)
			sbExec(t, p, `insert into feed.comments(id,post_id,root_id,author_id,text,post_version,creation_request)values($1,$2,$1,$3,$4,1,'[]')`, id, target, f.viewer, fmt.Sprintf("root-%d", i))
			sbExec(t, p, "update feed.comment_statistics set likes=$2 where comment_id=$1", id, i)
		}
		first := page(f.stranger, target, nil, "popular", nil, -1, 7)
		session := first["session_id"].(string)
		initial := first["items"].([]any)
		if initial[0].(map[string]any)["id"] != ids[39] {
			t.Fatal("default popularity order incorrect")
		}
		sbExec(t, p, "update feed.comment_statistics set likes=1000-likes where post_id=$1", target)
		retry := page(f.stranger, target, nil, "popular", session, -1, 7)["items"].([]any)
		for i := range initial {
			if retry[i].(map[string]any)["id"] != initial[i].(map[string]any)["id"] {
				t.Fatal("page replay changed IDs")
			}
		}
		seen := map[string]bool{}
		current := first
		for n := 0; n < 10; n++ {
			for _, item := range current["items"].([]any) {
				id := item.(map[string]any)["id"].(string)
				if seen[id] {
					t.Fatal("duplicate after popularity changed")
				}
				seen[id] = true
			}
			if current["has_more"] == false {
				break
			}
			cursor := current["next_cursor"].(map[string]any)
			current = page(f.stranger, target, nil, "popular", session, int(cursor["after_position"].(float64)), 7)
		}
		if len(seen) != 40 {
			t.Fatalf("missing roots: %d/40", len(seen))
		}
		expect(f.stranger, "get_feed_comments", map[string]any{"requested_post": post, "requested_session": session, "requested_limit": 7}, 400, "comment_cursor_context_mismatch")
		expect(f.viewer, "get_feed_comments", map[string]any{"requested_post": target, "requested_session": session, "requested_limit": 7}, 404, "comment_session_not_found")
		sbExec(t, p, "update feed.page_sessions set expires_at=statement_timestamp()-interval '1 second'where id=$1", session)
		expect(f.stranger, "get_feed_comments", map[string]any{"requested_post": target, "requested_session": session, "requested_limit": 7}, 410, "comment_session_expired")
	})
	t.Run("comment_reaction_versions_and_signal_removal", func(t *testing.T) {
		root := comment(f.viewer, post, "A useful comment", nil)
		id := root["id"].(string)
		request := domain.UUID()
		args := map[string]any{"requested_id": request, "requested_comment": id, "requested_liked": true, "expected_version": 0, "expected_comment_version": 1, "expected_post_version": 1}
		liked := rpc(f.stranger, "set_feed_comment_liked", args)
		if liked["action_version"] != float64(1) {
			t.Fatal("missing reaction version")
		}
		undo := map[string]any{"requested_id": domain.UUID(), "requested_comment": id, "requested_liked": false, "expected_version": 1, "expected_comment_version": 1, "expected_post_version": 1}
		rpc(f.stranger, "set_feed_comment_liked", undo)
		rpc(f.stranger, "set_feed_comment_liked", args)
		var count int
		p.QueryRow(ctx, "select count(*)from feed.comment_likes where user_id=$1 and comment_id=$2", f.stranger, id).Scan(&count)
		if count != 0 {
			t.Fatal("late like replay restored an undone like")
		}
		expect(f.stranger, "set_feed_comment_liked", map[string]any{"requested_id": domain.UUID(), "requested_comment": id, "requested_liked": true, "expected_version": 0, "expected_comment_version": 1, "expected_post_version": 1}, 409, "comment_action_version_conflict")
		rpc(f.viewer, "edit_feed_comment", map[string]any{"requested_id": domain.UUID(), "requested_comment": id, "expected_version": 1, "requested_text": "Changed"})
		expect(f.stranger, "set_feed_comment_liked", map[string]any{"requested_id": domain.UUID(), "requested_comment": id, "requested_liked": true, "expected_version": 2, "expected_comment_version": 1, "expected_post_version": 1}, 409, "comment_content_changed")
	})
	t.Run("plain_reposts_quotes_friend_feed_and_live_original_access", func(t *testing.T) {
		sbExec(t, p, "insert into public.friendships(user_one_id,user_two_id)values(least($1::uuid,$2::uuid),greatest($1::uuid,$2::uuid))", f.viewer, f.stranger)
		id := domain.UUID()
		args := map[string]any{"requested_id": id, "requested_original": post}
		out := rpc(f.viewer, "create_feed_repost", args)
		if out["post_id"] != nil {
			t.Fatal("plain repost duplicated original content")
		}
		rpc(f.viewer, "create_feed_repost", args)
		var count int
		p.QueryRow(ctx, "select count(*)from feed.posts where id=$1", id).Scan(&count)
		if count != 0 {
			t.Fatal("plain repost became a second post")
		}
		feed := rpc(f.stranger, "get_friends_feed", map[string]int{"requested_limit": 30})
		found := false
		for _, item := range feed["items"].([]any) {
			if item.(map[string]any)["post"].(map[string]any)["id"] == post {
				found = true
			}
		}
		if !found {
			t.Fatal("friend's repost of an outside author was missing")
		}
		quote := domain.UUID()
		rpc(f.viewer, "create_feed_repost", map[string]any{"requested_id": quote, "requested_original": post, "requested_text": "My own commentary"})
		dto := rpc(f.stranger, "get_feed_post", map[string]any{"requested_id": quote})
		if dto["text"] != "My own commentary" || dto["original"].(map[string]any)["id"] != post {
			t.Fatal("quote projection missing original or own text")
		}
		rpc(f.author, "edit_feed_post", map[string]any{"requested_id": post, "expected_version": 1, "requested_text": "Edited original"})
		dto = rpc(f.stranger, "get_feed_post", map[string]any{"requested_id": quote})
		if dto["original"].(map[string]any)["text"] != "Edited original" {
			t.Fatal("quote cached obsolete original text")
		}
		sbExec(t, p, "update feed.posts set audience='friends'where id=$1", post)
		// The reader is a friend of the reposter, not the original author.
		raw, status, err := c.rpc(tokens[f.stranger], "get_feed_post", map[string]any{"requested_id": quote})
		if err != nil || status != 200 || strings.TrimSpace(string(raw)) != "null" {
			t.Fatal("repost leaked private original")
		}
		sbExec(t, p, "update feed.posts set audience='public'where id=$1", post)
		deletion := map[string]any{"requested_id": domain.UUID(), "requested_repost": id, "expected_version": 1}
		rpc(f.viewer, "delete_feed_repost", deletion)
		rpc(f.viewer, "delete_feed_repost", deletion)
		p.QueryRow(ctx, "select count(*)from recsys.social_feedback where user_id=$1 and kind='repost'and entity_id=$2", f.viewer, id).Scan(&count)
		if count != 0 {
			t.Fatal("undo repost left an active recommendation signal")
		}
	})
	t.Run("concurrent_create_replay_and_report_compatibility", func(t *testing.T) {
		id := domain.UUID()
		var group sync.WaitGroup
		for i := 0; i < 12; i++ {
			group.Add(1)
			go func() {
				defer group.Done()
				_, err := f.rpc(f.viewer, "select public.create_feed_comment($1,$2,'Concurrent',null)", id, post)
				if err != nil {
					t.Error(err)
				}
			}()
		}
		group.Wait()
		var count int
		p.QueryRow(ctx, "select count(*)from feed.comments where id=$1", id).Scan(&count)
		if count != 1 {
			t.Fatal("concurrent retry duplicated comment")
		}
		report := map[string]any{"requested_id": domain.UUID(), "requested_type": "comment", "requested_content": id, "requested_reason": "spam"}
		rpc(f.stranger, "submit_feed_content_report", report)
		rpc(f.stranger, "submit_feed_content_report", report)
		// Comment reports do not spend or block the existing user-report allowance.
		if _, err := f.rpc(f.stranger, "select to_jsonb(public.submit_user_report($1,'spam'))", f.viewer); err != nil {
			t.Fatal("content report changed old user-report contract", err)
		}
		var users, content int
		if err := p.QueryRow(ctx, "select target_report_count,content_report_count from private.moderation_user_reports where target_type='comment'and content_id=$1", id).Scan(&users, &content); err != nil || users != 1 || content != 1 {
			t.Fatalf("moderation counters: user=%d content=%d %v", users, content, err)
		}
		roots := page(f.stranger, post, nil, "new", nil, -1, 30)["items"].([]any)
		for _, item := range roots {
			row := item.(map[string]any)
			if row["id"] == id && row["text"] != nil {
				t.Fatal("reported comment remained visible")
			}
		}
		rpc(f.stranger, "submit_feed_content_report", map[string]any{"requested_id": domain.UUID(), "requested_type": "post", "requested_content": post, "requested_reason": "other"})
		raw, status, err := c.rpc(tokens[f.stranger], "get_feed_post", map[string]any{"requested_id": post})
		if err != nil || status != 200 || strings.TrimSpace(string(raw)) != "null" {
			t.Fatal("reported post remained visible")
		}
		var exposed bool
		p.QueryRow(ctx, "select has_table_privilege('authenticated','feed.comments','SELECT')or has_table_privilege('yap_recommendation_worker','feed.reposts','INSERT')or has_function_privilege('anon','public.create_feed_comment(uuid,uuid,text,uuid)','EXECUTE')").Scan(&exposed)
		if exposed {
			t.Fatal("raw content or anonymous write grant")
		}
	})
	t.Run("feedback_is_processed_by_the_actual_go_personalizer", func(t *testing.T) {
		target := f.create(t, "Semantic signal from a comment", "public")
		comment(f.viewer, target, "I like this topic", nil)
		// Use a technical normalized vector for deterministic relevance; real E5 is
		// covered by the separately enabled model regression.
		sbExec(t, p, `insert into recsys.post_embeddings(post_id,post_version,model,embedding)values($1,1,$2,$3::text::extensions.vector)`, target, sbModel, "[1,"+strings.Repeat("0,", 382)+"0]")
		// The integration role is created by the existing pipeline fixture.
		// A fresh restricted login comes from the configured service identity.
		config, err := os.ReadFile(localSupabaseTarget(t).workerConfig())
		if err != nil {
			t.Fatal("run restricted worker setup")
		}
		var identity struct {
			DSN string `json:"database_url"`
		}
		if json.Unmarshal(config, &identity) != nil {
			t.Fatal("invalid local worker config")
		}
		workerURL, err := url.Parse(identity.DSN)
		if err != nil || !localSupabaseTarget(t).acceptsURL(workerURL) {
			t.Fatal("explicit local worker required")
		}
		repo, err := postgres.OpenIndexRepository(ctx, identity.DSN, sbModel, 3)
		if err != nil {
			t.Fatal(err)
		}
		defer repo.Close()
		rec, err := repo.Recommendations(ctx)
		if err != nil {
			t.Fatal(err)
		}
		processor := application.Personalizer{Repository: rec}
		var clusters int
		deadline := time.Now().Add(6 * time.Second)
		for time.Now().Before(deadline) {
			_, err := processor.Once(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := p.QueryRow(ctx, "select count(*)from recsys.interest_clusters where user_id=$1", f.viewer).Scan(&clusters); err != nil {
				t.Fatal(err)
			}
			if clusters > 0 {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if clusters == 0 {
			t.Fatal("comment signal never reached recommendation profile")
		}
	})
}
