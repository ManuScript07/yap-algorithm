package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"yap-algorithm/internal/adapters/postgres"
	"yap-algorithm/internal/application"
	"yap-algorithm/internal/domain"
)

func TestSupabaseLocalFollows(t *testing.T) {
	if os.Getenv("YAP_SUPABASE_INTEGRATION") != "1" {
		t.Skip("local integration required")
	}
	cfg, err := pgxpool.ParseConfig(os.Getenv("YAP_SUPABASE_LOCAL_ADMIN_DSN"))
	if err != nil || !localSupabaseTarget(t).acceptsPool(cfg) || os.Getenv("YAP_SUPABASE_LOCAL_URL") != localSupabaseTarget(t).apiURL {
		t.Fatal("explicit local Supabase required")
	}
	cfg.MaxConns = 16
	ctx := context.Background()
	p, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	client := soakHTTP{os.Getenv("YAP_SUPABASE_LOCAL_URL"), os.Getenv("YAP_SUPABASE_LOCAL_ANON_KEY"), &http.Client{Timeout: 15 * time.Second}}
	rpc := func(t *testing.T, user, name string, args any, want int) map[string]any {
		t.Helper()
		raw, status, err := client.rpc(localMediaJWT(t, user), name, args)
		if err != nil || status != want {
			t.Fatalf("%s: status=%d want=%d error=%v response=%s", name, status, want, err, raw)
		}
		var out map[string]any
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	set := func(t *testing.T, user, author string, following bool, version int) map[string]any {
		t.Helper()
		return rpc(t, user, "set_feed_author_followed", map[string]any{"requested_id": domain.UUID(), "requested_author": author, "requested_following": following, "expected_version": version}, 200)
	}
	t.Run("following_is_not_friendship_or_a_location_permission", func(t *testing.T) {
		f := sbUsers(t, p)
		public := f.create(t, "Public followed post", "public")
		private := f.create(t, "Friends only post", "friends")
		sbExec(t, p, "update feed.posts set latitude=55.75,longitude=37.61 where id=$1", public)
		sbExec(t, p, "insert into public.user_locations(user_id,latitude,longitude,updated_at)values($1,55.75,37.61,statement_timestamp())on conflict(user_id)do update set latitude=excluded.latitude,longitude=excluded.longitude,updated_at=excluded.updated_at", f.viewer)
		set(t, f.viewer, f.author, true, 0)
		state := rpc(t, f.viewer, "get_feed_author_follow_state", map[string]any{"requested_author": f.author}, 200)
		if state["following"] != true || state["is_friend"] != false {
			t.Fatal(state)
		}
		feed := rpc(t, f.viewer, "get_friends_feed", map[string]any{"requested_limit": 30}, 200)
		items := feed["items"].([]any)
		if len(items) != 1 || items[0].(map[string]any)["post"].(map[string]any)["id"] != public {
			t.Fatal("subscription feed leaked friends-only or omitted public post", feed)
		}
		post := items[0].(map[string]any)["post"].(map[string]any)
		location, ok := post["location"].(map[string]any)
		if !ok || location["nearby"] != true || len(location) != 1 {
			t.Fatalf("follow granted precise coordinates: %v", post)
		}
		if post["author_relationship"].(map[string]any)["following"] != true {
			t.Fatal("missing relationship in post DTO")
		}
		raw, status, err := client.rpc(localMediaJWT(t, f.viewer), "get_feed_post", map[string]any{"requested_id": private})
		if err != nil || status != 200 || strings.TrimSpace(string(raw)) != "null" {
			t.Fatal("direct private post leaked", status, err)
		}
		// Incoming follow does not grant the followed author reciprocal access.
		var count int
		if err := p.QueryRow(ctx, "select count(*)from public.friendships where user_one_id=least($1::uuid,$2::uuid)and user_two_id=greatest($1::uuid,$2::uuid)", f.viewer, f.author).Scan(&count); err != nil || count != 0 {
			t.Fatal("follow created friendship", err)
		}
		sbExec(t, p, "insert into public.friendships(user_one_id,user_two_id)values(least($1::uuid,$2::uuid),greatest($1::uuid,$2::uuid))", f.viewer, f.author)
		feed = rpc(t, f.viewer, "get_friends_feed", map[string]any{"requested_limit": 30}, 200)
		if len(feed["items"].([]any)) != 2 {
			t.Fatal("friend+follow duplicated or lost posts", feed)
		}
		set(t, f.viewer, f.author, false, 1)
		feed = rpc(t, f.viewer, "get_friends_feed", map[string]any{"requested_limit": 30}, 200)
		if len(feed["items"].([]any)) != 2 {
			t.Fatal("unfollow removed independent friendship", feed)
		}
		sbExec(t, p, "delete from public.friendships where user_one_id=least($1::uuid,$2::uuid)and user_two_id=greatest($1::uuid,$2::uuid)", f.viewer, f.author)
		// A cached page must be rechecked after losing its last relationship.
		feed = rpc(t, f.viewer, "get_friends_feed", map[string]any{"requested_limit": 30, "requested_session": feed["session_id"]}, 200)
		if len(feed["items"].([]any)) != 0 {
			t.Fatal("old page survived loss of follow and friendship", feed)
		}
	})
	t.Run("retry_versions_and_parallel_intents", func(t *testing.T) {
		f := sbUsers(t, p)
		id := domain.UUID()
		args := map[string]any{"requested_id": id, "requested_author": f.author, "requested_following": true, "expected_version": 0}
		first := rpc(t, f.viewer, "set_feed_author_followed", args, 200)
		if first["version"] != float64(1) {
			t.Fatal(first)
		}
		set(t, f.viewer, f.author, false, 1)
		replay := rpc(t, f.viewer, "set_feed_author_followed", args, 200)
		if replay["replayed"] != true {
			t.Fatal("retry missing replay marker", replay)
		}
		state := rpc(t, f.viewer, "get_feed_author_follow_state", map[string]any{"requested_author": f.author}, 200)
		if state["following"] != false || state["version"] != float64(2) {
			t.Fatal("old retry overwrote unfollow", state)
		}
		args["requested_id"] = domain.UUID()
		rpc(t, f.viewer, "set_feed_author_followed", args, 409)
		args["requested_id"] = id
		args["requested_author"] = f.stranger
		rpc(t, f.viewer, "set_feed_author_followed", args, 409)
		var wg sync.WaitGroup
		var mu sync.Mutex
		wins, conflicts := 0, 0
		for i := 0; i < 24; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := f.rpc(f.viewer, "select public.set_feed_author_followed($1,$2,true,2)", domain.UUID(), f.author)
				mu.Lock()
				defer mu.Unlock()
				if err == nil {
					wins++
				} else {
					var problem *pgconn.PgError
					if !errors.As(err, &problem) || problem.Code != "PT409" {
						t.Error(err)
					} else {
						conflicts++
					}
				}
			}()
		}
		wg.Wait()
		if wins != 1 || conflicts != 23 {
			t.Fatalf("wins=%d conflicts=%d", wins, conflicts)
		}
	})
	t.Run("public_lists_cursor_live_block_ban_and_profile_delete", func(t *testing.T) {
		f := sbUsers(t, p)
		other := sbUsers(t, p)
		authors := []string{f.author, other.author, other.viewer, other.stranger}
		for _, author := range authors {
			set(t, f.viewer, author, true, 0)
		}
		first := rpc(t, f.stranger, "get_feed_following", map[string]any{"requested_user": f.viewer, "requested_limit": 2}, 200)
		if first["has_more"] != true || len(first["items"].([]any)) != 2 {
			t.Fatal(first)
		}
		cursor := first["next_cursor"].(map[string]any)
		cursor["requested_limit"] = 2
		next := rpc(t, f.stranger, "get_feed_following", cursor, 200)
		if next["has_more"] != false || len(next["items"].([]any)) != 2 {
			t.Fatal(next)
		}
		seen := map[string]bool{}
		for _, page := range []map[string]any{first, next} {
			for _, item := range page["items"].([]any) {
				user := item.(map[string]any)["user"].(map[string]any)
				id := user["id"].(string)
				if seen[id] {
					t.Fatal("duplicate follows cursor")
				}
				seen[id] = true
				if len(user) != 3 {
					t.Fatal("follow list exposed extra profile fields", user)
				}
			}
		}
		followers := rpc(t, f.stranger, "get_feed_followers", map[string]any{"requested_user": f.author, "requested_limit": 1}, 200)
		if len(followers["items"].([]any)) != 1 {
			t.Fatal("public incoming list missing follow", followers)
		}
		// List owner and list entries independently respect the reader's blocks.
		if _, err := f.rpc(f.stranger, "select to_jsonb(public.block_user($1))", other.author); err != nil {
			t.Fatal(err)
		}
		page := rpc(t, f.stranger, "get_feed_following", map[string]any{"requested_user": f.viewer}, 200)
		if len(page["items"].([]any)) != 3 {
			t.Fatal("blocked list entry visible", page)
		}
		sbExec(t, p, "insert into private.global_account_bans(target_user_id,note)values($1,'follow fixture')", other.viewer)
		page = rpc(t, f.stranger, "get_feed_following", map[string]any{"requested_user": f.viewer}, 200)
		if len(page["items"].([]any)) != 2 {
			t.Fatal("banned list entry visible", page)
		}
		sbExec(t, p, "delete from private.global_account_bans where target_user_id=$1", other.viewer)
		sbExec(t, p, "delete from public.profiles where id=$1", other.stranger)
		page = rpc(t, f.viewer, "get_feed_following", map[string]any{}, 200)
		if len(page["items"].([]any)) != 3 {
			t.Fatal("deleted profile left active relation", page)
		}
		if _, err := f.rpc(f.stranger, "select to_jsonb(public.block_user($1))", f.viewer); err != nil {
			t.Fatal(err)
		}
		rpc(t, f.stranger, "get_feed_following", map[string]any{"requested_user": f.viewer}, 404)
		rpc(t, f.stranger, "get_feed_followers", map[string]any{"requested_user": f.viewer}, 404)
	})
	t.Run("block_removes_both_directions_and_unblock_does_not_restore", func(t *testing.T) {
		f := sbUsers(t, p)
		set(t, f.viewer, f.author, true, 0)
		set(t, f.author, f.viewer, true, 0)
		if _, err := f.rpc(f.author, "select to_jsonb(public.block_user($1))", f.viewer); err != nil {
			t.Fatal(err)
		}
		var active int
		if err := p.QueryRow(ctx, "select count(*)from feed.author_follows where follower_id=any($1::uuid[])and following", []string{f.viewer, f.author}).Scan(&active); err != nil || active != 0 {
			t.Fatal("block retained active subscription", err)
		}
		if _, err := f.rpc(f.author, "select to_jsonb(public.unblock_user($1))", f.viewer); err != nil {
			t.Fatal(err)
		}
		state := rpc(t, f.viewer, "get_feed_author_follow_state", map[string]any{"requested_author": f.author}, 200)
		if state["following"] != false || state["version"] != float64(2) {
			t.Fatal("block did not fence old actions", state)
		}
		set(t, f.viewer, f.author, true, 2)
	})
	t.Run("all_new_reads_and_writes_have_independent_limits", func(t *testing.T) {
		f := sbUsers(t, p)
		for _, op := range []struct {
			name, quota string
			args        any
			limit       int
		}{
			{"get_feed_following", "following_read", map[string]any{}, 60},
			{"get_feed_followers", "followers_read", map[string]any{}, 60},
			{"get_feed_author_follow_state", "follow_state_read", map[string]any{"requested_author": f.author}, 120},
		} {
			sbExec(t, p, "insert into private.feed_social_limits(user_id,operation,window_started_at,requests,day_utc,day_count)values($1,$2,statement_timestamp(),$3,(statement_timestamp()at time zone 'UTC')::date,1)", f.viewer, op.quota, op.limit-1)
			rpc(t, f.viewer, op.name, op.args, 200)
			out := rpc(t, f.viewer, op.name, op.args, 400)
			if out["code"] != "42901" {
				t.Fatal("missing rate limit", op.name, out)
			}
		}
		set(t, f.viewer, f.author, true, 0)
		sbExec(t, p, "update private.feed_social_limits set requests=120 where user_id=$1 and operation='follow_write'", f.viewer)
		out := rpc(t, f.viewer, "set_feed_author_followed", map[string]any{"requested_id": domain.UUID(), "requested_author": f.author, "requested_following": false, "expected_version": 1}, 400)
		if out["code"] != "42901" {
			t.Fatal("write rate limit missing", out)
		}
	})
	t.Run("daily_creation_limit_does_not_break_exact_retries", func(t *testing.T) {
		f := sbUsers(t, p)
		request := map[string]any{"requested_id": domain.UUID(), "requested_author": f.author, "requested_following": true, "expected_version": 0}
		rpc(t, f.viewer, "set_feed_author_followed", request, 200)
		sbExec(t, p, "update private.feed_social_limits set day_count=500,window_started_at=statement_timestamp()-interval '1 minute'where user_id=$1 and operation='follow_create'", f.viewer)
		if out := rpc(t, f.viewer, "set_feed_author_followed", request, 200); out["replayed"] != true {
			t.Fatal(out)
		}
		out := rpc(t, f.viewer, "set_feed_author_followed", map[string]any{"requested_id": domain.UUID(), "requested_author": f.stranger, "requested_following": true, "expected_version": 0}, 400)
		if out["code"] != "42901" {
			t.Fatal("daily creation cap missing", out)
		}
		set(t, f.viewer, f.author, false, 1)
		sbExec(t, p, "update private.feed_social_limits set day_count=2000,window_started_at=statement_timestamp()-interval '1 minute'where user_id=$1 and operation='follow_write'", f.viewer)
		out = rpc(t, f.viewer, "set_feed_author_followed", map[string]any{"requested_id": domain.UUID(), "requested_author": f.author, "requested_following": false, "expected_version": 2}, 400)
		if out["code"] != "42901" {
			t.Fatal("daily write cap missing", out)
		}
	})
	t.Run("followed_reposts_and_private_originals", func(t *testing.T) {
		f := sbUsers(t, p)
		other := sbUsers(t, p)
		original := other.create(t, "Outside original", "public")
		if _, err := f.rpc(f.author, "select public.create_feed_repost($1,$2)", domain.UUID(), original); err != nil {
			t.Fatal(err)
		}
		set(t, f.viewer, f.author, true, 0)
		feed := rpc(t, f.viewer, "get_friends_feed", map[string]any{}, 200)
		if len(feed["items"].([]any)) != 1 {
			t.Fatal("followed author's repost missing", feed)
		}
		post := feed["items"].([]any)[0].(map[string]any)["post"].(map[string]any)
		if len(post["reposted_by"].([]any)) != 1 {
			t.Fatal("followed repost context missing", post)
		}
		sbExec(t, p, "update feed.posts set audience='friends'where id=$1", original)
		feed = rpc(t, f.viewer, "get_friends_feed", map[string]any{"requested_session": feed["session_id"]}, 200)
		if len(feed["items"].([]any)) != 0 {
			t.Fatal("cached followed repost leaked private original", feed)
		}
	})
	t.Run("block_races_follow_and_never_revives_a_relationship", func(t *testing.T) {
		for i := 0; i < 6; i++ {
			f := sbUsers(t, p)
			done := make(chan error, 2)
			go func() {
				_, err := f.rpc(f.viewer, "select public.set_feed_author_followed($1,$2,true,0)", domain.UUID(), f.author)
				done <- err
			}()
			go func() { _, err := f.rpc(f.author, "select to_jsonb(public.block_user($1))", f.viewer); done <- err }()
			for j := 0; j < 2; j++ {
				err := <-done
				if err != nil {
					var problem *pgconn.PgError
					if !errors.As(err, &problem) || problem.Code != "PT404" {
						t.Fatal(err)
					}
				}
			}
			var active bool
			if err := p.QueryRow(ctx, "select exists(select 1 from feed.author_follows where follower_id=$1 and author_id=$2 and following)", f.viewer, f.author).Scan(&active); err != nil || active {
				t.Fatal("follow/block race retained relation", err)
			}
		}
	})
	t.Run("new_follow_waits_for_account_delete_without_lock_inversion", func(t *testing.T) {
		f := sbUsers(t, p)
		tx, err := p.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		if _, err := tx.Exec(ctx, "select id from public.profiles where id=$1 for update", f.author); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() {
			_, err := f.rpc(f.viewer, "select public.set_feed_author_followed($1,$2,true,0)", domain.UUID(), f.author)
			done <- err
		}()
		select {
		case err := <-done:
			t.Fatal("follow did not wait for deleting profile", err)
		case <-time.After(100 * time.Millisecond):
		}
		if _, err := tx.Exec(ctx, "delete from public.profiles where id=$1", f.author); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		sbErrorCode(t, <-done, "PT404")
	})
	t.Run("validation_and_unfollow_remain_safe_for_a_banned_target", func(t *testing.T) {
		f := sbUsers(t, p)
		out := rpc(t, f.viewer, "set_feed_author_followed", map[string]any{"requested_id": domain.UUID(), "requested_author": f.viewer, "requested_following": true, "expected_version": 0}, 400)
		if out["code"] != "22023" {
			t.Fatal(out)
		}
		rpc(t, f.viewer, "get_feed_following", map[string]any{"requested_limit": 101}, 400)
		set(t, f.viewer, f.author, true, 0)
		sbExec(t, p, "insert into private.global_account_bans(target_user_id,note)values($1,'follow fixture')", f.author)
		state := rpc(t, f.viewer, "get_feed_author_follow_state", map[string]any{"requested_author": f.author}, 200)
		if state["can_follow"] != false {
			t.Fatal("banned target can be followed", state)
		}
		set(t, f.viewer, f.author, false, 1)
		rpc(t, f.viewer, "set_feed_author_followed", map[string]any{"requested_id": domain.UUID(), "requested_author": f.author, "requested_following": true, "expected_version": 2}, 404)
	})
	t.Run("follow_changes_semantic_profile_and_reset_keeps_explicit_follow", func(t *testing.T) {
		f := sbUsers(t, p)
		target := f.create(t, "Followed author's semantic text", "public")
		sbExec(t, p, "insert into recsys.post_embeddings(post_id,post_version,model,embedding)values($1,1,$2,$3::text::extensions.vector)", target, sbModel, "[1,"+strings.Repeat("0,", 382)+"0]")
		related := domain.UUID()
		if _, err := f.rpc(f.stranger, "select public.create_feed_post($1,'Another author, same semantic topic')", related); err != nil {
			t.Fatal(err)
		}
		sbExec(t, p, "insert into recsys.post_embeddings(post_id,post_version,model,embedding)values($1,1,$2,$3::text::extensions.vector)", related, sbModel, "[1,"+strings.Repeat("0,", 382)+"0]")
		set(t, f.viewer, f.author, true, 0)
		raw, err := os.ReadFile(localSupabaseTarget(t).workerConfig())
		if err != nil {
			t.Fatal(err)
		}
		var config struct {
			DSN string `json:"database_url"`
		}
		if json.Unmarshal(raw, &config) != nil {
			t.Fatal("worker config")
		}
		endpoint, err := url.Parse(config.DSN)
		if err != nil || !localSupabaseTarget(t).acceptsURL(endpoint) {
			t.Fatal("local worker required")
		}
		repo, err := postgres.OpenIndexRepository(ctx, config.DSN, sbModel, 3)
		if err != nil {
			t.Fatal(err)
		}
		defer repo.Close()
		rec, err := repo.Recommendations(ctx)
		if err != nil {
			t.Fatal(err)
		}
		processor := application.Personalizer{Repository: rec}
		process := func(wantClusters bool) {
			t.Helper()
			deadline := time.Now().Add(8 * time.Second)
			for time.Now().Before(deadline) {
				if _, err := processor.Once(ctx); err != nil {
					t.Fatal(err)
				}
				var count int
				var fresh bool
				if err := p.QueryRow(ctx, "select (select count(*)from recsys.interest_clusters where user_id=$1),processed_revision=revision from recsys.user_states where user_id=$1", f.viewer).Scan(&count, &fresh); err != nil {
					t.Fatal(err)
				}
				if fresh && (count > 0) == wantClusters {
					return
				}
				time.Sleep(100 * time.Millisecond)
			}
			t.Fatal("follow signal never converged")
		}
		process(true)
		var similar bool
		if err := p.QueryRow(ctx, "select exists(select 1 from recsys.feed_snapshot_items i join recsys.user_states s on s.current_snapshot=i.snapshot_id where s.user_id=$1 and i.post_id=$2 and 'similar_interests'=any(i.reasons))", f.viewer, related).Scan(&similar); err != nil || !similar {
			t.Fatal("follow did not transfer semantic interest to another author", err)
		}
		var source bool
		if err := p.QueryRow(ctx, "select exists(select 1 from recsys.feed_snapshot_items i join recsys.user_states s on s.current_snapshot=i.snapshot_id where s.user_id=$1 and i.post_id=$2 and 'following'=any(i.sources))", f.viewer, target).Scan(&source); err != nil || !source {
			t.Fatal("follow source missing", err)
		}
		rpc(t, f.viewer, "reset_recommendation_interests", map[string]any{"requested_id": domain.UUID()}, 200)
		process(true)
		set(t, f.viewer, f.author, false, 1)
		process(false)
		var vector string
		if err := p.QueryRow(ctx, "select embedding::text from recsys.post_embeddings where post_id=$1", target).Scan(&vector); err != nil || !bytes.HasPrefix([]byte(vector), []byte("[1,0")) {
			t.Fatal("follow changed author's text vector", err)
		}
	})
}
