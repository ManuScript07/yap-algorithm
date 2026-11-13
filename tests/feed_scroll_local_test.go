package tests

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"yap-algorithm/internal/domain"
)

type scrollItem struct {
	Position int    `json:"position"`
	Receipt  string `json:"delivery_id"`
	Model    string `json:"model"`
	Post     struct {
		ID            string `json:"id"`
		ActionVersion int64  `json:"action_version"`
		Liked         bool   `json:"liked"`
	} `json:"post"`
}

type scrollPage struct {
	Contract int          `json:"contract_version"`
	Session  string       `json:"session_id"`
	Next     string       `json:"next_cursor"`
	Status   string       `json:"status"`
	More     bool         `json:"has_more"`
	Items    []scrollItem `json:"items"`
}

func scrollArgs(session, cursor string, limit int) map[string]any {
	args := map[string]any{"requested_session": session, "requested_limit": limit}
	if cursor != "" {
		args["requested_cursor"] = cursor
	}
	return args
}

func readScroll(t *testing.T, user, session, cursor string, limit int) scrollPage {
	t.Helper()
	raw := stage5RPC(t, user, "get_for_you_feed_v2", scrollArgs(session, cursor, limit), 200)
	body, err := json.Marshal(raw)
	var page scrollPage
	if err != nil || json.Unmarshal(body, &page) != nil || page.Contract != 2 || page.Session != session ||
		page.More != (page.Next != "") || (page.Status == "exhausted") != !page.More ||
		(page.Status == "continue") != (len(page.Items) == 0 && page.More) || len(page.Items) > limit {
		t.Fatalf("invalid scroll envelope: %v", raw)
	}
	return page
}

func isolateScrollViewer(t *testing.T, p *pgxpool.Pool, f sbFixture) {
	t.Helper()
	sbExec(t, p, `insert into recsys.hidden_authors(user_id,author_id)
	select distinct $1::uuid,author_id from feed.posts where author_id<>$1::uuid and author_id<>$2::uuid on conflict do nothing`, f.viewer, f.author)
}

func insertScrollPosts(t *testing.T, p *pgxpool.Pool, author string, count int, age string, audience string) {
	t.Helper()
	sbExec(t, p, `insert into feed.posts(id,author_id,text,audience,language,creation_request,created_at)
	select extensions.gen_random_uuid(),$1,'Scroll fixture '||n,$3,'en','[]',statement_timestamp()-$4::interval
	from generate_series(1,$2::int)n`, author, count, audience, age)
}

func TestSupabaseLocalFeedScroll(t *testing.T) {
	p := stage5Pool(t)
	ctx := context.Background()
	var exposed bool
	if err := p.QueryRow(ctx, `select has_function_privilege('anon','public.get_for_you_feed_v2(uuid,uuid,integer)','EXECUTE')
		or has_table_privilege('authenticated','recsys.feed_scroll_pages','SELECT')
		or has_table_privilege('yap_recommendation_worker','recsys.feed_scroll_seen','SELECT')`).Scan(&exposed); err != nil || exposed {
		t.Fatal("scroll state is directly exposed", err)
	}
	t.Run("beyond_snapshot_with_stable_retries_and_live_access", func(t *testing.T) {
		f := sbUsers(t, p)
		isolateScrollViewer(t, p, f)
		insertScrollPosts(t, p, f.author, 310, "9 days", "public")
		snapshot := domain.UUID()
		sbExec(t, p, "select private.lock_recommendation_user($1)", f.viewer)
		sbExec(t, p, `insert into recsys.feed_snapshots(id,user_id,epoch,revision,model,embedding_model,policy_version,feature_version)
		select $1,user_id,epoch,revision,'personalized-v2',$3,'hybrid-v2','feed-features-v2' from recsys.user_states where user_id=$2`,
			snapshot, f.viewer, sbModel)
		sbExec(t, p, `insert into recsys.feed_snapshot_items(snapshot_id,position,post_id,post_version,features,reasons)
		select $1,row_number() over(order by id desc)-1,id,version,array_fill(0::float8,array[8]),array['semantic']
		from (select id,version from feed.posts where author_id=$2 order by id desc limit 200)p`, snapshot, f.author)
		sbExec(t, p, "update recsys.user_states set current_snapshot=$1 where user_id=$2", snapshot, f.viewer)
		session := domain.UUID()
		first := readScroll(t, f.viewer, session, "", 30)
		if len(first.Items) != 30 || first.Items[0].Model != "personalized-v2" {
			t.Fatal("prepared pool was not used first")
		}
		var policy, source string
		if err := p.QueryRow(ctx, `select private.feed_feedback_delivery_context(d)->>'policy_version',
			private.feed_feedback_delivery_context(d)->>'snapshot_id' from recsys.deliveries d where id=$1`,
			first.Items[0].Receipt).Scan(&policy, &source); err != nil || policy != "hybrid-v2" || source != snapshot {
			t.Fatal("prepared receipt lost source provenance", err)
		}
		insertScrollPosts(t, p, f.author, 1, "0 seconds", "public")
		sbExec(t, p, "update recsys.user_states set current_snapshot=null where user_id=$1", f.viewer)
		sbExec(t, p, "update recsys.feed_snapshots set expires_at=statement_timestamp()-interval '2 hours' where id=$1", snapshot)
		sbExec(t, p, "select recsys.cleanup_recommendations()")
		sbExec(t, p, "update feed.posts set deleted_at=statement_timestamp() where id=$1", first.Items[0].Post.ID)
		replay := readScroll(t, f.viewer, session, "", 30)
		if replay.Next != first.Next || !slices.Equal(replay.Items, first.Items[1:]) {
			t.Fatal("replay changed positions or exposed a deleted post")
		}
		client := soakHTTP{os.Getenv("YAP_SUPABASE_LOCAL_URL"), os.Getenv("YAP_SUPABASE_LOCAL_ANON_KEY"), &http.Client{Timeout: 15 * time.Second}}
		token := localMediaJWT(t, f.viewer)
		var bodies [2][]byte
		var wg sync.WaitGroup
		for i := range bodies {
			wg.Go(func() {
				raw, status, err := client.rpc(token, "get_for_you_feed_v2", scrollArgs(session, first.Next, 30))
				if err != nil || status != 200 {
					t.Errorf("concurrent page: status %d, %v", status, err)
				}
				bodies[i] = raw
			})
		}
		wg.Wait()
		var left, right scrollPage
		if json.Unmarshal(bodies[0], &left) != nil || json.Unmarshal(bodies[1], &right) != nil ||
			left.Next != right.Next || !slices.Equal(left.Items, right.Items) || len(left.Items) != 30 {
			t.Fatal("parallel cursor requests produced different pages")
		}
		seen := map[string]bool{}
		for _, item := range first.Items {
			seen[item.Post.ID] = true
		}
		next, archived, calls := first.Next, 0, 0
		for ; next != "" && calls < 35; calls++ {
			page := readScroll(t, f.viewer, session, next, 30)
			if page.Next == next {
				t.Fatal("cursor did not advance")
			}
			for _, item := range page.Items {
				if seen[item.Post.ID] || item.Position != len(seen) {
					t.Fatalf("unstable position or duplicate at %d", item.Position)
				}
				seen[item.Post.ID] = true
				if item.Model == "archive-v2" {
					archived++
				}
			}
			next = page.Next
		}
		if len(seen) != 310 || archived != 110 || next != "" {
			t.Fatalf("archive did not exhaust as expected: seen=%d archive=%d calls=%d", len(seen), archived, calls)
		}
		stage5RPC(t, f.stranger, "get_for_you_feed_v2", scrollArgs(session, first.Next, 30), 404)
		stage5RPC(t, f.viewer, "reset_recommendation_interests", map[string]string{"requested_id": domain.UUID()}, 200)
		stage5RPC(t, f.viewer, "get_for_you_feed_v2", scrollArgs(session, first.Next, 30), 409)
	})

	t.Run("sparse_archive_advances_even_when_a_page_is_empty", func(t *testing.T) {
		f := sbUsers(t, p)
		isolateScrollViewer(t, p, f)
		insertScrollPosts(t, p, f.author, 201, "8 days", "public")
		insertScrollPosts(t, p, f.author, 265, "1 day", "friends")
		session := domain.UUID()
		first := readScroll(t, f.viewer, session, "", 30)
		second := readScroll(t, f.viewer, session, first.Next, 30)
		if first.Status != "continue" || second.Status != "continue" || first.Next == second.Next {
			t.Fatal("sparse archive returned premature exhaustion")
		}
		next, count := second.Next, 0
		for i := 0; next != "" && i < 15; i++ {
			page := readScroll(t, f.viewer, session, next, 30)
			count += len(page.Items)
			next = page.Next
		}
		if count != 201 || next != "" {
			t.Fatalf("sparse archive lost old posts: got %d", count)
		}
		sbExec(t, p, "update recsys.feed_scrolls set expires_at=statement_timestamp()+interval '30 seconds' where request_id=$1", session)
		readScroll(t, f.viewer, session, first.Next, 30)
		var renewed bool
		if err := p.QueryRow(ctx, "select expires_at>statement_timestamp()+interval '18 minutes' from recsys.feed_scrolls where request_id=$1",
			session).Scan(&renewed); err != nil || !renewed {
			t.Fatal("active session did not renew its idle window", err)
		}
		sbExec(t, p, "update recsys.feed_scrolls set expires_at=statement_timestamp()-interval '1 second' where request_id=$1", session)
		stage5RPC(t, f.viewer, "get_for_you_feed_v2", scrollArgs(session, first.Next, 30), 410)
		sbExec(t, p, "select recsys.cleanup_recommendations()")
		stage5RPC(t, f.viewer, "get_for_you_feed_v2", scrollArgs(session, first.Next, 30), 404)
		sbExec(t, p, `update feed.posts set deleted_at=statement_timestamp() where id=(
			select id from feed.posts where author_id=$1 and audience='public' order by id limit 1)`, f.author)
		session, next, count = domain.UUID(), "", 0
		for i := 0; i < 15; i++ {
			page := readScroll(t, f.viewer, session, next, 25)
			count += len(page.Items)
			next = page.Next
			if next == "" {
				break
			}
		}
		if count != 200 || next != "" {
			t.Fatalf("exactly 200 accessible posts did not exhaust correctly: got %d", count)
		}
	})

	t.Run("small_inventory_repeats_once", func(t *testing.T) {
		f := sbUsers(t, p)
		isolateScrollViewer(t, p, f)
		session := domain.UUID()
		empty := readScroll(t, f.viewer, session, "", 5)
		if empty.Status != "exhausted" || len(empty.Items) != 0 {
			t.Fatal("empty corpus should terminate")
		}
		insertScrollPosts(t, p, f.author, 1, "1 day", "public")
		single := readScroll(t, f.viewer, domain.UUID(), "", 5)
		if single.More || len(single.Items) != 1 {
			t.Fatal("single post should not repeat")
		}
		insertScrollPosts(t, p, f.author, 2, "1 day", "public")
		session = domain.UUID()
		first := readScroll(t, f.viewer, session, "", 5)
		if !first.More || len(first.Items) != 3 {
			t.Fatal("expected all unique posts before repeats")
		}
		last := readScroll(t, f.viewer, session, first.Next, 5)
		if last.More || len(last.Items) != 2 || last.Items[0].Post.ID == last.Items[1].Post.ID ||
			last.Items[0].Post.ID == first.Items[2].Post.ID {
			t.Fatal("repeat budget or ordering was not respected")
		}
	})

	t.Run("archive_receipts_and_revoked_original_access", func(t *testing.T) {
		f := sbUsers(t, p)
		isolateScrollViewer(t, p, f)
		for _, viewer := range []string{f.viewer, f.stranger} {
			sbExec(t, p, "insert into public.friendships(user_one_id,user_two_id)values(least($1::uuid,$2::uuid),greatest($1::uuid,$2::uuid))",
				viewer, f.author)
		}
		original := f.create(t, "Friends-only original", "friends")
		quote := domain.UUID()
		stage5RPC(t, f.stranger, "create_feed_repost", map[string]any{
			"requested_id": quote, "requested_original": original, "requested_text": "Public caption",
		}, 200)
		session := domain.UUID()
		first := readScroll(t, f.viewer, session, "", 5)
		if len(first.Items) != 2 || first.Items[0].Post.ID != quote {
			t.Fatal("expected visible original and quote in the archive")
		}
		receipt := first.Items[0].Receipt
		sbExec(t, p, "update recsys.deliveries set delivered_at=statement_timestamp()-interval '3 seconds' where id=$1", receipt)
		event := domain.UUID()
		accepted := stage5RPC(t, f.viewer, "record_feed_events", map[string]any{"requested_events": []map[string]any{{
			"event_id": event, "delivery_id": receipt, "kind": "impression", "visible_ms": 1000, "visible_fraction": 1,
		}}}, 200)
		if accepted["accepted"] != float64(1) {
			t.Fatal("archive delivery did not accept impressions")
		}
		var proper bool
		if err := p.QueryRow(ctx, `select context->>'policy_version'='archive-v2' and context->>'feature_version'='none'
			from recsys.feedback_facts where request_id=$1 and user_id=$2 and kind='impression'`, event, f.viewer).Scan(&proper); err != nil || !proper {
			t.Fatal("archive telemetry was labeled as personalized", err)
		}
		sbExec(t, p, "delete from public.friendships where user_one_id=least($1::uuid,$2::uuid) and user_two_id=greatest($1::uuid,$2::uuid)",
			f.viewer, f.author)
		again := readScroll(t, f.viewer, session, "", 5)
		if len(again.Items) != 0 || len(readScroll(t, f.viewer, session, first.Next, 5).Items) != 0 {
			t.Fatal("a cached or repeated page exposed the private original")
		}
		sbExec(t, p, "insert into private.global_account_bans(target_user_id,note) values($1,'scroll test')", f.viewer)
		stage5RPC(t, f.viewer, "get_for_you_feed_v2", scrollArgs(session, "", 5), 400)
	})
}
