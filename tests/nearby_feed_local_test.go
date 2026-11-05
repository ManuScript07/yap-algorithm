package tests

import (
	"context"
	"testing"

	"yap-algorithm/internal/domain"
)

func TestSupabaseLocalNearby(t *testing.T) {
	p := stage5Pool(t)
	ctx := context.Background()
	locate := func(user string, lat, lon float64) {
		t.Helper()
		sbExec(t, p, "insert into public.user_locations(user_id,latitude,longitude,updated_at)values($1,$2,$3,statement_timestamp())on conflict(user_id)do update set latitude=excluded.latitude,longitude=excluded.longitude,updated_at=excluded.updated_at", user, lat, lon)
	}
	post := func(f sbFixture, text, audience string, lat, lon float64) string {
		t.Helper()
		id := domain.UUID()
		if _, err := f.rpc(f.author, "select public.publish_feed_post($1,$2,$3,'ru','{}',jsonb_build_object('latitude',$4::float8,'longitude',$5::float8))", id, text, audience, lat, lon); err != nil {
			t.Fatal(err)
		}
		return id
	}
	t.Run("bounded_pages_optional_geo_and_no_friend_permission_from_follow", func(t *testing.T) {
		f := sbUsers(t, p)
		missing := stage5RPC(t, f.viewer, "get_nearby_feed", map[string]any{}, 412)
		if missing["message"] != "nearby_location_required" {
			t.Fatal(missing)
		}
		locate(f.viewer, 55.75, 37.61)
		locate(f.stranger, 55.75, 37.61)
		near := post(f, "Nearby public", "public", 55.75, 37.61)
		private := post(f, "Nearby friends only", "friends", 55.75, 37.61)
		post(f, "Beyond radius", "public", 56.8, 37.61)
		post(f, "Bounding-box corner outside circle", "public", 56.18, 38.38)
		f.create(t, "No explicit geotag", "public")
		stage5RPC(t, f.viewer, "set_feed_author_followed", map[string]any{"requested_id": domain.UUID(), "requested_author": f.author, "requested_following": true, "expected_version": 0}, 200)
		first := stage5RPC(t, f.viewer, "get_nearby_feed", map[string]any{"requested_limit": 1}, 200)
		items := first["items"].([]any)
		if len(items) != 1 || items[0].(map[string]any)["post"].(map[string]any)["id"] != near || first["has_more"] != false {
			t.Fatal("geo/audience filtering failed", first)
		}
		location := items[0].(map[string]any)["post"].(map[string]any)["location"].(map[string]any)
		if len(location) != 1 || location["nearby"] != true {
			t.Fatal("follow revealed exact coordinates", location)
		}
		stage5RPC(t, f.stranger, "get_nearby_feed", map[string]any{"requested_session": first["session_id"], "requested_limit": 1}, 404)
		stage5RPC(t, f.viewer, "get_nearby_feed", map[string]any{"requested_session": first["session_id"], "requested_limit": 1, "requested_radius_km": 10}, 400)
		sbExec(t, p, "insert into public.friendships(user_one_id,user_two_id)values(least($1::uuid,$2::uuid),greatest($1::uuid,$2::uuid))", f.viewer, f.author)
		first = stage5RPC(t, f.viewer, "get_nearby_feed", map[string]any{"requested_limit": 1}, 200)
		if first["has_more"] != true {
			t.Fatal("new current friend did not gain allowed private post", first)
		}
		next := first["next_cursor"].(map[string]any)
		second := stage5RPC(t, f.viewer, "get_nearby_feed", map[string]any{"requested_session": next["session_id"], "after_position": next["after_position"], "requested_limit": 1}, 200)
		if len(second["items"].([]any)) != 1 {
			t.Fatal("missing next page", second)
		}
		a := first["items"].([]any)[0].(map[string]any)["post"].(map[string]any)["id"]
		b := second["items"].([]any)[0].(map[string]any)["post"].(map[string]any)["id"]
		if a == b || (a != near && a != private) || (b != near && b != private) {
			t.Fatal("pagination duplicated or leaked posts")
		}
		// Current bans are checked on an already issued page, without the worker.
		sbExec(t, p, "insert into private.global_account_bans(target_user_id,note)values($1,'nearby fixture')", f.author)
		replay := stage5RPC(t, f.viewer, "get_nearby_feed", map[string]any{"requested_session": first["session_id"], "requested_limit": 1}, 200)
		if len(replay["items"].([]any)) != 0 {
			t.Fatal("cached nearby page exposed banned author", replay)
		}
		sbExec(t, p, "delete from private.global_account_bans where target_user_id=$1", f.author)
		stage5RPC(t, f.viewer, "reset_recommendation_interests", map[string]any{"requested_id": domain.UUID()}, 200)
		stage5RPC(t, f.viewer, "get_nearby_feed", map[string]any{"requested_session": first["session_id"], "requested_limit": 1}, 409)
		sbExec(t, p, "update private.feed_social_limits set requests=60,window_started_at=statement_timestamp()where user_id=$1 and operation='nearby_read'", f.viewer)
		if out := stage5RPC(t, f.viewer, "get_nearby_feed", map[string]any{}, 400); out["code"] != "42901" {
			t.Fatal("nearby has no independent quota", out)
		}
	})
	t.Run("dateline_and_polar_longitude_bounds", func(t *testing.T) {
		f := sbUsers(t, p)
		locate(f.viewer, 0, 179.9)
		cross := post(f, "Across the date line", "public", 0, -179.9)
		post(f, "Far across the date line", "public", 0, -179)
		page := stage5RPC(t, f.viewer, "get_nearby_feed", map[string]any{}, 200)
		if len(page["items"].([]any)) != 1 || page["items"].([]any)[0].(map[string]any)["post"].(map[string]any)["id"] != cross {
			t.Fatal("dateline bounding failed", page)
		}
		locate(f.viewer, 89.9, 0)
		pole := post(f, "Across polar longitude", "public", 89.9, 170)
		page = stage5RPC(t, f.viewer, "get_nearby_feed", map[string]any{}, 200)
		if len(page["items"].([]any)) != 1 || page["items"].([]any)[0].(map[string]any)["post"].(map[string]any)["id"] != pole {
			t.Fatal("polar longitude bounding failed", page)
		}
	})
	t.Run("inaccessible_scan_window_advances_without_losing_older_visible_post", func(t *testing.T) {
		f := sbUsers(t, p)
		locate(f.viewer, 55, 37)
		visible := post(f, "Older public near post", "public", 55, 37)
		sbExec(t, p, "update feed.posts set created_at=statement_timestamp()-interval '1 hour'where id=$1", visible)
		// Stress fixtures intentionally bypass the 12/hour creation allowance.
		sbExec(t, p, "insert into feed.posts(id,author_id,text,audience,language,creation_request,latitude,longitude)select extensions.gen_random_uuid(),$1,'Hidden window','friends','ru','[]',55,37 from generate_series(1,2010)", f.author)
		first := stage5RPC(t, f.viewer, "get_nearby_feed", map[string]any{}, 200)
		if len(first["items"].([]any)) != 0 || first["has_more"] != true {
			t.Fatal("scan window was unbounded or silently truncated", first)
		}
		cursor := first["next_cursor"].(map[string]any)
		if cursor["after_position"].(float64) != 1999 {
			t.Fatal("empty window cursor did not advance", cursor)
		}
		second := stage5RPC(t, f.viewer, "get_nearby_feed", map[string]any{"requested_session": cursor["session_id"], "after_position": cursor["after_position"]}, 200)
		if len(second["items"].([]any)) != 1 || second["items"].([]any)[0].(map[string]any)["post"].(map[string]any)["id"] != visible {
			t.Fatal("bounded scan lost older visible post", second)
		}
		if second["has_more"] != false {
			t.Fatal("exhausted scan still has more", second)
		}
	})
	t.Run("nearby_uses_common_receipts_and_small_recommendation_source", func(t *testing.T) {
		f := sbUsers(t, p)
		locate(f.viewer, 55, 37)
		target := post(f, "Geographic signal to interests", "public", 55, 37)
		page := stage5RPC(t, f.viewer, "get_nearby_feed", map[string]any{}, 200)
		item := page["items"].([]any)[0].(map[string]any)
		result := stage5RPC(t, f.viewer, "apply_feed_action", map[string]any{"requested_id": domain.UUID(), "requested_delivery": item["delivery_id"], "requested_action": "like", "expected_version": 0}, 200)
		if result["liked"] != true {
			t.Fatal("nearby receipt did not work with common actions")
		}
		var surface string
		if err := p.QueryRow(ctx, "select surface from recsys.deliveries where id=$1", item["delivery_id"]).Scan(&surface); err != nil || surface != "nearby" {
			t.Fatal(surface, err)
		}
		_, rec := stage5Worker(t)
		// Feedback is debounced for one second. Make this fixture ready without
		// timing assertions or sleeping; the mixed-load test measures real delay.
		sbExec(t, p, "update recsys.refresh_jobs set available_at=statement_timestamp()where user_id=$1", f.viewer)
		job, ok, err := rec.ClaimRefresh(ctx)
		if err != nil || !ok || job.UserID != f.viewer {
			t.Fatal("common nearby feedback job", err)
		}
		input, err := rec.LoadProfile(ctx, job)
		if err != nil {
			t.Fatal(err)
		}
		candidates, err := rec.LoadCandidates(ctx, job, input, nil)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, candidate := range candidates {
			if candidate.ID == target {
				found = candidate.Nearby
			}
		}
		if !found {
			t.Fatal("nearby candidate lost its small geographic feature")
		}
	})
}
