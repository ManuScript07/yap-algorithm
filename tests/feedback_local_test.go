package tests

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"yap-algorithm/internal/domain"
)

// Exercise the production pure reducer against facts produced by real RPCs,
// rather than keeping an independent Go implementation of label semantics.
func feedbackOutcome(t *testing.T, p *pgxpool.Pool, user, post, event string) map[string]any {
	t.Helper()
	var rows []byte
	var start time.Time
	if err := p.QueryRow(context.Background(), `select coalesce(jsonb_agg(to_jsonb(f)order by sequence),'[]')from recsys.feedback_facts f where user_id=$1 or post_id=$2`, user, post).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if err := p.QueryRow(context.Background(), "select started_at from recsys.feedback_history_contract").Scan(&start); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	input, err := json.Marshal(map[string]any{"rows": json.RawMessage(rows), "exposure_event": event, "window_end": now, "as_of": now, "history_start": start})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "feedback.json")
	if err := os.WriteFile(path, input, 0600); err != nil {
		t.Fatal(err)
	}
	python := filepath.Join("..", ".venv", "bin", "python")
	if runtime.GOOS == "windows" {
		python = filepath.Join("..", ".venv", "Scripts", "python.exe")
	}
	python, err = filepath.Abs(python)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, python, "-c", `import json,sys;sys.path.insert(0,'ml');from feedback import derive_outcomes;x=json.load(open(sys.argv[1],encoding='utf-8'));rows=x.pop('rows');print(json.dumps(derive_outcomes(rows,**x)))`, path)
	command.Dir = ".."
	raw, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("feedback reducer: %v %s", err, raw)
	}
	var result map[string]any
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestSupabaseLocalFeedback(t *testing.T) {
	p := stage5Pool(t)
	ctx := context.Background()
	count := func(sql string, args ...any) int {
		t.Helper()
		var n int
		if err := p.QueryRow(ctx, sql, args...).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	confirm := func(t *testing.T, user, delivery string) string {
		t.Helper()
		sbExec(t, p, "update recsys.deliveries set delivered_at=statement_timestamp()-interval '10 seconds'where id=$1", delivery)
		id := domain.UUID()
		payload := map[string]any{"requested_events": []any{map[string]any{"event_id": id, "delivery_id": delivery, "kind": "impression", "visible_ms": 1000, "visible_fraction": .75}}}
		stage5RPC(t, user, "record_feed_events", payload, 200)
		stage5RPC(t, user, "record_feed_events", payload, 200)
		var event string
		if err := p.QueryRow(ctx, "select event_id from recsys.feedback_facts where user_id=$1 and request_id=$2 and kind='impression'", user, id).Scan(&event); err != nil {
			t.Fatal(err)
		}
		return event
	}
	t.Run("closed_tables_helpers_and_explicit_coverage_boundary", func(t *testing.T) {
		for _, table := range []string{"recsys.feedback_facts", "recsys.feedback_history_contract"} {
			for _, role := range []string{"anon", "authenticated", "yap_recommendation_worker", "yap_feed_media_worker"} {
				var safe bool
				if err := p.QueryRow(ctx, "select (select relrowsecurity from pg_class where oid=$1::regclass)and not has_table_privilege($2,$1,'SELECT,INSERT,UPDATE,DELETE')", table, role).Scan(&safe); err != nil || !safe {
					t.Fatalf("journal access %s/%s: %v", role, table, err)
				}
			}
		}
		var safe bool
		if err := p.QueryRow(ctx, `select not has_function_privilege('authenticated','private.capture_feed_feedback_fact()','EXECUTE')and not has_function_privilege('yap_recommendation_worker','private.attribute_feed_feedback_request()','EXECUTE')and exists(select 1 from recsys.feedback_history_contract where version=1 and retention_days=30 and isfinite(started_at)and started_at<=statement_timestamp())`).Scan(&safe); err != nil || !safe {
			t.Fatal("invalid contract boundary", err)
		}
	})
	t.Run("accepted_states_retries_noops_and_old_version_conflict", func(t *testing.T) {
		f := sbUsers(t, p)
		post := f.create(t, "Feedback history: Go", "public")
		delivery := sbDelivery(t, f, f.viewer, post)
		firstID := domain.UUID()
		args := map[string]any{"requested_id": firstID, "requested_delivery": delivery, "requested_action": "like", "expected_version": 0}
		stage5RPC(t, f.viewer, "apply_feed_action", args, 200)
		stage5RPC(t, f.viewer, "apply_feed_action", args, 200)
		if count("select count(*)from recsys.feedback_facts where user_id=$1 and kind='like'", f.viewer) != 1 {
			t.Fatal("retry duplicated a transition")
		}
		for i, action := range []string{"unlike", "like", "like", "less_similar", "undo_less_similar"} {
			stage5RPC(t, f.viewer, "apply_feed_action", map[string]any{"requested_id": domain.UUID(), "requested_delivery": delivery, "requested_action": action, "expected_version": i + 1}, 200)
		}
		args["requested_id"] = domain.UUID()
		stage5RPC(t, f.viewer, "apply_feed_action", args, 409)
		if count("select count(*)from recsys.feedback_facts where user_id=$1 and kind='like'", f.viewer) != 4 {
			t.Fatal("no-op checkpoint missing or stale action recorded")
		}
		if count("select count(*)from recsys.feedback_facts where user_id=$1 and(kind in('like','less_similar'))and(request_id is null or delivery_id<>$2::uuid or surface<>'direct')", f.viewer, delivery) != 0 {
			t.Fatal("action lost authoritative request/delivery context")
		}
		if count("select count(*)from recsys.feedback_facts where user_id=$1 and kind='impression'", f.viewer) != 0 {
			t.Fatal("action invented an impression")
		}
		event := confirm(t, f.viewer, delivery)
		for _, active := range []int{5000, 2000, 5000} {
			stage5RPC(t, f.viewer, "record_feed_events", map[string]any{"requested_events": []any{map[string]any{"event_id": domain.UUID(), "delivery_id": delivery, "kind": "dwell", "active_ms": active}}}, 200)
		}
		if count("select count(*)from recsys.feedback_facts where user_id=$1 and kind='dwell'", f.viewer) != 1 {
			t.Fatal("unchanged cumulative attention bloated the journal")
		}
		out := feedbackOutcome(t, p, f.viewer, post, event)
		states := out["outcomes"].(map[string]any)
		if out["eligible"] != true || states["liked"] != true || states["less_similar"] != false || states["active_ms"] != float64(5000) {
			t.Fatal(out)
		}
		// The same transaction must roll the mutation and fact back together.
		tx, err := p.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		if _, err := tx.Exec(ctx, "delete from feed.post_likes where user_id=$1 and post_id=$2", f.viewer, post); err != nil {
			t.Fatal(err)
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		if count("select count(*)from recsys.feedback_facts where user_id=$1 and kind='like'", f.viewer) != 4 {
			t.Fatal("rolled-back transition persisted")
		}
	})
	t.Run("social_entities_reports_and_unfollow_do_not_become_post_approval", func(t *testing.T) {
		f := sbUsers(t, p)
		post := f.create(t, "Social context", "public")
		delivery := sbDelivery(t, f, f.viewer, post)
		event := confirm(t, f.viewer, delivery)
		comment, reply := domain.UUID(), domain.UUID()
		stage5RPC(t, f.viewer, "create_feed_comment", map[string]any{"requested_id": comment, "requested_post": post, "requested_text": "A discussion"}, 200)
		stage5RPC(t, f.viewer, "create_feed_comment", map[string]any{"requested_id": reply, "requested_post": post, "requested_text": "A reply", "requested_reply_to": comment}, 200)
		stage5RPC(t, f.viewer, "delete_feed_comment", map[string]any{"requested_id": domain.UUID(), "requested_comment": comment, "expected_version": 1}, 200)
		other := domain.UUID()
		stage5RPC(t, f.stranger, "create_feed_comment", map[string]any{"requested_id": other, "requested_post": post, "requested_text": "Unrelated rude comment"}, 200)
		stage5RPC(t, f.viewer, "submit_feed_content_report", map[string]any{"requested_id": domain.UUID(), "requested_type": "comment", "requested_content": other, "requested_reason": "spam"}, 200)
		for i, text := range []string{"", "Quote commentary"} {
			id := domain.UUID()
			args := map[string]any{"requested_id": id, "requested_original": post, "requested_text": text}
			stage5RPC(t, f.viewer, "create_feed_repost", args, 200)
			stage5RPC(t, f.viewer, "create_feed_repost", args, 200)
			if i == 0 {
				stage5RPC(t, f.viewer, "delete_feed_repost", map[string]any{"requested_id": domain.UUID(), "requested_repost": id, "expected_version": 1}, 200)
			}
		}
		for i, following := range []bool{true, false} {
			stage5RPC(t, f.viewer, "set_feed_author_followed", map[string]any{"requested_id": domain.UUID(), "requested_author": f.author, "requested_following": following, "expected_version": i}, 200)
		}
		states := feedbackOutcome(t, p, f.viewer, post, event)["outcomes"].(map[string]any)
		if states["commented"] != true || states["reposted"] != false || states["quoted"] != true || states["comment_reported"] != true || states["post_reported"] != nil || states["liked"] != nil || states["follows_author"] != false {
			t.Fatal(states)
		}
		if count("select count(*)from recsys.feedback_facts where target_id=$1 and kind='comment'and root_id=$2::uuid and reply_to_id=$2::uuid", reply, comment) != 1 {
			t.Fatal("reply context missing")
		}
		if count("select count(*)from recsys.feedback_facts where user_id=$1 and kind='repost'and target_type<>'repost'", f.viewer) != 0 {
			t.Fatal("plain repost ID confused with a post ID")
		}
		if count("select count(*)from recsys.feedback_facts where user_id=$1 and(context::text like '%rude%'or context::text like '%commentary%')", f.viewer) != 0 {
			t.Fatal("raw content leaked into journal")
		}
	})
	t.Run("block_clears_follow_and_unblock_does_not_restore_it", func(t *testing.T) {
		f := sbUsers(t, p)
		post := f.create(t, "Follow lifecycle", "public")
		event := confirm(t, f.viewer, sbDelivery(t, f, f.viewer, post))
		for i, enabled := range []bool{true, false, true} {
			stage5RPC(t, f.viewer, "set_feed_author_followed", map[string]any{"requested_id": domain.UUID(), "requested_author": f.author, "requested_following": enabled, "expected_version": i}, 200)
		}
		for _, name := range []string{"block_user", "unblock_user"} {
			query := "select to_jsonb(public." + name + "($1))" // Fixed test operation names.
			if _, err := f.rpc(f.author, query, f.viewer); err != nil {
				t.Fatal(err)
			}
		}
		if feedbackOutcome(t, p, f.viewer, post, event)["outcomes"].(map[string]any)["follows_author"] != false {
			t.Fatal("unblock restored a removed follow")
		}
		stage5RPC(t, f.viewer, "set_feed_author_followed", map[string]any{"requested_id": domain.UUID(), "requested_author": f.author, "requested_following": true, "expected_version": 4}, 200)
		if feedbackOutcome(t, p, f.viewer, post, event)["outcomes"].(map[string]any)["follows_author"] != true {
			t.Fatal("accepted follow after unblock was ignored")
		}
	})
	t.Run("actual_snapshot_versions_survive_cleanup_and_reset_fences_old_epoch", func(t *testing.T) {
		f := sbUsers(t, p)
		post := f.create(t, "Snapshot contract", "public")
		page := sbRecommendPage(t, f, f.viewer, nil, -1, 10)
		if len(page.Items) == 0 {
			t.Fatal("empty fallback")
		}
		delivery := page.Items[0].Delivery
		sbExec(t, p, "update recsys.feed_snapshots set feature_version='test-feature-91',policy_version='test-policy-12',embedding_space='test-space'where id=$1", page.Session)
		event := confirm(t, f.viewer, delivery)
		sbExec(t, p, "delete from recsys.feed_snapshots where id=$1", page.Session)
		var correct bool
		if err := p.QueryRow(ctx, "select context->>'feature_version'='test-feature-91'and context->>'policy_version'='test-policy-12'and context->>'embedding_space'='test-space'from recsys.feedback_facts where event_id=$1", event).Scan(&correct); err != nil || !correct {
			t.Fatal("snapshot provenance lost", err)
		}
		stage5RPC(t, f.viewer, "reset_recommendation_interests", map[string]any{"requested_id": domain.UUID()}, 200)
		if feedbackOutcome(t, p, f.viewer, post, event)["eligible"] != false {
			t.Fatal("reset did not invalidate label")
		}
		out := stage5RPC(t, f.viewer, "record_feed_events", map[string]any{"requested_events": []any{map[string]any{"event_id": domain.UUID(), "delivery_id": delivery, "kind": "dwell", "active_ms": 1000}}}, 200)
		if out["stale"] != float64(1) || count("select count(*)from recsys.feedback_facts where user_id=$1 and kind='dwell'", f.viewer) != 0 {
			t.Fatal("stale telemetry recorded", out)
		}
	})
	t.Run("comment_like_versions_and_post_lifecycle_remain_distinct", func(t *testing.T) {
		f := sbUsers(t, p)
		post := f.create(t, "Versioned discussion", "public")
		delivery := sbDelivery(t, f, f.viewer, post)
		event := confirm(t, f.viewer, delivery)
		comment := domain.UUID()
		stage5RPC(t, f.stranger, "create_feed_comment", map[string]any{"requested_id": comment, "requested_post": post, "requested_text": "First version"}, 200)
		stage5RPC(t, f.viewer, "set_feed_comment_liked", map[string]any{"requested_id": domain.UUID(), "requested_comment": comment, "requested_liked": true, "expected_version": 0, "expected_comment_version": 1, "expected_post_version": 1}, 200)
		stage5RPC(t, f.stranger, "edit_feed_comment", map[string]any{"requested_id": domain.UUID(), "requested_comment": comment, "expected_version": 1, "requested_text": "Changed version"}, 200)
		if feedbackOutcome(t, p, f.viewer, post, event)["outcomes"].(map[string]any)["comment_liked"] != nil {
			t.Fatal("old comment like transferred to edited text")
		}
		stage5RPC(t, f.viewer, "set_feed_comment_liked", map[string]any{"requested_id": domain.UUID(), "requested_comment": comment, "requested_liked": false, "expected_version": 1, "expected_comment_version": 2, "expected_post_version": 1}, 200)
		if feedbackOutcome(t, p, f.viewer, post, event)["outcomes"].(map[string]any)["comment_liked"] != false {
			t.Fatal("comment unlike state lost")
		}
		stage5RPC(t, f.author, "edit_feed_post", map[string]any{"requested_id": post, "expected_version": 1, "requested_text": "Edited post"}, 200)
		if feedbackOutcome(t, p, f.viewer, post, event)["eligible"] != false {
			t.Fatal("observation transferred to edited post")
		}
		if _, err := f.rpc(f.author, "select public.delete_feed_post($1)", post); err != nil {
			t.Fatal(err)
		}
		var active bool
		if err := p.QueryRow(ctx, "select active from recsys.feedback_facts where post_id=$1 and kind='content'order by sequence desc limit 1", post).Scan(&active); err != nil || active {
			t.Fatal("post deletion lifecycle missing", err)
		}
	})
	t.Run("own_impression_no_statistics_inflation_and_failed_batch_is_atomic", func(t *testing.T) {
		f := sbUsers(t, p)
		post := f.create(t, "Own post", "public")
		delivery := sbDelivery(t, f, f.author, post)
		event := confirm(t, f.author, delivery)
		if feedbackOutcome(t, p, f.author, post, event)["eligible"] != false {
			t.Fatal("own exposure used for labels")
		}
		if count("select coalesce((select impressions from recsys.post_statistics where post_id=$1),0)", post) != 0 {
			t.Fatal("own view inflated impressions")
		}
		delivery = sbDelivery(t, f, f.viewer, post)
		sbExec(t, p, "update recsys.deliveries set delivered_at=statement_timestamp()-interval '10 seconds'where id=$1", delivery)
		stage5RPC(t, f.viewer, "record_feed_events", map[string]any{"requested_events": []any{
			map[string]any{"event_id": domain.UUID(), "delivery_id": delivery, "kind": "impression", "visible_ms": 1000, "visible_fraction": .75},
			map[string]any{"event_id": domain.UUID(), "delivery_id": delivery, "kind": "unknown"},
		}}, 400)
		if count("select count(*)from recsys.feedback_facts where user_id=$1", f.viewer) != 0 {
			t.Fatal("failed batch left facts")
		}
	})
	t.Run("bounded_retention_and_account_erasure", func(t *testing.T) {
		f := sbUsers(t, p)
		post := f.create(t, "Retention", "public")
		sbExec(t, p, `insert into recsys.feedback_facts(user_id,epoch,source,kind,target_type,target_id,active,recorded_at)
 select $1,1,'synthetic-test','like','post',$2,false,statement_timestamp()-interval '31 days'from generate_series(1,2001)`, f.viewer, post)
		sbExec(t, p, "select recsys.cleanup_recommendations()")
		if count("select count(*)from recsys.feedback_facts where user_id=$1", f.viewer) != 1001 {
			t.Fatal("cleanup not bounded")
		}
		sbExec(t, p, "select recsys.cleanup_recommendations()")
		sbExec(t, p, "select recsys.cleanup_recommendations()")
		if count("select count(*)from recsys.feedback_facts where user_id=$1", f.viewer) != 0 {
			t.Fatal("expired history remains")
		}
		delivery := sbDelivery(t, f, f.viewer, post)
		sbAction(t, f, f.viewer, domain.UUID(), delivery, "like")
		sbExec(t, p, "delete from public.profiles where id=$1", f.viewer)
		if count("select count(*)from recsys.feedback_facts where user_id=$1", f.viewer) != 0 {
			t.Fatal("removed account history remains")
		}
	})
}
