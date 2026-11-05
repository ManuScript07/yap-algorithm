package tests

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"yap-algorithm/internal/domain"
)

func TestSupabaseLocalSocialSecurity(t *testing.T) {
	if os.Getenv("YAP_SUPABASE_INTEGRATION") != "1" {
		t.Skip("local integration required")
	}
	cfg, err := pgxpool.ParseConfig(os.Getenv("YAP_SUPABASE_LOCAL_ADMIN_DSN"))
	if err != nil || cfg.ConnConfig.Host != "127.0.0.1" || cfg.ConnConfig.Port != 54322 {
		t.Fatal("local database required")
	}
	cfg.MaxConns = 16
	ctx := context.Background()
	p, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	f := sbUsers(t, p)
	c := soakHTTP{os.Getenv("YAP_SUPABASE_LOCAL_URL"), os.Getenv("YAP_SUPABASE_LOCAL_ANON_KEY"), &http.Client{Timeout: 15 * time.Second}}
	token := localMediaJWT(t, f.stranger)
	t.Run("report_limits_are_independent_and_keep_deleted_target_context", func(t *testing.T) {
		var posts []string
		for i := 0; i < 21; i++ {
			id := domain.UUID()
			posts = append(posts, id)
			sbExec(t, p, "insert into feed.posts(id,author_id,text,audience,language,creation_request)values($1,$2,'Report fixture','public','ru','[]')", id, f.author)
		}
		report := func(kind, id string, want int) {
			t.Helper()
			sbExec(t, p, "update private.feed_social_limits set window_started_at=statement_timestamp()-interval '1 minute'where user_id=$1 and operation=$2", f.stranger, "report_"+kind)
			body, status, err := c.rpc(token, "submit_feed_content_report", map[string]any{"requested_id": domain.UUID(), "requested_type": kind, "requested_content": id, "requested_reason": "spam"})
			if err != nil || status != want {
				t.Fatalf("%s report status=%d want=%d body=%s error=%v", kind, status, want, body, err)
			}
		}
		for _, id := range posts[:20] {
			report("post", id, 200)
		}
		report("post", posts[20], 400)
		var roots []string
		for i := 0; i < 31; i++ {
			id := domain.UUID()
			roots = append(roots, id)
			sbExec(t, p, "insert into feed.comments(id,post_id,root_id,author_id,text,post_version,creation_request)values($1,$2,$1,$3,'Comment report',1,'[]')", id, posts[20], f.viewer)
		}
		for _, id := range roots[:30] {
			report("comment", id, 200)
		}
		report("comment", roots[30], 400)
		if _, err := f.rpc(f.stranger, "select to_jsonb(public.submit_user_report($1,'spam'))", f.author); err != nil {
			t.Fatal("post reports blocked user reporting", err)
		}
		sbExec(t, p, "delete from feed.posts where id=any($1::uuid[])", posts)
		var reports, hides int
		if err := p.QueryRow(ctx, "select count(*)from private.user_reports where reporter_user_id=$1 and target_type<>'user'and content_snapshot->>'text'is not null", f.stranger).Scan(&reports); err != nil || reports != 50 {
			t.Fatalf("moderator context lost: %d %v", reports, err)
		}
		if err := p.QueryRow(ctx, "select count(*)from feed.reported_content_hides where user_id=$1", f.stranger).Scan(&hides); err != nil || hides != 0 {
			t.Fatal("deleted content left orphan hides", err)
		}
	})
	t.Run("concurrent_reactions_have_one_winner_and_no_duplicate_counts", func(t *testing.T) {
		post := f.create(t, "Concurrent comment reactions", "public")
		id := domain.UUID()
		if _, err := f.rpc(f.viewer, "select public.create_feed_comment($1,$2,'Reaction target',null)", id, post); err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		var mu sync.Mutex
		wins, conflicts := 0, 0
		for i := 0; i < 24; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := f.rpc(f.stranger, "select public.set_feed_comment_liked($1,$2,true,0,1,1)", domain.UUID(), id)
				mu.Lock()
				defer mu.Unlock()
				if err == nil {
					wins++
				} else {
					var e *pgconn.PgError
					if !errors.As(err, &e) || e.Code != "PT409" {
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
		var count int
		if err := p.QueryRow(ctx, "select likes from feed.comment_statistics where comment_id=$1", id).Scan(&count); err != nil || count != 1 {
			t.Fatal("like counter drift", err)
		}
	})
	t.Run("comment_burst_and_friends_cursor_fencing", func(t *testing.T) {
		users := sbUsers(t, p)
		post := users.create(t, "Comment quota", "public")
		var first string
		for i := 0; i < 30; i++ {
			id := domain.UUID()
			if i == 0 {
				first = id
			}
			if _, err := users.rpc(users.viewer, "select public.create_feed_comment($1,$2,'Quota comment')", id, post); err != nil {
				t.Fatal(err)
			}
		}
		_, err := users.rpc(users.viewer, "select public.create_feed_comment($1,$2,'Overflow')", domain.UUID(), post)
		sbErrorCode(t, err, "42901")
		if _, err := users.rpc(users.viewer, "select public.create_feed_comment($1,$2,'Quota comment')", first, post); err != nil {
			t.Fatal("replay consumed creation allowance", err)
		}
		sbExec(t, p, "insert into public.friendships(user_one_id,user_two_id)values(least($1::uuid,$2::uuid),greatest($1::uuid,$2::uuid))", users.author, users.viewer)
		raw, err := users.rpc(users.viewer, "select public.get_friends_feed()")
		if err != nil {
			t.Fatal(err)
		}
		var page struct {
			Session string            `json:"session_id"`
			Items   []json.RawMessage `json:"items"`
		}
		if err := json.Unmarshal(raw, &page); err != nil || len(page.Items) != 1 {
			t.Fatalf("friends first page: %s %v", raw, err)
		}
		_, err = users.rpc(users.stranger, "select public.get_friends_feed($1)", page.Session)
		sbErrorCode(t, err, "PT404")
		if _, err := users.rpc(users.viewer, "select public.reset_recommendation_interests($1)", domain.UUID()); err != nil {
			t.Fatal(err)
		}
		_, err = users.rpc(users.viewer, "select public.get_friends_feed($1)", page.Session)
		sbErrorCode(t, err, "PT409")
	})
	t.Run("reference_depth_namespace_and_current_blocking", func(t *testing.T) {
		post := f.create(t, "Reference original", "public")
		plain := domain.UUID()
		if _, err := f.rpc(f.viewer, "select public.create_feed_repost($1,$2)", plain, post); err != nil {
			t.Fatal(err)
		}
		_, err := f.rpc(f.author, "select public.create_feed_post($1,'collision')", plain)
		sbErrorCode(t, err, "23505")
		original := post
		for i := 0; i < 4; i++ {
			id := domain.UUID()
			if _, err := f.rpc(f.viewer, "select public.create_feed_repost($1,$2,'Quote')", id, original); err != nil {
				t.Fatal(err)
			}
			original = id
		}
		_, err = f.rpc(f.viewer, "select public.create_feed_repost($1,$2,'Too deep')", domain.UUID(), original)
		sbErrorCode(t, err, "22023")
		root := domain.UUID()
		if _, err := f.rpc(f.viewer, "select public.create_feed_comment($1,$2,'Blocked target',null)", root, post); err != nil {
			t.Fatal(err)
		}
		sbExec(t, p, "insert into public.user_blocks(blocker_user_id,blocked_user_id)values($1,$2)", f.viewer, f.stranger)
		_, err = f.rpc(f.stranger, "select public.create_feed_comment($1,$2,'Cannot address blocked author',$3)", domain.UUID(), post, root)
		sbErrorCode(t, err, "PT404")
		sbExec(t, p, "delete from public.user_blocks where blocker_user_id=$1 and blocked_user_id=$2", f.viewer, f.stranger)
		// A current global ban applies to already issued page sessions too.
		raw, err := f.rpc(f.stranger, "select public.get_feed_comments($1)", post)
		if err != nil {
			t.Fatal(err)
		}
		var page map[string]any
		json.Unmarshal(raw, &page)
		sbExec(t, p, "insert into private.global_account_bans(target_user_id,note)values($1,'social fixture')", f.viewer)
		raw, err = f.rpc(f.stranger, "select public.get_feed_comments($1,null,'popular',$2,-1,20)", post, page["session_id"])
		if err != nil {
			t.Fatal(err)
		}
		var got struct {
			Items []struct {
				Author any `json:"author"`
				Text   any `json:"text"`
			} `json:"items"`
		}
		json.Unmarshal(raw, &got)
		for _, row := range got.Items {
			if row.Author != nil || row.Text != nil {
				t.Fatal("cached session exposed banned author")
			}
		}
		sbExec(t, p, "delete from private.global_account_bans where target_user_id=$1", f.viewer)
	})
}
