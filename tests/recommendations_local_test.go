package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"yap-algorithm/internal/adapters/postgres"
	"yap-algorithm/internal/application"
	"yap-algorithm/internal/domain"
	"yap-algorithm/internal/ranking"
)

type sbRecommendationPage struct {
	Session string `json:"session_id"`
	Model   string `json:"model"`
	Items   []struct {
		Position int    `json:"position"`
		Delivery string `json:"delivery_id"`
		Post     struct {
			ID     string `json:"id"`
			Author string `json:"author_id"`
		} `json:"post"`
	} `json:"items"`
	Next *struct {
		Session string `json:"session_id"`
		After   int    `json:"after_position"`
	} `json:"next_cursor"`
}

func sbRecommendPage(t *testing.T, f sbFixture, user string, session any, after, limit int) sbRecommendationPage {
	t.Helper()
	raw, err := f.rpc(user, "select public.get_for_you_feed($1,$2,$3)", session, after, limit)
	if err != nil {
		t.Fatal(err)
	}
	var page sbRecommendationPage
	if err := json.Unmarshal(raw, &page); err != nil {
		t.Fatal(err)
	}
	return page
}
func sbDelivery(t *testing.T, f sbFixture, user, post string) string {
	t.Helper()
	raw, err := f.rpc(user, "select public.get_feed_post($1)", post)
	if err != nil {
		t.Fatal(err)
	}
	var body struct {
		Delivery string `json:"delivery_id"`
	}
	if err := json.Unmarshal(raw, &body); err != nil || body.Delivery == "" {
		t.Fatalf("missing delivery %v", err)
	}
	return body.Delivery
}
func sbAction(t *testing.T, f sbFixture, user, id, delivery, kind string) map[string]any {
	t.Helper()
	var version int64
	if err := f.admin.QueryRow(context.Background(), `select coalesce(
 (select (payload->>3)::bigint from recsys.action_requests where user_id=$1 and id=$2),
 (select v.version from recsys.post_action_versions v join recsys.deliveries d on d.post_id=v.post_id where d.id=$3 and v.user_id=$1),0)`, user, id, delivery).Scan(&version); err != nil {
		t.Fatal(err)
	}
	raw, err := f.rpc(user, "select public.apply_feed_action($1,$2,$3,$4)", id, delivery, kind, version)
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	return out
}
func sbCorpus(t *testing.T, f sbFixture, count int, splitAuthors bool) []string {
	t.Helper()
	ids := make([]string, 0, count)
	for i := 0; i < count; i++ {
		id := domain.UUID()
		author := f.author
		axis := i % 2
		if splitAuthors && axis == 1 {
			author = f.stranger
		}
		sbExec(t, f.admin, `insert into feed.posts(id,author_id,text,audience,language,creation_request)values($1,$2,$3,'public','en','[]')`, id, author, fmt.Sprintf("topic %d sample %d", axis, i))
		v := make([]float64, domain.Dimensions)
		v[axis] = 1
		vector, err := domain.Vector(v)
		if err != nil {
			t.Fatal(err)
		}
		sbExec(t, f.admin, "insert into recsys.post_embeddings(post_id,post_version,model,embedding)values($1,1,$2,$3::text::extensions.vector)", id, sbModel, vector)
		sbExec(t, f.admin, "update recsys.index_jobs set done_at=statement_timestamp()where post_id=$1", id)
		ids = append(ids, id)
	}
	return ids
}
func sbRefresh(t *testing.T, f sbFixture, repo *postgres.RecommendationRepository) {
	t.Helper()
	sbExec(t, f.admin, "update recsys.refresh_jobs set available_at=statement_timestamp()where user_id=any($1::uuid[])", []string{f.author, f.viewer, f.stranger})
	for i := 0; i < 20; i++ {
		worked, err := (application.Personalizer{Repository: repo}).Once(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if !worked {
			return
		}
	}
	t.Fatal("refresh did not converge")
}

func sbRecommendations(t *testing.T, admin *pgxpool.Pool, index *postgres.IndexRepository, workerPool *pgxpool.Pool) {
	repo, err := index.Recommendations(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	t.Run("cold_start_stable_pagination_and_deduplicated_refresh", func(t *testing.T) {
		f := sbUsers(t, admin)
		sbCorpus(t, f, 27, false)
		page := sbRecommendPage(t, f, f.viewer, nil, -1, 7)
		if page.Model != "fallback-v1" || len(page.Items) != 7 || page.Next == nil {
			t.Fatal("cold-start fallback missing")
		}
		replay := sbRecommendPage(t, f, f.viewer, page.Session, -1, 7)
		if replay.Session != page.Session || replay.Items[0].Delivery != page.Items[0].Delivery {
			t.Fatal("page retry changed the session/delivery")
		}
		ids := map[string]bool{}
		for {
			for _, item := range page.Items {
				if ids[item.Post.ID] {
					t.Fatal("duplicate across pages")
				}
				ids[item.Post.ID] = true
			}
			if page.Next == nil {
				break
			}
			page = sbRecommendPage(t, f, f.viewer, page.Next.Session, page.Next.After, 7)
		}
		if len(ids) != 27 {
			t.Fatalf("pagination lost posts: %d", len(ids))
		}
		var jobs int
		if err := admin.QueryRow(ctx, "select count(*)from recsys.refresh_jobs where user_id=$1", f.viewer).Scan(&jobs); err != nil || jobs != 1 {
			t.Fatal("refresh requests were not coalesced")
		}
		sbRefresh(t, f, repo)
		prepared := sbRecommendPage(t, f, f.viewer, nil, -1, 7)
		if prepared.Model != "personalized-v1" {
			t.Fatal("worker snapshot not served")
		}
		_, err := f.rpc(f.stranger, "select public.get_for_you_feed($1,-1,7)", prepared.Session)
		sbErrorCode(t, err, "PT404")
		_, err = f.rpc(f.viewer, "select public.get_for_you_feed(null,0,10000)")
		sbErrorCode(t, err, "22023")
	})
	t.Run("personalization_like_undo_and_late_retry", func(t *testing.T) {
		f := sbUsers(t, admin)
		posts := sbCorpus(t, f, 8, false)
		tech := sbDelivery(t, f, f.viewer, posts[0])
		sports := sbDelivery(t, f, f.stranger, posts[1])
		likeID := domain.UUID()
		first := sbAction(t, f, f.viewer, likeID, tech, "like")
		for i := 0; i < 3; i++ {
			if result := sbAction(t, f, f.viewer, likeID, tech, "like"); result["replayed"] != true {
				t.Fatal("retry not recognized")
			}
		}
		sbAction(t, f, f.stranger, domain.UUID(), sports, "like")
		sbRefresh(t, f, repo)
		a := sbRecommendPage(t, f, f.viewer, nil, -1, 8)
		b := sbRecommendPage(t, f, f.stranger, nil, -1, 8)
		techIDs, sportIDs := map[string]bool{}, map[string]bool{}
		for i, id := range posts {
			if i%2 == 0 {
				techIDs[id] = true
			} else {
				sportIDs[id] = true
			}
		}
		if len(a.Items) == 0 || len(b.Items) == 0 || !techIDs[a.Items[0].Post.ID] || !sportIDs[b.Items[0].Post.ID] {
			t.Fatal("different interests did not change the first recommendation")
		}
		undo := sbAction(t, f, f.viewer, domain.UUID(), tech, "unlike")
		if undo["liked"] != false || undo["revision"].(float64) <= first["revision"].(float64) {
			t.Fatal("undo failed")
		}
		sbAction(t, f, f.viewer, likeID, tech, "like")
		var likes int
		var revision int64
		if err := admin.QueryRow(ctx, "select (select count(*)from feed.post_likes where user_id=$1),(select revision from recsys.user_states where user_id=$1)", f.viewer).Scan(&likes, &revision); err != nil || likes != 0 || float64(revision) != undo["revision"] {
			t.Fatal("late retry resurrected the like")
		}
		sbRefresh(t, f, repo)
		var topics int
		_ = admin.QueryRow(ctx, "select count(*)from recsys.interest_clusters where user_id=$1", f.viewer).Scan(&topics)
		if topics != 0 {
			t.Fatal("unlike left an old interest behind")
		}
		_, err := f.rpc(f.viewer, "select public.apply_feed_action($1,$2,'unlike',0)", likeID, tech)
		sbErrorCode(t, err, "23505")
	})
	t.Run("semantic_negative_hide_and_reset", func(t *testing.T) {
		f := sbUsers(t, admin)
		posts := sbCorpus(t, f, 12, true)
		old := sbRecommendPage(t, f, f.viewer, nil, -1, 20)
		delivery := sbDelivery(t, f, f.viewer, posts[0])
		sbAction(t, f, f.viewer, domain.UUID(), delivery, "less_similar")
		page := sbRecommendPage(t, f, f.viewer, old.Session, -1, 20)
		for _, item := range page.Items {
			if item.Post.ID == posts[0] {
				t.Fatal("suppressed post visible in old session")
			}
		}
		sbRefresh(t, f, repo)
		page = sbRecommendPage(t, f, f.viewer, nil, -1, 20)
		if len(page.Items) == 0 || page.Items[0].Post.Author != f.stranger {
			t.Fatal("semantic negative did not reduce related content")
		}
		hideID := domain.UUID()
		if _, err := f.rpc(f.viewer, "select public.set_recommendation_author_hidden($1,$2,true)", hideID, f.author); err != nil {
			t.Fatal(err)
		}
		page = sbRecommendPage(t, f, f.viewer, old.Session, -1, 20)
		for _, item := range page.Items {
			if item.Post.Author == f.author {
				t.Fatal("hidden author visible in existing recommendations")
			}
		}
		_ = sbDelivery(t, f, f.viewer, posts[0]) // hiding recommendations is not a block
		resetID := domain.UUID()
		raw, err := f.rpc(f.viewer, "select public.reset_recommendation_interests($1)", resetID)
		if err != nil {
			t.Fatal(err)
		}
		var reset map[string]any
		_ = json.Unmarshal(raw, &reset)
		if _, err := f.rpc(f.viewer, "select public.reset_recommendation_interests($1)", resetID); err != nil {
			t.Fatal(err)
		}
		var epoch, hidden, suppressed int
		if err := admin.QueryRow(ctx, "select (select epoch from recsys.user_states where user_id=$1),(select count(*)from recsys.hidden_authors where user_id=$1),(select count(*)from recsys.semantic_suppressions where user_id=$1)", f.viewer).Scan(&epoch, &hidden, &suppressed); err != nil || float64(epoch) != reset["epoch"] || hidden != 1 || suppressed != 0 {
			t.Fatal("reset was not idempotent or removed explicit hiding")
		}
		_, err = f.rpc(f.viewer, "select public.get_for_you_feed($1,-1,20)", old.Session)
		sbErrorCode(t, err, "PT409")
		_, err = f.rpc(f.viewer, "select public.apply_feed_action($1,$2,'like',0)", domain.UUID(), delivery)
		sbErrorCode(t, err, "22023")
		sbRefresh(t, f, repo)
	})
	t.Run("live_access_checks_refill_existing_pages", func(t *testing.T) {
		f := sbUsers(t, admin)
		posts := sbCorpus(t, f, 25, true)
		page := sbRecommendPage(t, f, f.viewer, nil, -1, 3)
		sbExec(t, admin, "insert into public.user_blocks(blocker_user_id,blocked_user_id)values($1,$2)", f.viewer, f.author)
		next := sbRecommendPage(t, f, f.viewer, page.Session, -1, 3)
		if len(next.Items) != 3 || next.Next == nil {
			t.Fatal("blocked positions caused premature pagination end")
		}
		for _, item := range next.Items {
			if item.Post.Author == f.author {
				t.Fatal("old cursor bypassed the block")
			}
		}
		sbExec(t, admin, "delete from public.user_blocks where blocker_user_id=$1", f.viewer)
		sbExec(t, admin, "update feed.posts set audience='friends'where author_id=$1", f.author)
		next = sbRecommendPage(t, f, f.viewer, page.Session, -1, 30)
		for _, item := range next.Items {
			if item.Post.Author == f.author {
				t.Fatal("friends-only post leaked")
			}
		}
		sbExec(t, admin, "insert into public.friendships(user_one_id,user_two_id)values(least($1::uuid,$2::uuid),greatest($1::uuid,$2::uuid))", f.viewer, f.author)
		next = sbRecommendPage(t, f, f.viewer, page.Session, -1, 30)
		if len(next.Items) != 25 {
			t.Fatal("current friends cannot see old friends-only posts")
		}
		sbExec(t, admin, "delete from public.friendships where user_one_id=least($1::uuid,$2::uuid)and user_two_id=greatest($1::uuid,$2::uuid)", f.viewer, f.author)
		sbExec(t, admin, "update feed.posts set text='edited'where id=$1", posts[1])
		next = sbRecommendPage(t, f, f.viewer, page.Session, -1, 30)
		for _, item := range next.Items {
			if item.Post.Author == f.author || item.Post.ID == posts[1] {
				t.Fatal("removed friendship or edited version bypassed live checks")
			}
		}
		sbExec(t, admin, "insert into private.global_account_bans(target_user_id,note)values($1,'local test')", f.stranger)
		next = sbRecommendPage(t, f, f.viewer, page.Session, -1, 30)
		if len(next.Items) != 0 {
			t.Fatal("globally banned author visible")
		}
	})
	t.Run("parallel_publication_limit", func(t *testing.T) {
		f := sbUsers(t, admin)
		var group sync.WaitGroup
		codes := make(chan error, 13)
		for i := 0; i < 13; i++ {
			group.Add(1)
			go func() {
				defer group.Done()
				_, err := f.rpc(f.author, "select public.create_feed_post($1,'parallel publication','public','en')", domain.UUID())
				codes <- err
			}()
		}
		group.Wait()
		close(codes)
		success, limited := 0, 0
		for err := range codes {
			if err == nil {
				success++
			} else {
				sbErrorCode(t, err, "42901")
				limited++
			}
		}
		if success != 12 || limited != 1 {
			t.Fatalf("hourly limit race: %d accepted, %d rejected", success, limited)
		}
	})
	t.Run("late_first_delivery_cannot_overwrite_newer_intent", func(t *testing.T) {
		f := sbUsers(t, admin)
		posts := sbCorpus(t, f, 2, false)
		delivery := sbDelivery(t, f, f.viewer, posts[0])
		// 'Unlike' is already the current state, but is still a newer intent.
		if _, err := f.rpc(f.viewer, "select public.apply_feed_action($1,$2,'unlike',0)", domain.UUID(), delivery); err != nil {
			t.Fatal(err)
		}
		_, err := f.rpc(f.viewer, "select public.apply_feed_action($1,$2,'like',0)", domain.UUID(), delivery)
		sbErrorCode(t, err, "PT409")
		var likes int
		_ = admin.QueryRow(ctx, "select count(*)from feed.post_likes where user_id=$1", f.viewer).Scan(&likes)
		if likes != 0 {
			t.Fatal("an out-of-order intent resurrected a like")
		}
	})
	t.Run("worker_raw_access_stays_closed", func(t *testing.T) {
		for _, query := range []string{"select * from recsys.deliveries", "select * from recsys.interest_clusters", "select * from feed.post_likes"} {
			_, err := workerPool.Exec(ctx, query)
			sbErrorCode(t, err, "42501")
		}
	})
	t.Run("expired_lease_and_content_version_fencing", func(t *testing.T) {
		f := sbUsers(t, admin)
		posts := sbCorpus(t, f, 4, false)
		delivery := sbDelivery(t, f, f.viewer, posts[0])
		sbAction(t, f, f.viewer, domain.UUID(), delivery, "like")
		sbExec(t, admin, "update recsys.refresh_jobs set available_at=statement_timestamp()where user_id=$1", f.viewer)
		old, found, err := repo.ClaimRefresh(ctx)
		if err != nil || !found {
			t.Fatal("missing initial lease")
		}
		input, err := repo.LoadProfile(ctx, old)
		if err != nil || len(input.Evidence) != 1 {
			t.Fatalf("profile input: %v", err)
		}
		sbExec(t, admin, "update recsys.refresh_jobs set locked_until=clock_timestamp()-interval '1 second'where user_id=$1", f.viewer)
		current, found, err := repo.ClaimRefresh(ctx)
		if err != nil || !found || old.Lease == current.Lease {
			t.Fatal("lease was not reclaimed")
		}
		if accepted, err := repo.Publish(ctx, old, application.RecommendationResult{Input: input, FeatureVersion: application.PersonalizationVersion}); err != nil || accepted {
			t.Fatal("expired owner published")
		}
		if err := repo.RetryRefresh(ctx, old); err != nil {
			t.Fatal(err)
		}
		input, err = repo.LoadProfile(ctx, current)
		if err != nil {
			t.Fatal(err)
		}
		sbExec(t, admin, "update feed.posts set text='different meaning'where id=$1", posts[0])
		if accepted, err := repo.Publish(ctx, current, application.RecommendationResult{Input: input, Profile: ranking.BuildProfile(input.Evidence, input.At), FeatureVersion: application.PersonalizationVersion}); err != nil || accepted {
			t.Fatal("obsolete text evidence published")
		}
		sbRefresh(t, f, repo)
		var topics int
		_ = admin.QueryRow(ctx, "select count(*)from recsys.interest_clusters where user_id=$1", f.viewer).Scan(&topics)
		if topics != 0 {
			t.Fatal("old like was applied to edited text")
		}
	})
	t.Run("feedback_arriving_during_calculation_is_not_lost", func(t *testing.T) {
		f := sbUsers(t, admin)
		posts := sbCorpus(t, f, 4, false)
		sbRecommendPage(t, f, f.viewer, nil, -1, 20)
		job, found, err := repo.ClaimRefresh(ctx)
		if err != nil || !found {
			t.Fatal("missing refresh")
		}
		input, err := repo.LoadProfile(ctx, job)
		if err != nil {
			t.Fatal(err)
		}
		delivery := sbDelivery(t, f, f.viewer, posts[0])
		sbAction(t, f, f.viewer, domain.UUID(), delivery, "like")
		if accepted, err := repo.Publish(ctx, job, application.RecommendationResult{Input: input, FeatureVersion: application.PersonalizationVersion}); err != nil || !accepted {
			t.Fatalf("intermediate snapshot: %v", err)
		}
		var remaining int
		if err := admin.QueryRow(ctx, "select count(*)from recsys.refresh_jobs where user_id=$1 and target_revision>$2", f.viewer, input.Revision).Scan(&remaining); err != nil || remaining != 1 {
			t.Fatal("newer feedback was acknowledged by an older calculation")
		}
		sbRefresh(t, f, repo)
		var lag int64
		if err := admin.QueryRow(ctx, "select revision-processed_revision from recsys.user_states where user_id=$1", f.viewer).Scan(&lag); err != nil || lag != 0 {
			t.Fatal("feedback did not converge")
		}
	})
	t.Run("publication_waits_for_concurrent_text_edit", func(t *testing.T) {
		f := sbUsers(t, admin)
		posts := sbCorpus(t, f, 2, false)
		delivery := sbDelivery(t, f, f.viewer, posts[0])
		sbAction(t, f, f.viewer, domain.UUID(), delivery, "like")
		sbExec(t, admin, "update recsys.refresh_jobs set available_at=statement_timestamp()where user_id=$1", f.viewer)
		job, found, err := repo.ClaimRefresh(ctx)
		if err != nil || !found {
			t.Fatal("missing job")
		}
		input, err := repo.LoadProfile(ctx, job)
		if err != nil {
			t.Fatal(err)
		}
		tx, err := admin.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		if _, err := tx.Exec(ctx, "select id from feed.posts where id=$1 for update", posts[0]); err != nil {
			t.Fatal(err)
		}
		type outcome struct {
			accepted bool
			err      error
		}
		done := make(chan outcome, 1)
		go func() {
			accepted, err := repo.Publish(ctx, job, application.RecommendationResult{Input: input, Profile: ranking.BuildProfile(input.Evidence, input.At), FeatureVersion: application.PersonalizationVersion})
			done <- outcome{accepted, err}
		}()
		locked := false
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if err := admin.QueryRow(ctx, `select exists(select 1 from pg_stat_activity where query like 'select recsys.publish_recommendations%' and wait_event_type='Lock')`).Scan(&locked); err != nil {
				t.Fatal(err)
			}
			if locked {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		if !locked {
			t.Fatal("publication did not protect its evidence row")
		}
		if _, err := tx.Exec(ctx, "update feed.posts set text='changed while publishing'where id=$1", posts[0]); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		result := <-done
		if result.err != nil || result.accepted {
			t.Fatal("publication accepted a concurrently edited evidence version")
		}
	})
	t.Run("hidden_authors_keyset_and_cascade_counters", func(t *testing.T) {
		f := sbUsers(t, admin)
		posts := sbCorpus(t, f, 2, true)
		for _, author := range []string{f.author, f.stranger} {
			if _, err := f.rpc(f.viewer, "select public.set_recommendation_author_hidden($1,$2,true)", domain.UUID(), author); err != nil {
				t.Fatal(err)
			}
		}
		raw, err := f.rpc(f.viewer, "select public.get_recommendation_hidden_authors(null,1)")
		if err != nil {
			t.Fatal(err)
		}
		var page struct {
			IDs  []string `json:"author_ids"`
			Next string   `json:"next_cursor"`
		}
		if err := json.Unmarshal(raw, &page); err != nil || len(page.IDs) != 1 || page.Next == "" {
			t.Fatal("missing keyset cursor")
		}
		raw, err = f.rpc(f.viewer, "select public.get_recommendation_hidden_authors($1,1)", page.Next)
		if err != nil {
			t.Fatal(err)
		}
		var second struct {
			IDs []string `json:"author_ids"`
		}
		_ = json.Unmarshal(raw, &second)
		if len(second.IDs) != 1 || second.IDs[0] == page.IDs[0] {
			t.Fatal("hidden author pagination duplicated/skipped")
		}
		delivery := sbDelivery(t, f, f.viewer, posts[0])
		sbAction(t, f, f.viewer, domain.UUID(), delivery, "like")
		sbExec(t, admin, "delete from public.profiles where id=$1", f.viewer)
		var likes int
		if err := admin.QueryRow(ctx, "select likes from recsys.post_statistics where post_id=$1", posts[0]).Scan(&likes); err != nil || likes != 0 {
			t.Fatal("deleted user's like remained in public statistics")
		}
	})
	t.Run("parallel_refresh_claims", func(t *testing.T) {
		f := sbUsers(t, admin)
		sbCorpus(t, f, 20, false)
		for _, user := range []string{f.viewer, f.stranger} {
			sbRecommendPage(t, f, user, nil, -1, 20)
		}
		var group sync.WaitGroup
		errs := make(chan error, 8)
		for i := 0; i < 8; i++ {
			group.Add(1)
			go func() {
				defer group.Done()
				_, err := (application.Personalizer{Repository: repo}).Once(ctx)
				if err != nil {
					errs <- err
				}
			}()
		}
		group.Wait()
		close(errs)
		for err := range errs {
			t.Error(err)
		}
		var snapshots int
		if err := admin.QueryRow(ctx, "select count(*)from recsys.feed_snapshots where user_id=any($1::uuid[])and model='personalized-v1'", []string{f.viewer, f.stranger}).Scan(&snapshots); err != nil || snapshots != 2 {
			t.Fatalf("duplicate/lost refresh: %d %v", snapshots, err)
		}
	})
	t.Run("telemetry_and_stale_jobs", func(t *testing.T) { sbRecommendationTelemetry(t, admin, repo) })
	t.Run("unseen_retrieval_advances_beyond_the_initial_pool", func(t *testing.T) {
		f := sbUsers(t, admin)
		sbCorpus(t, f, 450, false)
		sbExec(t, admin, `insert into recsys.daily_exposures(user_id,post_id,day_utc,post_version,epoch)
 select $1,id,(statement_timestamp()at time zone 'UTC')::date,version,1 from feed.posts where author_id=$2 order by created_at desc,id limit 300`, f.viewer, f.author)
		sbRecommendPage(t, f, f.viewer, nil, -1, 20)
		sbRefresh(t, f, repo)
		page := sbRecommendPage(t, f, f.viewer, nil, -1, 30)
		if len(page.Items) != 30 {
			t.Fatal("unseen posts outside the first 300 were lost")
		}
		for _, item := range page.Items {
			var seen bool
			if err := admin.QueryRow(ctx, "select exists(select 1 from recsys.daily_exposures where user_id=$1 and post_id=$2)", f.viewer, item.Post.ID).Scan(&seen); err != nil || seen {
				t.Fatal("read post repeated while unseen inventory remained")
			}
		}
	})
	t.Run("unchanged_pool_is_reused_with_original_feature_time", func(t *testing.T) {
		f := sbUsers(t, admin)
		sbCorpus(t, f, 10, false)
		sbRecommendPage(t, f, f.viewer, nil, -1, 2)
		sbRefresh(t, f, repo)
		first := sbRecommendPage(t, f, f.viewer, nil, -1, 2)
		sbExec(t, admin, "update recsys.user_states set prepared_at=statement_timestamp()-interval '31 seconds'where user_id=$1", f.viewer)
		sbRecommendPage(t, f, f.viewer, nil, -1, 2)
		sbRefresh(t, f, repo)
		second := sbRecommendPage(t, f, f.viewer, nil, -1, 2)
		if second.Session != first.Session {
			t.Fatal("identical evidence/order created another 200-row snapshot")
		}
		page := sbRecommendPage(t, f, f.viewer, second.Next.Session, second.Next.After, 2)
		var correct bool
		if err := admin.QueryRow(ctx, "select d.features_at=s.features_at from recsys.deliveries d join recsys.feed_snapshots s on s.id=d.snapshot_id where d.id=$1", page.Items[0].Delivery).Scan(&correct); err != nil || !correct {
			t.Fatal("cached feature provenance was changed on delivery")
		}
	})
	t.Run("retention_drains_multiple_small_batches", func(t *testing.T) {
		f := sbUsers(t, admin)
		posts := sbCorpus(t, f, 2, false)
		sbExec(t, admin, `insert into recsys.feed_snapshots(user_id,epoch,revision,model,embedding_model,created_at,expires_at)
 select $1,1,1,'synthetic',$2,statement_timestamp()-interval '3 hours',statement_timestamp()-interval '2 hours'from generate_series(1,100)`, f.viewer, sbModel)
		sbExec(t, admin, `insert into recsys.deliveries(user_id,post_id,post_version,epoch,surface,model,embedding_model,delivered_at,valid_until)
 select $1,$2,1,1,'direct','synthetic',$3,statement_timestamp()-interval '31 days',statement_timestamp()-interval '30 days'from generate_series(1,300)`, f.viewer, posts[0], sbModel)
		sbExec(t, admin, `insert into recsys.telemetry_events(user_id,id,delivery_id,payload)select user_id,extensions.gen_random_uuid(),id,'{}'from recsys.deliveries where user_id=$1`, f.viewer)
		sbExec(t, admin, `insert into recsys.action_requests(user_id,id,payload,response,created_at)select $1,extensions.gen_random_uuid(),'[]','{}',statement_timestamp()-interval '31 days'from generate_series(1,1001)`, f.viewer)
		if err := repo.CleanupRecommendations(ctx); err != nil {
			t.Fatal(err)
		}
		var remaining int
		if err := admin.QueryRow(ctx, "select(select count(*)from recsys.feed_snapshots where user_id=$1)+(select count(*)from recsys.deliveries where user_id=$1)+(select count(*)from recsys.action_requests where user_id=$1)+(select count(*)from recsys.telemetry_events where user_id=$1)", f.viewer).Scan(&remaining); err != nil || remaining != 0 {
			t.Fatalf("expired backlog remains: %d %v", remaining, err)
		}
	})
	t.Run("exhausted_jobs_can_be_recovered_without_touching_active_leases", func(t *testing.T) {
		f := sbUsers(t, admin)
		posts := sbCorpus(t, f, 2, false)
		sbRecommendPage(t, f, f.viewer, nil, -1, 20)
		sbExec(t, admin, "update recsys.index_jobs set done_at=null,attempts=8,last_error='processing_failed'where post_id=$1", posts[0])
		lease := domain.UUID()
		sbExec(t, admin, "update recsys.index_jobs set done_at=null,attempts=8,lease=$2,locked_until=statement_timestamp()+interval '1 hour'where post_id=$1", posts[1], lease)
		sbExec(t, admin, "update recsys.refresh_jobs set attempts=8,last_error='processing_failed'where user_id=$1", f.viewer)
		count, err := repo.RetryFailed(ctx)
		if err != nil || count != 2 {
			t.Fatalf("recover jobs=%d err=%v", count, err)
		}
		var preserved bool
		if err := admin.QueryRow(ctx, "select lease=$2::uuid and attempts=8 from recsys.index_jobs where post_id=$1", posts[1], lease).Scan(&preserved); err != nil || !preserved {
			t.Fatal("maintenance reset an active lease")
		}
	})
	t.Run("500_active_users_load", func(t *testing.T) { sbRecommendationLoad(t, admin, index, repo) })
}

func sbRecommendationTelemetry(t *testing.T, admin *pgxpool.Pool, repo *postgres.RecommendationRepository) {
	f := sbUsers(t, admin)
	posts := sbCorpus(t, f, 4, false)
	delivery := sbDelivery(t, f, f.viewer, posts[0])
	ctx := context.Background()
	impression := map[string]any{"event_id": domain.UUID(), "delivery_id": delivery, "kind": "impression", "visible_ms": 1000, "visible_fraction": .8}
	call := func(user string, events ...map[string]any) (map[string]any, error) {
		encoded, _ := json.Marshal(events)
		raw, err := f.rpc(user, "select public.record_feed_events($1::text::jsonb)", string(encoded))
		if err != nil {
			return nil, err
		}
		var result map[string]any
		err = json.Unmarshal(raw, &result)
		return result, err
	}
	_, err := call(f.viewer, impression)
	sbErrorCode(t, err, "22023")
	sbExec(t, admin, "update recsys.deliveries set delivered_at=statement_timestamp()-interval '61 seconds'where id=$1", delivery)
	dwell := map[string]any{"event_id": domain.UUID(), "delivery_id": delivery, "kind": "dwell", "active_ms": 60000}
	if result, err := call(f.viewer, impression, dwell); err != nil || result["accepted"] != float64(2) {
		t.Fatalf("qualified events: %v", err)
	}
	if result, err := call(f.viewer, impression, dwell); err != nil || result["duplicates"] != float64(2) {
		t.Fatal("event retry not deduplicated")
	}
	// An invalid event rolls back earlier valid changes in the same batch.
	_, err = call(f.viewer, map[string]any{"event_id": domain.UUID(), "delivery_id": delivery, "kind": "open_post"},
		map[string]any{"event_id": domain.UUID(), "delivery_id": domain.UUID(), "kind": "impression", "visible_ms": 1000, "visible_fraction": 1})
	sbErrorCode(t, err, "22023")
	var opened bool
	if err := admin.QueryRow(ctx, "select opened from recsys.deliveries where id=$1", delivery).Scan(&opened); err != nil || opened {
		t.Fatal("invalid batch partially committed")
	}
	// An expired but owned delivery is stale, so it must not poison a batch of
	// otherwise valid queued events after an offline/reconnect interval.
	expired := sbDelivery(t, f, f.viewer, posts[2])
	fresh := sbDelivery(t, f, f.viewer, posts[1])
	sbExec(t, admin, "update recsys.deliveries set valid_until=statement_timestamp()-interval '1 second'where id=$1", expired)
	sbExec(t, admin, "update recsys.deliveries set delivered_at=statement_timestamp()-interval '2 seconds'where id=$1", fresh)
	if result, err := call(f.viewer, map[string]any{"event_id": domain.UUID(), "delivery_id": expired, "kind": "impression", "visible_ms": 1000, "visible_fraction": 1},
		map[string]any{"event_id": domain.UUID(), "delivery_id": fresh, "kind": "impression", "visible_ms": 1000, "visible_fraction": 1}); err != nil || result["accepted"] != float64(1) || result["stale"] != float64(1) {
		t.Fatalf("offline batch handling: %v", err)
	}
	_, err = call(f.stranger, impression)
	sbErrorCode(t, err, "22023")
	sbRefresh(t, f, repo)
	var weight float64
	var views int
	if err := admin.QueryRow(ctx, "select (select max(weight)from recsys.interest_clusters where user_id=$1),(select impressions from recsys.post_statistics where post_id=$2)", f.viewer, posts[0]).Scan(&weight, &views); err != nil || weight > 2.001 || views != 1 {
		t.Fatalf("feedback amplified by retries: %v", err)
	}
	job, found, err := repo.ClaimRefresh(ctx)
	if err != nil || found {
		t.Fatal("refresh left work behind")
	}
	// Claim a refresh, then reset while its old input is outside the transaction.
	_ = sbAction(t, f, f.viewer, domain.UUID(), delivery, "like")
	sbExec(t, admin, "update recsys.refresh_jobs set available_at=statement_timestamp()where user_id=$1", f.viewer)
	job, found, err = repo.ClaimRefresh(ctx)
	if err != nil || !found {
		t.Fatal("missing refresh job")
	}
	input, err := repo.LoadProfile(ctx, job)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.rpc(f.viewer, "select public.reset_recommendation_interests($1)", domain.UUID()); err != nil {
		t.Fatal(err)
	}
	accepted, err := repo.Publish(ctx, job, application.RecommendationResult{Input: input, Profile: ranking.BuildProfile(input.Evidence, input.At), FeatureVersion: application.PersonalizationVersion})
	if err != nil || accepted {
		t.Fatal("pre-reset job published a profile")
	}
	if result, err := call(f.viewer, map[string]any{"event_id": domain.UUID(), "delivery_id": delivery, "kind": "impression", "visible_ms": 1000, "visible_fraction": 1}); err != nil || result["stale"] != float64(1) {
		t.Fatal("pre-reset telemetry was learned")
	}
	sbRefresh(t, f, repo)
}
