package tests

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"yap-algorithm/internal/adapters/postgres"
	"yap-algorithm/internal/application"
	"yap-algorithm/internal/domain"
	"yap-algorithm/internal/model"
)

func topicIntent(id string, epoch int, version int64, topics []string) map[string]any {
	return map[string]any{"requested_id": id, "expected_epoch": epoch, "expected_version": version,
		"requested_catalog_version": 1, "requested_topics": topics}
}

func TestSupabaseLocalTopicsContract(t *testing.T) {
	p := stage5Pool(t)
	ctx := context.Background()
	f := sbUsers(t, p)
	var exposed bool
	if err := p.QueryRow(ctx, `select has_function_privilege('anon','public.get_recommendation_topics(text)','execute')
		or has_function_privilege('yap_recommendation_worker','public.set_recommendation_topics(uuid,integer,bigint,integer,text[])','execute')
		or has_table_privilege('authenticated','recsys.user_topic_choices','select')
		or has_table_privilege('service_role','recsys.interest_topics','select')`).Scan(&exposed); err != nil || exposed {
		t.Fatalf("topic tables or RPCs were over-granted: %v", err)
	}
	get := func(user, language string) map[string]any {
		t.Helper()
		return stage5RPC(t, user, "get_recommendation_topics", map[string]string{"requested_language": language}, 200)
	}
	catalog := get(f.viewer, "ru")
	first := catalog["topics"].([]any)[0].(map[string]any)
	if catalog["catalog_version"] != float64(1) || catalog["max_selected"] != float64(4) ||
		len(catalog["topics"].([]any)) != 12 || first["label"] != "Животные" || len(first) != 2 {
		t.Fatal("unexpected catalog response")
	}
	if get(f.viewer, "en")["topics"].([]any)[0].(map[string]any)["label"] != "Animals" {
		t.Fatal("localized label missing")
	}
	if catalog["selection"].(map[string]any)["status"] != "not_set" {
		t.Fatal("catalog read wrote a selection")
	}
	set := func(body map[string]any, status int) map[string]any {
		t.Helper()
		return stage5RPC(t, f.viewer, "set_recommendation_topics", body, status)
	}
	skip := set(topicIntent(domain.UUID(), 1, 0, []string{}), 200)
	if skip["status"] != "skipped" || skip["version"] != float64(1) {
		t.Fatal("empty selection is not a valid explicit skip")
	}
	var jobs int
	if err := p.QueryRow(ctx, "select count(*) from recsys.refresh_jobs where user_id=$1", f.viewer).Scan(&jobs); err != nil || jobs != 0 {
		t.Fatal("skipping topics scheduled unnecessary work", err)
	}
	id := domain.UUID()
	body := topicIntent(id, 1, 1, []string{"music", "animals"})
	selected := set(body, 200)
	if selected["version"] != float64(2) || selected["status"] != "selected" || selected["topics"].([]any)[0] != "animals" {
		t.Fatal("selection was not normalized")
	}
	if set(topicIntent(id, 1, 1, []string{"animals", "music"}), 200)["replayed"] != true {
		t.Fatal("equivalent retry was not idempotent")
	}
	if set(topicIntent(id, 1, 1, []string{"games"}), 409)["message"] != "topic_request_conflict" {
		t.Fatal("a UUID was allowed to represent a different intent")
	}
	if set(topicIntent(domain.UUID(), 1, 1, []string{}), 409)["message"] != "topic_version_conflict" {
		t.Fatal("stale intent overwrote a newer choice")
	}
	for _, ids := range [][]string{{"unknown"}, {"animals", "animals"}, {"a", "b", "c", "d", "e"}} {
		set(topicIntent(domain.UUID(), 1, 2, ids), 400)
	}
	staleCatalog := topicIntent(domain.UUID(), 1, 2, []string{"animals"})
	staleCatalog["requested_catalog_version"] = 2
	if set(staleCatalog, 409)["message"] != "topic_catalog_conflict" {
		t.Fatal("stale catalog was accepted")
	}
	if get(f.stranger, "ru")["selection"].(map[string]any)["status"] != "not_set" {
		t.Fatal("viewer preference leaked to another account")
	}

	// Separate HTTP requests and database transactions contend on the same
	// expected version. Exactly one write can advance that version.
	token := localMediaJWT(t, f.viewer)
	client := soakHTTP{os.Getenv("YAP_SUPABASE_LOCAL_URL"), os.Getenv("YAP_SUPABASE_LOCAL_ANON_KEY"), &http.Client{Timeout: 10 * time.Second}}
	codes := make(chan int, 2)
	for _, topic := range []string{"games", "food"} {
		go func(topic string) {
			_, status, err := client.rpc(token, "set_recommendation_topics", topicIntent(domain.UUID(), 1, 2, []string{topic}))
			if err != nil {
				status = 0
			}
			codes <- status
		}(topic)
	}
	got := []int{<-codes, <-codes}
	slices.Sort(got)
	if !slices.Equal(got, []int{200, 409}) {
		t.Fatalf("out-of-order writes did not serialize: %v", got)
	}
	if set(topicIntent(domain.UUID(), 1, 3, []string{}), 200)["status"] != "skipped" {
		t.Fatal("clearing topics did not restore skip")
	}
	set(topicIntent(domain.UUID(), 1, 4, []string{"animals"}), 200)
	stage5RPC(t, f.viewer, "set_feed_author_followed", map[string]any{
		"requested_id": domain.UUID(), "requested_author": f.author, "requested_following": true, "expected_version": 0,
	}, 200)
	stage5RPC(t, f.viewer, "set_recommendation_author_hidden", map[string]any{
		"requested_id": domain.UUID(), "requested_author": f.author, "requested_hidden": true,
	}, 200)
	sbExec(t, p, "insert into public.user_blocks(blocker_user_id,blocked_user_id) values($1,$2)", f.viewer, f.stranger)
	sbExec(t, p, "insert into public.friendships(user_one_id,user_two_id) values(least($1::uuid,$2::uuid),greatest($1::uuid,$2::uuid))", f.viewer, f.author)
	stage5RPC(t, f.viewer, "reset_recommendation_interests", map[string]any{"requested_id": domain.UUID()}, 200)
	state := get(f.viewer, "en")["selection"].(map[string]any)
	if state["epoch"] != float64(2) || state["version"] != float64(0) || state["status"] != "not_set" {
		t.Fatal("reset did not clear choices and versions")
	}
	if set(body, 409)["message"] != "topic_epoch_conflict" {
		t.Fatal("previous epoch retry recreated topics")
	}
	var receipts int
	if err := p.QueryRow(ctx, "select count(*) from recsys.topic_write_receipts where user_id=$1", f.viewer).Scan(&receipts); err != nil || receipts != 0 {
		t.Fatal("reset retained topic receipts", err)
	}
	var preserved bool
	if err := p.QueryRow(ctx, `select
	    exists(select 1 from feed.author_follows where follower_id=$1 and author_id=$2 and following)
	    and exists(select 1 from recsys.hidden_authors where user_id=$1 and author_id=$2)
	    and exists(select 1 from public.user_blocks where blocker_user_id=$1 and blocked_user_id=$3)
	    and exists(select 1 from public.friendships where user_one_id=least($1::uuid,$2::uuid)
	      and user_two_id=greatest($1::uuid,$2::uuid))`, f.viewer, f.author, f.stranger).Scan(&preserved); err != nil || !preserved {
		t.Fatal("reset changed a durable social or privacy relationship", err)
	}

	assertQuota := func(token, rpc string, args any) {
		t.Helper()
		raw, status, err := client.rpc(token, rpc, args)
		if err != nil || status < 400 || !strings.Contains(string(raw), "42901") {
			t.Fatalf("quota not enforced: %s status=%d err=%v body=%s", rpc, status, err, raw)
		}
	}
	sbExec(t, p, "update private.feed_social_limits set requests=60 where user_id=$1 and operation='topics_read'", f.viewer)
	assertQuota(token, "get_recommendation_topics", map[string]string{"requested_language": "ru"})
	sbExec(t, p, "update private.feed_social_limits set requests=12 where user_id=$1 and operation='topics_write'", f.viewer)
	assertQuota(token, "set_recommendation_topics", topicIntent(domain.UUID(), 2, 0, []string{"food"}))
	sbExec(t, p, "insert into private.global_account_bans(target_user_id,note) values($1,'topic contract fixture')", f.stranger)
	deniedRead := stage5RPC(t, f.stranger, "get_recommendation_topics", map[string]string{"requested_language": "ru"}, 400)
	deniedWrite := stage5RPC(t, f.stranger, "set_recommendation_topics", topicIntent(domain.UUID(), 1, 0, []string{"animals"}), 400)
	if deniedRead["message"] != "account_globally_banned" || deniedWrite["message"] != "account_globally_banned" {
		t.Fatal("topic RPCs did not preserve account restrictions")
	}
}

func topicPreparedPage(t *testing.T, p *pgxpool.Pool, repo *postgres.RecommendationRepository, viewer, policy string) map[string]any {
	t.Helper()
	ctx := context.Background()
	worker := application.Personalizer{Repository: repo}
	sbExec(t, p, "update recsys.refresh_jobs set available_at=statement_timestamp() where user_id=$1", viewer)
	for i := 0; i < 40; i++ {
		worked, err := worker.Once(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var ready bool
		err = p.QueryRow(ctx, `select exists(select 1 from recsys.user_states u join recsys.feed_snapshots s
		    on s.id=u.current_snapshot where u.user_id=$1 and u.processed_revision=u.revision and s.policy_version=$2)`, viewer, policy).Scan(&ready)
		if err != nil {
			t.Fatal(err)
		}
		if ready {
			return stage5RPC(t, viewer, "get_for_you_feed", map[string]int{"requested_limit": 30}, 200)
		}
		if !worked {
			break
		}
	}
	t.Fatalf("worker did not publish %s", policy)
	return nil
}

func TestSupabaseLocalTopicColdStart(t *testing.T) {
	p := stage5Pool(t)
	if os.Getenv("YAP_SUPABASE_MODEL_TEST") != "1" {
		t.Skip("enable -WithModel to exercise the actual E5 model")
	}
	ctx := context.Background()
	f := sbUsers(t, p)
	repo, rec := stage5Worker(t)
	categories := [][]string{
		{"Мой котёнок сегодня уснул рядом с собакой. Люблю домашних животных.",
			"Кошки, собаки, корм и уход за домашними питомцами.",
			"My dog and cat have become friends. Caring for our pets is fun.",
			"Photos and stories about puppies, kittens, and adopting pets."},
		{"Новая видеоигра, настольные игры и шахматный турнир.",
			"Обсуждаем видеоигры, шахматы и стратегии.",
			"Board games, chess, puzzles and the weekend gaming tournament.",
			"Video games, gaming sessions and puzzle game reviews."},
	}
	var ids []string
	animals := map[string]bool{}
	for category, texts := range categories {
		for i, text := range texts {
			author := f.author
			if i%2 == 1 {
				author = f.stranger
			}
			post := domain.UUID()
			stage5RPC(t, author, "create_feed_post", map[string]any{
				"requested_id": post, "requested_text": text, "requested_audience": "public",
				"requested_language": []string{"ru", "ru", "en", "en"}[i],
			}, 200)
			ids = append(ids, post)
			if category == 0 {
				animals[post] = true
				sbExec(t, p, "update feed.posts set created_at=statement_timestamp()-interval '16 hours' where id=$1", post)
			}
		}
	}
	indexer := application.BatchIndexer{Repository: repo, Encoder: model.New("http://127.0.0.1:8091", sbModel), Kind: "live", Size: 8}
	indexed := 0
	for i := 0; i < 40; i++ {
		if _, err := indexer.Once(ctx); err != nil {
			t.Fatal(err)
		}
		if err := p.QueryRow(ctx, "select count(*) from recsys.post_embeddings where post_id=any($1::uuid[]) and model=$2", ids, sbModel).Scan(&indexed); err != nil {
			t.Fatal(err)
		}
		if indexed == len(ids) {
			break
		}
	}
	if indexed != len(ids) {
		t.Fatal("test posts were not indexed by E5")
	}
	stage5RPC(t, f.viewer, "get_for_you_feed", map[string]int{"requested_limit": 30}, 200)
	baseline := topicPreparedPage(t, p, rec, f.viewer, "hybrid-v2")
	claim := func() application.RefreshJob {
		t.Helper()
		job, found, err := rec.ClaimRefresh(ctx)
		if err != nil || !found || job.UserID != f.viewer {
			t.Fatalf("missing refresh lease for the fixture viewer: %v", err)
		}
		return job
	}
	sbExec(t, p, "select private.queue_recommendation_refresh($1,revision,true) from recsys.user_states where user_id=$1", f.viewer)
	preChoice := claim()
	plain, err := rec.LoadProfile(ctx, preChoice)
	if err != nil || len(plain.Topics.Vectors) != 0 {
		t.Fatalf("expected a leased build with no topics: %v", err)
	}
	resultFor := func(input application.ProfileInput) application.RecommendationResult {
		return application.RecommendationResult{Input: input, FeatureVersion: application.FeatureVersion, PolicyVersion: application.PolicyVersion}
	}
	stage5RPC(t, f.viewer, "set_recommendation_topics", topicIntent(domain.UUID(), 1, 0, []string{"animals"}), 200)
	if accepted, err := rec.Publish(ctx, preChoice, resultFor(plain)); err != nil || accepted {
		t.Fatalf("a pre-choice build published after selection: %v", err)
	}
	replay := stage5RPC(t, f.viewer, "get_for_you_feed", map[string]any{
		"requested_session": baseline["session_id"], "after_position": -1, "requested_limit": 30,
	}, 200)
	oldItems, _ := json.Marshal(baseline["items"])
	replayedItems, _ := json.Marshal(replay["items"])
	if string(oldItems) != string(replayedItems) {
		t.Fatal("topic selection reordered an already opened feed session")
	}
	withTopics := topicPreparedPage(t, p, rec, f.viewer, "hybrid-v2-topics-v1")
	topHits := func(page map[string]any) int {
		items := page["items"].([]any)
		hits, tested := 0, 0
		for _, item := range items {
			raw, _ := json.Marshal(item)
			for _, id := range ids {
				if strings.Contains(string(raw), id) {
					tested++
					if animals[id] {
						hits++
					}
					break
				}
			}
			if tested == 3 {
				return hits
			}
		}
		t.Fatal("page omitted the controlled corpus")
		return 0
	}
	before, after := topHits(baseline), topHits(withTopics)
	t.Logf("cold-start animals hits@3 among controlled posts: without=%d with=%d", before, after)
	if after <= before || after < 2 {
		t.Fatal("chosen topics did not improve the expected cold-start ranking")
	}
	var clusters int
	if err := p.QueryRow(ctx, "select count(*) from recsys.interest_clusters where user_id=$1", f.viewer).Scan(&clusters); err != nil || clusters != 0 {
		t.Fatal("explicit topics became learned feedback", err)
	}
	for _, id := range ids[4:] {
		delivery := stage5RPC(t, f.viewer, "get_feed_post", map[string]string{"requested_id": id}, 200)
		stage5RPC(t, f.viewer, "apply_feed_action", map[string]any{"requested_id": domain.UUID(),
			"requested_delivery": delivery["delivery_id"], "requested_action": "like", "expected_version": 0}, 200)
	}
	afterFeedback := topicPreparedPage(t, p, rec, f.viewer, "hybrid-v2-topics-v1")
	t.Logf("animals hits@3 after four likes on games: %d", topHits(afterFeedback))
	if topHits(afterFeedback) >= after {
		t.Fatal("explicit topics outweighed the actual shift in interest")
	}

	stage5RPC(t, f.viewer, "set_recommendation_topics", topicIntent(domain.UUID(), 1, 1, []string{"music"}), 200)
	job := claim()
	input, err := rec.LoadProfile(ctx, job)
	if err != nil || input.Topics.Version != 2 {
		t.Fatalf("missing prior for leased revision: %v", err)
	}
	input.Topics.Confidence = .7
	mismatched := input
	mismatched.Topics.Version--
	_, err = rec.Publish(ctx, job, resultFor(mismatched))
	sbErrorCode(t, err, "40001")
	stage5RPC(t, f.viewer, "set_recommendation_topics", topicIntent(domain.UUID(), 1, 2, []string{}), 200)
	if accepted, err := rec.Publish(ctx, job, resultFor(input)); err != nil || accepted {
		t.Fatalf("a pre-clear build published after clearing topics: %v", err)
	}
	resetJob := claim()
	resetInput, err := rec.LoadProfile(ctx, resetJob)
	if err != nil {
		t.Fatal(err)
	}
	stage5RPC(t, f.viewer, "reset_recommendation_interests", map[string]string{"requested_id": domain.UUID()}, 200)
	if accepted, err := rec.Publish(ctx, resetJob, resultFor(resetInput)); err != nil || accepted {
		t.Fatalf("a lease from the old epoch published after reset: %v", err)
	}
	page := stage5RPC(t, f.viewer, "get_for_you_feed", map[string]int{"requested_limit": 30}, 200)
	if len(page["items"].([]any)) == 0 || page["versions"].(map[string]any)["policy"] == "hybrid-v2-topics-v1" {
		t.Fatal("reset lost the ordinary fallback")
	}
}
