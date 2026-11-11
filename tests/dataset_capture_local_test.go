package tests

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"yap-algorithm/internal/dataset"
	"yap-algorithm/internal/domain"
)

func datasetScope(t *testing.T, p *pgxpool.Pool, ids ...string) (string, *pgx.ConnConfig) {
	t.Helper()
	cohort := domain.UUID()
	sbExec(t, p, "insert into recsys_dataset.cohorts(id,origin,seed,source_hash) values($1,'synthetic',42,repeat('a',64))", cohort)
	sbExec(t, p, "insert into recsys_dataset.members(cohort_id,user_id) select $1,unnest($2::uuid[])", cohort, ids)
	t.Cleanup(func() { sbExec(t, p, "delete from recsys_dataset.cohorts where id=$1", cohort) })

	role := "dataset_test_" + strings.ReplaceAll(domain.UUID(), "-", "")
	secret := domain.UUID()
	// The role name and password are generated locally, with a fixed alphabet.
	sbExec(t, p, "create role "+role+" login inherit password '"+secret+"'")
	t.Cleanup(func() { sbExec(t, p, "drop role "+role) })
	sbExec(t, p, "grant yap_recommendation_exporter to "+role)
	cfg := p.Config().ConnConfig
	cfg.User, cfg.Password = role, secret
	return cohort, cfg
}

func TestSupabaseLocalDatasetCapture(t *testing.T) {
	p := stage5Pool(t)
	ctx, stop := context.WithTimeout(context.Background(), 35*time.Second)
	defer stop()
	f := sbUsers(t, p)
	cohort, cfg := datasetScope(t, p, f.viewer, f.author)
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal("restricted exporter could not connect")
	}
	defer conn.Close(context.Background())
	for _, query := range []string{
		"select * from recsys.feedback_facts",
		"select * from recsys_dataset.members",
		"select * from feed.posts",
		"select recsys_dataset.capture_scope(null)",
	} {
		_, err := conn.Exec(ctx, query)
		sbErrorCode(t, err, "42501")
	}
	for _, role := range []string{"anon", "authenticated", "service_role", "yap_recommendation_worker", "yap_feed_media_worker"} {
		var allowed bool
		if err := p.QueryRow(ctx, "select has_schema_privilege($1,'recsys_dataset','USAGE') or has_function_privilege($1,'recsys_dataset.capture_page(uuid,bigint,integer)','EXECUTE')", role).Scan(&allowed); err != nil || allowed {
			t.Fatalf("export granted to %s: %v", role, err)
		}
	}
	_, err = conn.Exec(ctx, "select recsys_dataset.capture_info($1::uuid)", cohort)
	sbErrorCode(t, err, "25006")

	publish := func(author, message string) string {
		id := domain.UUID()
		stage5RPC(t, author, "create_feed_post", map[string]any{"requested_id": id, "requested_text": message,
			"requested_audience": "public", "requested_language": "en"}, 200)
		return id
	}
	held, err := p.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Rollback(context.Background())
	if _, err = held.Exec(ctx, "select set_config('request.jwt.claim.sub',$1,true)", f.author); err != nil {
		t.Fatal(err)
	}
	if _, err = held.Exec(ctx, "set local role authenticated"); err != nil {
		t.Fatal(err)
	}
	late := domain.UUID()
	if _, err = held.Exec(ctx, "select public.create_feed_post($1,'lower sequence commits last','public','en')", late); err != nil {
		t.Fatal(err)
	}
	// Use a different author to avoid waiting on the publication quota row.
	high := publish(f.viewer, "higher sequence commits first")
	outside := publish(f.stranger, "must never leave the source")
	direct := stage5RPC(t, f.author, "get_feed_post", map[string]string{"requested_id": high}, 200)
	external := stage5RPC(t, f.author, "get_feed_post", map[string]string{"requested_id": outside}, 200)
	time.Sleep(1100 * time.Millisecond)
	event := func(page map[string]any) map[string]any {
		return map[string]any{"event_id": domain.UUID(), "delivery_id": page["delivery_id"], "kind": "impression",
			"visible_ms": 1000, "visible_fraction": 0.8}
	}
	stage5RPC(t, f.author, "record_feed_events", map[string]any{"requested_events": []any{event(direct), event(external)}}, 200)

	root := t.TempDir()
	capture := func(name string, pageSize, maxRows int) (dataset.Manifest, string, error) {
		t.Helper()
		path := filepath.Join(root, name)
		m, err := dataset.Capture(ctx, conn, dataset.Options{Cohort: cohort, Output: path,
			PageSize: pageSize, MaxRows: maxRows, Timeout: 10 * time.Second, BinaryHash: strings.Repeat("f", 64)})
		if err != nil {
			return m, "", err
		}
		raw, err := os.ReadFile(filepath.Join(path, "facts.jsonl"))
		return m, string(raw), err
	}
	first, facts, err := capture("held", 1, 100)
	if err != nil {
		t.Fatal(err)
	}
	var db struct {
		Settled *time.Time `json:"settled_through"`
		Started time.Time  `json:"snapshot_started_at"`
		Active  int        `json:"active_writer_count"`
	}
	if err := json.Unmarshal(first.Database, &db); err != nil || db.Settled == nil || !db.Settled.Before(db.Started) || db.Active < 1 {
		t.Fatalf("held writer did not fence the capture: %s (%v)", first.Database, err)
	}
	if strings.Contains(facts, late) || strings.Contains(facts, outside) || strings.Contains(facts, "higher sequence commits first") ||
		!strings.Contains(facts, high) || !strings.Contains(facts, `"text_sha256"`) || first.Excluded < 1 {
		t.Fatalf("incorrect scoped projection: %d rows %d excluded", first.Rows, first.Excluded)
	}
	frozen, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		t.Fatal(err)
	}
	defer frozen.Rollback(context.Background())
	var firstSeq int64
	if err := frozen.QueryRow(ctx, "select sequence from recsys_dataset.capture_page($1::uuid,0,1)", cohort).Scan(&firstSeq); err != nil {
		t.Fatal(err)
	}
	if err := held.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	var again int64
	if err := frozen.QueryRow(ctx, "select sequence from recsys_dataset.capture_page($1::uuid,0,1)", cohort).Scan(&again); err != nil || again != firstSeq {
		t.Fatalf("paging changed inside the established snapshot: %d -> %d (%v)", firstSeq, again, err)
	}
	if err := frozen.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	second, facts, err := capture("committed", 3, 100)
	if err != nil || !strings.Contains(facts, late) || second.Rows != first.Rows+1 {
		t.Fatalf("capture after out-of-order commit: rows=%d error=%v", second.Rows, err)
	}
	if _, _, err := capture("over-budget", 1, 1); err == nil {
		t.Fatal("row budget was ignored")
	}
	if _, err := os.Stat(filepath.Join(root, "over-budget")); !os.IsNotExist(err) {
		t.Fatal("incomplete capture was published")
	}
	// Blocking affects the export even when the impression was valid at delivery.
	sbExec(t, p, "insert into public.user_blocks(blocker_user_id,blocked_user_id)values($1,$2)", f.author, f.viewer)
	_, facts, err = capture("blocked", 500, 100)
	if err != nil || strings.Contains(facts, `"text_sha256"`) {
		t.Fatalf("blocked content still available: %v", err)
	}
	sbExec(t, p, "delete from public.user_blocks where blocker_user_id=$1 and blocked_user_id=$2", f.author, f.viewer)
	stage5RPC(t, f.author, "apply_feed_action", map[string]any{"requested_id": domain.UUID(),
		"requested_delivery": direct["delivery_id"], "requested_action": "less_similar", "expected_version": 0}, 200)
	_, facts, err = capture("preference-feedback", 500, 100)
	if err != nil || !strings.Contains(facts, `"text_sha256"`) || !strings.Contains(facts, `"less_similar"`) {
		t.Fatalf("negative feedback erased the eligible exposure: %v", err)
	}
	// The latest content version is authoritative, even though the receipt was valid at delivery.
	stage5RPC(t, f.viewer, "edit_feed_post", map[string]any{"requested_id": high, "expected_version": 1, "requested_text": "different version"}, 200)
	_, facts, err = capture("after-edit", 500, 100)
	if err != nil || strings.Contains(facts, `"text_sha256"`) {
		t.Fatalf("old post version still available: %v", err)
	}

	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	_, err = tx.Exec(ctx, "select * from recsys_dataset.capture_page($1::uuid,0,501)", cohort)
	sbErrorCode(t, err, "22023")
}
