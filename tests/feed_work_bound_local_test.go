package tests

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func runFeedWorkSQL(t *testing.T, sql string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	endpoint, err := exec.CommandContext(ctx, "docker", "context", "inspect", "--format", "{{.Endpoints.docker.Host}}").Output()
	if err != nil || (!strings.HasPrefix(string(endpoint), "npipe:") && !strings.HasPrefix(string(endpoint), "unix:")) {
		t.Fatal("local Docker socket required")
	}

	command := exec.CommandContext(ctx, "docker", "exec", "-i", localSupabaseTarget(t).container, "psql", "-U", "supabase_admin", "-d", "postgres", "-v", "ON_ERROR_STOP=1", "-q")
	command.Stdin = strings.NewReader(sql)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("bounded SQL work regression: %v %s", err, output)
	}
	t.Logf("%s", output)
}

// Permission work must depend on page size even under adverse cached plans.
func TestSupabaseLocalFeedWorkBound(t *testing.T) {
	stage5Pool(t)
	runFeedWorkSQL(t, `begin;
set local statement_timeout='60s';
set local track_functions='pl';
insert into auth.users(id,is_sso_user,is_anonymous,raw_user_meta_data)values
 ('62000000-0000-4000-8000-000000000001',false,false,'{"name":"Work bound fixture"}'),
 ('62000000-0000-4000-8000-000000000002',false,false,'{"name":"Work bound fixture"}');
insert into feed.posts(id,author_id,text,audience,language,creation_request,latitude,longitude)
 select extensions.gen_random_uuid(),'62000000-0000-4000-8000-000000000002','Bounded permission work','public','en','[]',55,37 from generate_series(1,2000);
insert into public.user_locations(user_id,latitude,longitude,updated_at)values('62000000-0000-4000-8000-000000000001',55,37,statement_timestamp());
insert into feed.author_follows(follower_id,author_id,following,version)values('62000000-0000-4000-8000-000000000001','62000000-0000-4000-8000-000000000002',true,1);
select set_config('request.jwt.claim.sub','62000000-0000-4000-8000-000000000001',true);
select set_config('yap_test.feed_access_oid','private.feed_post_accessible(uuid,feed.posts,boolean)'::regprocedure::oid::text,true);
set local role authenticated;
-- Prepare the bounded fallback before measuring reads of its issued pool.
select jsonb_array_length(public.get_for_you_feed(null,-1,10)->'items');
reset role;
set local plan_cache_mode='force_generic_plan';
set local enable_indexscan=off;
set local enable_indexonlyscan=off;
set local enable_bitmapscan=off;
set local enable_hashjoin=off;
set local enable_mergejoin=off;
set local role authenticated;
do $$declare prior bigint;current_calls bigint;page jsonb;name text;begin
 foreach name in array array['get_nearby_feed','get_friends_feed','get_for_you_feed']loop
  select coalesce((select calls from pg_stat_xact_user_functions where funcid=current_setting('yap_test.feed_access_oid')::oid),0)into prior;
  execute format('select public.%I(null,-1,10)',name)into page;
  select coalesce((select calls from pg_stat_xact_user_functions where funcid=current_setting('yap_test.feed_access_oid')::oid),0)into current_calls;
  if jsonb_array_length(page->'items')<>10 then raise exception 'Unfilled fixture page: %',name;end if;
  if current_calls-prior>100 then raise exception 'Permission work depends on corpus size: % checks=%',name,current_calls-prior;end if;
  raise notice '%: % permission checks for 10 items against 2000 posts',name,current_calls-prior;
 end loop;
end $$;
reset role;
rollback;`)
}

func TestSupabaseLocalFollowAuthorWork(t *testing.T) {
	stage5Pool(t)
	root := os.Getenv("YAP_SUPABASE_APP_ROOT")
	if !filepath.IsAbs(root) {
		t.Fatal("explicit absolute Supabase AppRoot required; use test-supabase-local.ps1")
	}
	source, err := os.ReadFile(filepath.Join(root, "supabase", "tests", "feed_follow_author_work_bound.sql"))
	if err != nil {
		t.Fatal(err)
	}
	runFeedWorkSQL(t, string(source))
}
