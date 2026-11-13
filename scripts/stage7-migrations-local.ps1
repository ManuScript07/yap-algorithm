param([Parameter(Mandatory = $true)][string]$AppRoot)
$ErrorActionPreference = 'Stop'
$serviceRoot = Split-Path $PSScriptRoot -Parent
$AppRoot = [IO.Path]::GetFullPath($AppRoot)
. (Join-Path $PSScriptRoot 'local-supabase-target.ps1')
$target = Get-LocalSupabaseTarget $serviceRoot $AppRoot $true
$cli = Join-Path $AppRoot 'node_modules/.bin/supabase.cmd'
$settings = & $cli --workdir $target.Workdir status -o json 2>$null | ConvertFrom-Json
if ($LASTEXITCODE -ne 0 -or $settings.API_URL -ne $target.Api -or !(Test-LocalSupabaseDatabase $settings.DB_URL $target)) {
    throw 'Migration rehearsal requires the isolated stage 7 stack.'
}

$canonical = Get-ChildItem -LiteralPath (Join-Path $AppRoot 'supabase/migrations') -File -Filter '*.sql'
$staged = Get-ChildItem -LiteralPath (Join-Path $target.Workdir 'supabase/migrations') -File -Filter '*.sql'
if (Compare-Object $canonical.Name $staged.Name) { throw 'The isolated migration files differ from YapChat.' }
foreach ($migration in $canonical) {
    $copy = Join-Path $target.Workdir ("supabase/migrations/" + $migration.Name)
    if ((Get-FileHash -LiteralPath $migration.FullName).Hash -cne (Get-FileHash -LiteralPath $copy).Hash) {
        throw "The isolated copy of $($migration.Name) is stale."
    }
}
$previous = '20261112150000'
$continuation = '20261112160000'
function Invoke-IsolatedSql([string]$Sql) {
    $Sql | docker exec -i $target.Container psql -X -qAt -v ON_ERROR_STOP=1 -U postgres -d postgres
    if ($LASTEXITCODE -ne 0) { throw 'The isolated SQL verification failed.' }
}

$applied = Invoke-IsolatedSql "select count(*) from supabase_migrations.schema_migrations"
if ([int]$applied -ne $canonical.Count) { throw 'The clean start did not apply every canonical migration.' }
Invoke-IsolatedSql "select 1 / (to_regprocedure('public.get_for_you_feed_v2(uuid,uuid,integer)') is not null)::int"
Write-Output "Clean installation verified with $applied canonical migrations."

& $cli --workdir $target.Workdir db reset --local --version $previous --no-seed
if ($LASTEXITCODE -ne 0) { throw 'Could not build the previous schema in the isolated stack.' }
Invoke-IsolatedSql @'
begin;
select 1 / (to_regprocedure('public.get_for_you_feed_v2(uuid,uuid,integer)') is null)::int;
insert into auth.users(id,is_sso_user,is_anonymous,raw_user_meta_data) values
  ('53e70000-0000-4000-8000-000000000001',false,false,'{"name":"Migration reader"}'),
  ('53e70000-0000-4000-8000-000000000002',false,false,'{"name":"Migration author"}');
insert into feed.posts(id,author_id,text,audience,language,creation_request) values
  ('53e70000-0000-4000-8000-000000000003','53e70000-0000-4000-8000-000000000002','Migration sample','public','en','[]');
select set_config('request.jwt.claim.sub','53e70000-0000-4000-8000-000000000001',true);
set local role authenticated;
select 1 / coalesce((public.get_for_you_feed(requested_limit=>10)#>>'{items,0,post,id}'='53e70000-0000-4000-8000-000000000003')::int,0);
commit;
'@

& $cli --workdir $target.Workdir migration up --local
if ($LASTEXITCODE -ne 0) { throw 'Could not apply the pending migration to the populated previous schema.' }
Invoke-IsolatedSql @"
select 1 / (exists (select 1 from supabase_migrations.schema_migrations where version='$continuation'))::int;
"@
Invoke-IsolatedSql @'
begin;
select snapshot_id as old_session, id as old_delivery from recsys.deliveries
 where user_id='53e70000-0000-4000-8000-000000000001' and post_id='53e70000-0000-4000-8000-000000000003' \gset
select set_config('request.jwt.claim.sub','53e70000-0000-4000-8000-000000000001',true);
set local role authenticated;
select 1 / coalesce((public.get_for_you_feed(requested_session=>:'old_session',requested_limit=>10)#>>'{items,0,delivery_id}'=:'old_delivery')::int,0);
select 1 / coalesce((public.get_for_you_feed_v2('53e70000-0000-4000-8000-000000000004',null,10)#>>'{items,0,post,id}'='53e70000-0000-4000-8000-000000000003')::int,0);
rollback;
delete from public.profiles where id in ('53e70000-0000-4000-8000-000000000001','53e70000-0000-4000-8000-000000000002');
delete from auth.users where id in ('53e70000-0000-4000-8000-000000000001','53e70000-0000-4000-8000-000000000002');
'@
Write-Output 'Upgrade verified with a persisted legacy session and delivery, and the new cursor RPC.'
