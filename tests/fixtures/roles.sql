do $$begin
 if not exists(select 1 from pg_roles where rolname='yap_local_api')then create role yap_local_api login password 'yap-local-api' in role yap_recsys_api;end if;
 if not exists(select 1 from pg_roles where rolname='yap_local_worker')then create role yap_local_worker login password 'yap-local-worker' in role yap_recsys_worker;end if;
end$$;
-- The development auth adapter refuses databases without this marker function.
create or replace function recsys.is_fixture()returns boolean language sql stable security definer set search_path='' as $$select exists(select 1 from private.yap_algorithm_fixture)$$;
revoke all on function recsys.is_fixture()from public,anon,authenticated;
grant execute on function recsys.is_fixture()to yap_recsys_api,yap_recsys_worker;
