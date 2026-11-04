begin;
create function recsys.delete_own_post(target uuid)returns boolean language plpgsql security definer set search_path='' as $$
declare changed integer;
begin
 if not recsys.account_allowed(recsys.viewer())then return false;end if;
 update feed.posts set deleted_at=now(),version=version+1 where id=target and author_id=recsys.viewer()and deleted_at is null;
 get diagnostics changed=row_count;return changed=1;
end$$;
revoke all on function recsys.delete_own_post(uuid)from public,anon,authenticated;
grant execute on function recsys.delete_own_post(uuid)to yap_recsys_api;
revoke update on feed.posts from yap_recsys_api;
grant update(text,version,updated_at)on feed.posts to yap_recsys_api;
commit;
