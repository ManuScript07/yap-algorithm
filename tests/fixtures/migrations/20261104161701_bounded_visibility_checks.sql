begin;
-- Resolve account/block eligibility once per author and request rather than
-- performing several private lookups for every scanned publication.
create function recsys.eligible_authors()returns setof uuid language sql stable security definer set search_path='' as $$
 select p.id from public.profiles p where p.onboarding_completed
 and not private.is_account_globally_banned(p.id)
 and not private.is_account_pending_deletion(p.id)
 and not private.is_user_pair_blocked_impl(recsys.viewer(),p.id)
$$;
create function recsys.mutual_authors()returns setof uuid language sql stable security definer set search_path='' as $$
 select case when user_one_id=recsys.viewer()then user_two_id else user_one_id end
 from public.friendships where recsys.viewer()in(user_one_id,user_two_id)
$$;
revoke all on function recsys.eligible_authors(),recsys.mutual_authors()from public,anon,authenticated;
grant execute on function recsys.eligible_authors(),recsys.mutual_authors()to yap_recsys_api,yap_recsys_worker;
alter policy posts_read on feed.posts using(
 deleted_at is null and moderation_status='allowed'
 and(select recsys.account_allowed(recsys.viewer()))
 and author_id in(select recsys.eligible_authors())
 and(audience='public'or author_id=(select recsys.viewer())or author_id in(select recsys.mutual_authors()))
);
commit;
