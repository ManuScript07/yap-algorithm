begin;
create function recsys.unblocked(a uuid,b uuid)returns boolean language sql stable security definer set search_path='' as $$select not private.is_user_pair_blocked_impl(a,b)$$;
revoke all on function recsys.unblocked(uuid,uuid)from public,anon,authenticated;
grant execute on function recsys.unblocked(uuid,uuid)to yap_recsys_api,yap_recsys_worker;
alter policy comments_read on feed.comments using(deleted_at is null and exists(select 1 from feed.posts where id=post_id)and recsys.account_allowed(author_id)and recsys.unblocked(recsys.viewer(),author_id));
create function recsys.authoritative_feedback()returns trigger language plpgsql security definer set search_path='' as $$
declare u uuid;p uuid;k text;surface text:=coalesce(nullif(current_setting('yap.surface',true),''),'direct');
begin
 if tg_table_name in('likes','reposts')then
  if tg_op='DELETE'then u:=old.user_id;p:=old.post_id;else u:=new.user_id;p:=new.post_id;end if;
  k:=case when tg_table_name='likes'then 'like'else 'repost'end;
  if tg_op='DELETE'then k:='un'||k;end if;
 elsif tg_table_name='comments'then u:=new.author_id;p:=new.post_id;k:='comment';
 elsif tg_table_name='comment_likes'then u:=new.user_id;select post_id into p from feed.comments where id=new.comment_id;k:='comment_like';
 elsif tg_table_name='reports'then u:=new.reporter_id;p:=new.post_id;if p is null then select post_id into p from feed.comments where id=new.comment_id;end if;k:='report';
 end if;
 if surface not in('for_you','friends','nearby','direct','inbox')then surface:='direct';end if;
 if p is not null then insert into feed.events(id,user_id,post_id,kind,surface)values(gen_random_uuid(),u,p,k,surface);end if;
 if tg_op='DELETE'then return old;end if;return new;
end$$;
revoke all on function recsys.authoritative_feedback()from public,anon,authenticated;
create trigger likes_feedback after insert or delete on feed.likes for each row execute function recsys.authoritative_feedback();
create trigger reposts_feedback after insert or delete on feed.reposts for each row execute function recsys.authoritative_feedback();
create trigger comments_feedback after insert on feed.comments for each row execute function recsys.authoritative_feedback();
create trigger comment_likes_feedback after insert on feed.comment_likes for each row execute function recsys.authoritative_feedback();
create trigger reports_feedback after insert on feed.reports for each row execute function recsys.authoritative_feedback();
commit;
