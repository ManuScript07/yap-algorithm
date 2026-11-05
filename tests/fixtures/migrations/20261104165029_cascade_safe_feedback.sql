begin;
create or replace function recsys.authoritative_feedback()returns trigger language plpgsql security definer set search_path='' as $$
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
 -- FK cascades are cleanup, not new user actions. Their referenced rows may
 -- already be gone; inserting fresh feedback would break account deletion.
 if p is not null and exists(select 1 from feed.posts where id=p)and exists(select 1 from public.profiles where id=u)then
  insert into feed.events(id,user_id,post_id,kind,surface)values(gen_random_uuid(),u,p,k,surface);
 end if;
 if tg_op='DELETE'then return old;end if;return new;
end$$;
commit;
