-- Additive migration. Existing YapChat tables are read through restricted adapters.
-- Extension installation and this migration require a deployment/migration role.
begin;
create schema if not exists extensions;
create extension if not exists vector with schema extensions;
create extension if not exists postgis with schema extensions;
create schema feed;
create schema recsys;
revoke all on schema feed,recsys from public,anon,authenticated;
do $$begin
  if not exists(select 1 from pg_roles where rolname='yap_recsys_api')then create role yap_recsys_api nologin;end if;
  if not exists(select 1 from pg_roles where rolname='yap_recsys_worker')then create role yap_recsys_worker nologin;end if;
end$$;

create function recsys.viewer()returns uuid language sql stable set search_path='' as $$select nullif(current_setting('yap.viewer_id',true),'')::uuid$$;
create function recsys.account_allowed(u uuid)returns boolean language sql stable security definer set search_path='' as $$
 select u is not null and exists(select 1 from public.profiles where id=u and onboarding_completed)
 and not private.is_account_globally_banned(u) and not private.is_account_pending_deletion(u)
$$;
create function recsys.friends(a uuid,b uuid)returns boolean language sql stable security definer set search_path='' as $$
 select exists(select 1 from public.friendships where user_one_id=least(a,b)and user_two_id=greatest(a,b))
$$;
create function recsys.can_view(v uuid,a uuid,audience text)returns boolean language sql stable security definer set search_path='' as $$
 select recsys.account_allowed(v) and recsys.account_allowed(a) and not private.is_user_pair_blocked_impl(v,a)
 and (v=a or audience='public' or(audience='friends'and recsys.friends(v,a)))
$$;
create function recsys.precise_allowed(v uuid,a uuid)returns boolean language sql stable security definer set search_path='' as $$
 select v=a or(recsys.friends(v,a)and exists(select 1 from private.search_privacy_settings where user_id=a and share_precise_location)
 and not exists(select 1 from private.precise_location_exclusions where owner_user_id=a and viewer_user_id=v))
$$;
create function recsys.viewer_location()returns table(latitude double precision,longitude double precision)language sql stable security definer set search_path='' as $$
 select latitude,longitude from public.user_locations where user_id=recsys.viewer() and updated_at>statement_timestamp()-interval '12 hours'
$$;

create table feed.posts(
 id uuid primary key,author_id uuid not null references public.profiles(id)on delete cascade,
 text text not null check(char_length(text)<=5000),audience text not null check(audience in('public','friends')),
 language text not null check(language in('ru','en')),photos text[]not null default '{}',
 location extensions.geography(point,4326),version integer not null default 1 check(version>0),
 created_at timestamptz not null default now(),updated_at timestamptz not null default now(),deleted_at timestamptz,
 moderation_status text not null default 'allowed'check(moderation_status in('allowed','restricted','removed')),
 check(cardinality(photos)<=3),check(btrim(text)<>''or cardinality(photos)>0)
);
create index posts_recent_idx on feed.posts(created_at desc,id)where deleted_at is null and moderation_status='allowed';
create index posts_author_recent_idx on feed.posts(author_id,created_at desc,id)where deleted_at is null;
create index posts_location_idx on feed.posts using gist(location)where deleted_at is null;
create table feed.follows(user_id uuid not null references public.profiles on delete cascade,author_id uuid not null references public.profiles on delete cascade,created_at timestamptz not null default now(),primary key(user_id,author_id),check(user_id<>author_id));
create table feed.likes(user_id uuid not null references public.profiles on delete cascade,post_id uuid not null references feed.posts on delete cascade,created_at timestamptz not null default now(),primary key(user_id,post_id));
create index likes_post_idx on feed.likes(post_id);
create table feed.reposts(like feed.likes including all);
alter table feed.reposts add foreign key(user_id)references public.profiles on delete cascade;
alter table feed.reposts add foreign key(post_id)references feed.posts on delete cascade;
create index reposts_post_idx on feed.reposts(post_id);
create table feed.comments(id uuid primary key,post_id uuid not null references feed.posts on delete cascade,author_id uuid not null references public.profiles on delete cascade,parent_id uuid,text text not null check(char_length(text)between 1 and 5000),created_at timestamptz not null default now(),deleted_at timestamptz,unique(id,post_id),foreign key(parent_id,post_id)references feed.comments(id,post_id));
create table feed.comment_likes(user_id uuid not null references public.profiles on delete cascade,comment_id uuid not null references feed.comments on delete cascade,primary key(user_id,comment_id));
create table feed.reports(id uuid primary key,reporter_id uuid not null references public.profiles on delete cascade,post_id uuid references feed.posts on delete cascade,comment_id uuid references feed.comments on delete cascade,reason text not null check(reason in('spam','harassment','violence','sexual','other')),status text not null default 'pending'check(status in('pending','dismissed','confirmed')),created_at timestamptz not null default now(),check(num_nonnulls(post_id,comment_id)=1));
create unique index reports_post_unique on feed.reports(reporter_id,post_id)where post_id is not null;
create unique index reports_comment_unique on feed.reports(reporter_id,comment_id)where comment_id is not null;

create table recsys.user_state(user_id uuid primary key references public.profiles on delete cascade,preference_version integer not null default 1,reset_at timestamptz not null default '-infinity');
create table recsys.hidden_authors(user_id uuid not null references public.profiles on delete cascade,author_id uuid not null references public.profiles on delete cascade,primary key(user_id,author_id));
create table recsys.suppressed_posts(user_id uuid not null references public.profiles on delete cascade,post_id uuid not null references feed.posts on delete cascade,primary key(user_id,post_id));
create table recsys.post_embeddings(post_id uuid primary key references feed.posts on delete cascade,post_version integer not null,model text not null,embedding extensions.vector(384)not null,updated_at timestamptz not null default now());
create index post_embeddings_hnsw on recsys.post_embeddings using hnsw(embedding extensions.vector_cosine_ops);
create table recsys.interests(user_id uuid not null references public.profiles on delete cascade,cluster_id integer not null,negative boolean not null,model text not null,embedding extensions.vector(384)not null,weight double precision not null,updated_at timestamptz not null,primary key(user_id,cluster_id,negative));
create table recsys.post_stats(post_id uuid primary key references feed.posts on delete cascade,impressions bigint not null default 0,likes bigint not null default 0,reposts bigint not null default 0);
create table recsys.seen(user_id uuid not null references public.profiles on delete cascade,post_id uuid not null references feed.posts on delete cascade,seen_at timestamptz not null default now(),primary key(user_id,post_id));
create table recsys.sessions(id uuid primary key,user_id uuid not null references public.profiles on delete cascade,tab text not null check(tab in('for_you','friends','nearby')),preference_version integer not null,model text not null,created_at timestamptz not null default now(),expires_at timestamptz not null default now()+interval '20 minutes');
create index sessions_expiry_idx on recsys.sessions(expires_at);
create table recsys.session_items(session_id uuid not null references recsys.sessions on delete cascade,position integer not null,post_id uuid not null references feed.posts on delete cascade,features double precision[]not null,score double precision not null,reasons text[]not null,sources text[]not null,served_at timestamptz,primary key(session_id,position),unique(session_id,post_id));
create table feed.events(id uuid primary key,user_id uuid not null references public.profiles on delete cascade,post_id uuid not null references feed.posts on delete cascade,kind text not null check(kind in('impression','dwell','open_post','open_photo','like','unlike','repost','unrepost','comment','comment_like','not_interested','report')),surface text not null check(surface in('for_you','friends','nearby','direct','inbox')),session_id uuid references recsys.sessions on delete set null,position integer,active_ms integer not null default 0 check(active_ms between 0 and 120000),created_at timestamptz not null default now());
create unique index events_session_kind on feed.events(user_id,session_id,position,kind)where session_id is not null;
create index events_user_time on feed.events(user_id,created_at desc);
create index events_post_kind on feed.events(post_id,kind);
create table recsys.evidence(user_id uuid not null references public.profiles on delete cascade,post_id uuid not null references feed.posts on delete cascade,kind text not null,weight double precision not null,updated_at timestamptz not null default now(),primary key(user_id,post_id,kind));
create table recsys.jobs(id bigint generated always as identity primary key,kind text not null check(kind in('post','event')),target_id uuid not null,version integer not null default 0,attempts integer not null default 0,available_at timestamptz not null default now(),lease uuid,locked_until timestamptz,done_at timestamptz,last_error text,unique(kind,target_id,version));
create index jobs_ready_idx on recsys.jobs(available_at,id)where done_at is null;
create table recsys.applied_events(event_id uuid primary key references feed.events on delete cascade);

create function recsys.social_authors()returns table(author_id uuid)language sql stable security definer set search_path='' as $$
 select author_id from feed.follows where user_id=recsys.viewer()
 union select case when user_one_id=recsys.viewer()then user_two_id else user_one_id end from public.friendships where recsys.viewer()in(user_one_id,user_two_id)
$$;
create function recsys.queue_post()returns trigger language plpgsql security definer set search_path='' as $$begin
 insert into recsys.jobs(kind,target_id,version)values('post',new.id,new.version)on conflict do nothing;
 return new;
end$$;
create trigger post_outbox after insert or update of text,version,deleted_at on feed.posts for each row execute function recsys.queue_post();
create function recsys.queue_event()returns trigger language plpgsql security definer set search_path='' as $$begin
 insert into recsys.jobs(kind,target_id)values('event',new.id)on conflict do nothing;return new;
end$$;
create trigger event_outbox after insert on feed.events for each row execute function recsys.queue_event();

-- Supabase client roles have no grants on these schemas. RLS also protects the
-- restricted API connection; every transaction sets its verified subject locally.
do $$declare t record;begin
 for t in select schemaname,tablename from pg_tables where schemaname in('feed','recsys')loop
 execute format('alter table %I.%I enable row level security',t.schemaname,t.tablename);
 execute format('create policy worker on %I.%I to yap_recsys_worker using(true)with check(true)',t.schemaname,t.tablename);
 end loop;
end$$;
create policy posts_read on feed.posts for select to yap_recsys_api using(deleted_at is null and moderation_status='allowed'and recsys.can_view(recsys.viewer(),author_id,audience));
create policy posts_insert on feed.posts for insert to yap_recsys_api with check(author_id=recsys.viewer()and recsys.account_allowed(author_id));
create policy posts_update on feed.posts for update to yap_recsys_api using(author_id=recsys.viewer())with check(author_id=recsys.viewer()and recsys.account_allowed(author_id));
create policy follows_own on feed.follows to yap_recsys_api using(user_id=recsys.viewer())with check(user_id=recsys.viewer()and recsys.can_view(user_id,author_id,'public'));
create policy likes_own on feed.likes to yap_recsys_api using(user_id=recsys.viewer())with check(user_id=recsys.viewer()and exists(select 1 from feed.posts where id=post_id));
create policy reposts_own on feed.reposts to yap_recsys_api using(user_id=recsys.viewer())with check(user_id=recsys.viewer()and exists(select 1 from feed.posts where id=post_id));
create policy comments_read on feed.comments for select to yap_recsys_api using(deleted_at is null and exists(select 1 from feed.posts where id=post_id)and recsys.account_allowed(author_id)and not private.is_user_pair_blocked_impl(recsys.viewer(),author_id));
create policy comments_insert on feed.comments for insert to yap_recsys_api with check(author_id=recsys.viewer()and exists(select 1 from feed.posts where id=post_id));
create policy comment_likes_own on feed.comment_likes to yap_recsys_api using(user_id=recsys.viewer())with check(user_id=recsys.viewer()and exists(select 1 from feed.comments where id=comment_id));
create policy reports_own on feed.reports for select to yap_recsys_api using(reporter_id=recsys.viewer());
create policy reports_insert on feed.reports for insert to yap_recsys_api with check(reporter_id=recsys.viewer()and(exists(select 1 from feed.posts where id=post_id)or exists(select 1 from feed.comments where id=comment_id)));
create policy events_own on feed.events to yap_recsys_api using(user_id=recsys.viewer())with check(user_id=recsys.viewer()and exists(select 1 from feed.posts where id=post_id));
do $$declare n text;begin
 foreach n in array array['user_state','hidden_authors','suppressed_posts','interests','seen','sessions']loop
 execute format('create policy owner on recsys.%I to yap_recsys_api using(user_id=recsys.viewer())with check(user_id=recsys.viewer())',n);
 end loop;
end$$;
create policy items_own on recsys.session_items to yap_recsys_api using(exists(select 1 from recsys.sessions where id=session_id and user_id=recsys.viewer()))with check(exists(select 1 from recsys.sessions where id=session_id and user_id=recsys.viewer()));
create policy embeddings_read on recsys.post_embeddings for select to yap_recsys_api using(exists(select 1 from feed.posts where id=post_id));
create policy stats_read on recsys.post_stats for select to yap_recsys_api using(exists(select 1 from feed.posts where id=post_id));

revoke all on all functions in schema recsys from public,anon,authenticated;
grant usage on schema feed,recsys,extensions to yap_recsys_api,yap_recsys_worker;
grant execute on all functions in schema recsys to yap_recsys_api,yap_recsys_worker;
grant select,insert,update,delete on all tables in schema feed,recsys to yap_recsys_worker;
grant usage on all sequences in schema recsys to yap_recsys_worker;
grant select,insert,update on feed.posts to yap_recsys_api;
grant select,insert,delete on feed.follows,feed.likes,feed.reposts,feed.comment_likes to yap_recsys_api;
grant select,insert on feed.events,feed.comments,feed.reports to yap_recsys_api;
grant select,insert,update,delete on recsys.user_state,recsys.hidden_authors,recsys.suppressed_posts,recsys.interests,recsys.seen,recsys.sessions,recsys.session_items to yap_recsys_api;
grant select on recsys.post_embeddings,recsys.post_stats to yap_recsys_api;
-- Workers do not receive arbitrary access to existing core or private tables.
commit;
