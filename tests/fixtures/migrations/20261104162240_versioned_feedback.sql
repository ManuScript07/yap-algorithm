begin;
alter table feed.events add column post_version integer not null default 1;
alter table recsys.evidence add column post_version integer not null default 1;
create function recsys.capture_event_version()returns trigger language plpgsql security definer set search_path='' as $$begin
 select version into new.post_version from feed.posts where id=new.post_id;return new;
end$$;
revoke all on function recsys.capture_event_version()from public,anon,authenticated;
create trigger event_version before insert on feed.events for each row execute function recsys.capture_event_version();
commit;
