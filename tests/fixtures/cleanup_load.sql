-- Only the deterministic corpus from TestTenThousandPostCorpus, in this fixture.
do $$begin
 if current_database()<>'yap_algorithm'or not exists(select 1 from private.yap_algorithm_fixture)then raise exception 'not_fixture';end if;
end$$;
delete from recsys.jobs where kind='post'and target_id in(select id from feed.posts where text like 'load corpus post %');
delete from feed.posts where text like 'load corpus post %';
