.mode json
select count(distinct s.id) as eligible_public_story_count
from stories s
join story_events se on se.story_id = s.id and se.tenant_id = s.tenant_id
join inputs i on i.id = se.input_id and i.tenant_id = s.tenant_id
where s.tenant_id = '00000000-0000-0000-0000-00000000t328'
  and coalesce(json_extract(i.acl, '$.privacy'), 'public') = 'public'
  and not exists (
    select 1 from story_quarantines sq
    where sq.story_id = s.id
      and sq.tenant_id = s.tenant_id
      and sq.rollback_status <> 'restored'
  );
