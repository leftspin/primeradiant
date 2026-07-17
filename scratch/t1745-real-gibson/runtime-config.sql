.mode json
select prompt_version,
       json_extract(scope, '$.prompt_version_hash') as prompt_version_hash,
       inserted_at
from agent_runs
where prompt_version = 'story-identity.v1.t1269.live-loop'
order by inserted_at desc
limit 1;
