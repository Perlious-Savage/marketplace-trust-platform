-- risk.events can be re-produced on detector replay; the source CDC offset is the natural key
select
    (row_json->>'source_partition')::int as source_partition,
    (row_json->>'source_offset')::bigint as source_offset,
    (row_json->>'listing_id')::int as listing_id,
    (row_json->>'seller_id')::int as seller_id,
    (row_json->>'risk_score')::double as risk_score,
    (row_json->'flags')::varchar[] as flags,
    row_json->>'status' as status,
    epoch_ms((row_json->>'event_ts_ms')::bigint) as event_ts,
    (row_json->>'latency_ms')::int as latency_ms
from {{ ref('stg_cdc_events') }}
where topic = 'risk.events'
qualify row_number() over (partition by source_partition, source_offset order by kafka_offset) = 1
