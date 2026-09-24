select
    kafka_offset, op,
    (row_json->>'seller_id')::int as seller_id,
    row_json->>'name' as seller_name,
    row_json->>'phone' as phone,
    row_json->>'seller_type' as seller_type,
    replace(row_json->>'created_at', 'Z', '')::timestamp as created_at
from {{ ref('stg_cdc_events') }}
where topic = 'mkt.public.sellers'
