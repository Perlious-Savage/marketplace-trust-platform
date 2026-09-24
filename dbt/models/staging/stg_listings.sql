select
    kafka_partition, kafka_offset, op,
    (row_json->>'listing_id')::int as listing_id,
    (row_json->>'seller_id')::int as seller_id,
    row_json->>'category' as category,
    row_json->>'subcategory' as subcategory,
    row_json->>'location' as location,
    row_json->>'title' as title,
    (before_json->>'price')::double as price_before,
    (after_json->>'price')::double as price_after,
    replace(row_json->>'created_at', 'Z', '')::timestamp as created_at,
    replace(row_json->>'updated_at', 'Z', '')::timestamp as updated_at,
    epoch_ms(source_ts_ms) as event_ts
from {{ ref('stg_cdc_events') }}
where topic = 'mkt.public.listings'
