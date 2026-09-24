select
    (row_json->>'interaction_id')::bigint as interaction_id,
    (row_json->>'listing_id')::int as listing_id,
    row_json->>'interaction_type' as interaction_type,
    replace(row_json->>'created_at', 'Z', '')::timestamp as created_at
from {{ ref('stg_cdc_events') }}
where topic = 'mkt.public.listing_interactions' and op in ('c', 'r')
