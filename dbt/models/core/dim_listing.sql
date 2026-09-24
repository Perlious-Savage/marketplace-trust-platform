-- Current state per listing (SCD1). Kafka key = listing_id, so offset order within a partition is commit order.
with ranked as (
    select *,
        row_number() over (partition by listing_id order by kafka_offset desc) as rn,
        count(*) filter (where op = 'u' and price_before is distinct from price_after)
            over (partition by listing_id) as price_changes
    from {{ ref('stg_listings') }}
)
select
    listing_id, seller_id,
    md5(concat_ws('|', category, subcategory)) as category_key,
    md5(location) as location_key,
    category, subcategory, location, title,
    coalesce(price_after, price_before) as current_price,
    op = 'd' as is_deleted,
    created_at, updated_at, price_changes,
    event_ts as last_event_ts
from ranked
where rn = 1
