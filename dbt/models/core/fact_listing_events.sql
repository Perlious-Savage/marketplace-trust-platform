select
    md5(concat_ws('-', kafka_partition, kafka_offset)) as listing_event_id,
    listing_id, seller_id,
    md5(concat_ws('|', category, subcategory)) as category_key,
    md5(location) as location_key,
    case op
        when 'c' then 'created' when 'r' then 'snapshot' when 'd' then 'deleted'
        else case when price_before is distinct from price_after then 'price_changed' else 'updated' end
    end as event_type,
    price_before, price_after, event_ts, kafka_partition, kafka_offset
from {{ ref('stg_listings') }}
