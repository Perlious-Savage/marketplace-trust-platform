select
    md5(concat_ws('-', source_partition, source_offset)) as risk_event_id,
    listing_id, seller_id, risk_score, status, flags, event_ts, latency_ms,
    list_contains(flags, 'near_duplicate') as is_near_duplicate,
    list_contains(flags, 'duplicate_image') as is_duplicate_image,
    list_contains(flags, 'seller_velocity') as is_seller_velocity,
    list_contains(flags, 'shared_phone') as is_shared_phone,
    list_contains(flags, 'price_anomaly_low') or list_contains(flags, 'price_anomaly_high') as is_price_anomaly
from {{ ref('stg_risk_events') }}
