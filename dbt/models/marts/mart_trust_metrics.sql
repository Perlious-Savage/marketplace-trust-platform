select
    date_trunc('hour', event_ts) as metric_hour,
    count(*) as listings_scored,
    count(*) filter (where status != 'OK') as flagged,
    count(*) filter (where status = 'REVIEW') as review,
    round(avg(is_near_duplicate::int), 4) as duplicate_rate,
    round(avg(is_price_anomaly::int), 4) as price_anomaly_rate,
    count(*) filter (where is_seller_velocity) as velocity_alerts,
    count(*) filter (where is_shared_phone) as shared_phone_alerts,
    round(avg(risk_score), 3) as avg_risk_score,
    quantile_cont(latency_ms, 0.95) as p95_latency_ms
from {{ ref('fact_risk_events') }}
group by 1
