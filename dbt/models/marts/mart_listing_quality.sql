-- Freshness + engagement + latest risk for every live listing
with inter as (
    select listing_id,
        count(*) filter (where interaction_type = 'view' and created_at >= now()::timestamp - interval 7 day) as views_7d,
        count(*) filter (where interaction_type = 'lead' and created_at >= now()::timestamp - interval 7 day) as leads_7d,
        max(created_at) as last_interaction_at
    from {{ ref('fact_listing_interactions') }}
    group by 1
),
risk as (
    select listing_id, arg_max(risk_score, event_ts) as latest_risk_score, arg_max(status, event_ts) as risk_status
    from {{ ref('fact_risk_events') }}
    group by 1
)
select
    l.listing_id, l.seller_id, l.category, l.subcategory, l.location, l.current_price, l.created_at, l.updated_at,
    l.price_changes,
    date_diff('day', l.created_at, now()::timestamp) as days_since_created,
    date_diff('day', l.updated_at, now()::timestamp) as days_since_update,
    coalesce(i.views_7d, 0) as views_7d,
    coalesce(i.leads_7d, 0) as leads_7d,
    i.last_interaction_at,
    r.latest_risk_score,
    coalesce(r.risk_status, 'UNSCORED') as risk_status,
    days_since_update > 21 and views_7d < 3 as is_stale,
    case when risk_status = 'REVIEW' then 'poor' when is_stale then 'stale' when leads_7d > 0 then 'good' else 'ok' end
        as quality_band
from {{ ref('dim_listing') }} l
left join inter i using (listing_id)
left join risk r using (listing_id)
where not l.is_deleted
