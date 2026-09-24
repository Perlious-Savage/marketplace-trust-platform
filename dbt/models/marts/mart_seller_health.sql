with l as (
    select seller_id, count(*) as listings_total,
        count(*) filter (where not is_deleted) as active_listings,
        count(*) filter (where is_deleted) as deleted_listings
    from {{ ref('dim_listing') }} group by 1
),
r as (
    select seller_id, count(*) as scored_events,
        count(*) filter (where status = 'REVIEW') as review_events,
        avg(risk_score) as avg_risk_score,
        count(*) filter (where is_seller_velocity) as velocity_alerts,
        count(*) filter (where is_near_duplicate or is_duplicate_image) as duplicate_listings
    from {{ ref('fact_risk_events') }} group by 1
),
leads as (
    select l.seller_id, count(*) as leads
    from {{ ref('fact_listing_interactions') }} i join {{ ref('dim_listing') }} l using (listing_id)
    where i.interaction_type = 'lead' group by 1
)
select
    s.seller_id, s.seller_name, s.seller_type, s.phone_shared_accounts,
    coalesce(l.listings_total, 0) as listings_total,
    coalesce(l.active_listings, 0) as active_listings,
    coalesce(l.deleted_listings, 0) as deleted_listings,
    coalesce(r.review_events, 0) as review_events,
    coalesce(r.velocity_alerts, 0) as velocity_alerts,
    coalesce(r.duplicate_listings, 0) as duplicate_listings,
    round(coalesce(r.avg_risk_score, 0), 3) as avg_risk_score,
    coalesce(leads.leads, 0) as leads,
    round(greatest(0, 1 - coalesce(r.review_events, 0) / greatest(r.scored_events, 1)
        - 0.1 * least(coalesce(r.velocity_alerts, 0), 3)
        - case when s.phone_shared_accounts > 1 then 0.2 else 0 end), 3) as health_score,
    case when health_score < 0.5 then 'at_risk' when health_score < 0.8 then 'watch' else 'healthy' end as health_status
from {{ ref('dim_seller') }} s
left join l using (seller_id)
left join r using (seller_id)
left join leads using (seller_id)
