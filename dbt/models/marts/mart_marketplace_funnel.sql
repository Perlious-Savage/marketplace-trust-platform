-- listings created -> views -> favorites -> leads, per day x category x location
with created as (
    select created_at::date as activity_date, category, location, count(*) as listings_created
    from {{ ref('dim_listing') }} group by all
),
inter as (
    select i.interaction_date as activity_date, l.category, l.location,
        count(*) filter (where interaction_type = 'view') as views,
        count(*) filter (where interaction_type = 'favorite') as favorites,
        count(*) filter (where interaction_type = 'lead') as leads
    from {{ ref('fact_listing_interactions') }} i join {{ ref('dim_listing') }} l using (listing_id)
    group by all
)
select
    coalesce(c.activity_date, i.activity_date) as activity_date,
    coalesce(c.category, i.category) as category,
    coalesce(c.location, i.location) as location,
    coalesce(c.listings_created, 0) as listings_created,
    coalesce(i.views, 0) as views,
    coalesce(i.favorites, 0) as favorites,
    coalesce(i.leads, 0) as leads,
    round(coalesce(i.leads, 0) / nullif(i.views, 0), 4) as view_to_lead_rate
from created c
full outer join inter i
    on c.activity_date = i.activity_date and c.category = i.category and c.location = i.location
