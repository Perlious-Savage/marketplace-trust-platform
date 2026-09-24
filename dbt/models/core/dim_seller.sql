with latest as (
    select * from {{ ref('stg_sellers') }}
    qualify row_number() over (partition by seller_id order by kafka_offset desc) = 1
)
select seller_id, seller_name, phone, seller_type, created_at,
    count(*) over (partition by phone) as phone_shared_accounts
from latest
where op != 'd'
