select distinct md5(location) as location_key, location, 'Dubai' as city
from {{ ref('stg_listings') }}
