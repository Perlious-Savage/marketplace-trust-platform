select distinct md5(concat_ws('|', category, subcategory)) as category_key, category, subcategory
from {{ ref('stg_listings') }}
