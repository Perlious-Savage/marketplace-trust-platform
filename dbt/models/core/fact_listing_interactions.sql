select interaction_id, listing_id, interaction_type, created_at, created_at::date as interaction_date
from {{ ref('stg_interactions') }}
