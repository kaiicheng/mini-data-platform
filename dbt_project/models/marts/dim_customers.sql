-- Current state of customers
select
    user_id,
    email,
    first_name,
    last_name,
    city,
    state,
    age,
    customer_segment,
    created_at,
    last_login
from {{ ref('stg_users') }}
