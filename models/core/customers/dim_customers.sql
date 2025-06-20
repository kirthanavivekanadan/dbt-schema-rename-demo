select
    customer_id,
    initcap(first_name) || ' ' || initcap(last_name) as full_name,
    email,
    signup_date
from {{ ref('stg_customer') }}
