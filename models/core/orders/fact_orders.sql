with orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select * from {{ ref('dim_customers') }}
),

products as (
    select * from {{ ref('dim_products') }}
)

select
    o.order_id,
    o.order_date,
    o.customer_id,
    c.full_name as customer_name,
    o.product_id,
    p.product_name,
    p.category,
    p.price,
    o.quantity,
    o.quantity * p.price as total_amount
from orders o
left join customers c on o.customer_id = c.customer_id
left join products p on o.product_id = p.product_id
