select
    product_id,
    initcap(product_name) as product_name,
    initcap(category) as category,
    price
from {{ ref('stg_products') }}
