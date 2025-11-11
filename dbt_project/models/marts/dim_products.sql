-- Current state of products
select
    product_id,
    product_name,
    brand,
    category,
    price,
    cost,
    price - cost as margin,
    stock_quantity,
    created_at,
    updated_at
from {{ ref('stg_products') }}
