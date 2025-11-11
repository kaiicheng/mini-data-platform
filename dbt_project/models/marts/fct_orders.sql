-- Order line items with denormalized dimensions
-- No aggregations - just joins to make querying easier
select
    t.transaction_id,
    t.transaction_date,
    t.status,
    t.payment_method,
    
    -- User info
    t.user_id,
    u.email as customer_email,
    u.first_name as customer_first_name,
    u.last_name as customer_last_name,
    u.customer_segment,
    u.city as customer_city,
    u.state as customer_state,
    
    -- Product info
    t.product_id,
    p.product_name,
    p.brand,
    p.category,
    p.cost as product_cost,
    
    -- Order line details
    t.quantity,
    t.price as unit_price,
    t.subtotal,
    t.tax,
    t.shipping,
    t.discount,
    t.discount_code,
    t.total,
    
    -- Calculated fields
    t.total - (p.cost * t.quantity) as line_margin,
    case when t.discount > 0 then true else false end as is_discounted,
    
    -- Campaign attribution
    t.campaign_id,
    c.campaign_name,
    c.campaign_type,
    c.platform as campaign_platform,
    c.actual_spend as campaign_spend,
    
    t.created_at
    
from {{ ref('stg_transactions') }} t
left join {{ ref('stg_users') }} u on t.user_id = u.user_id
left join {{ ref('stg_products') }} p on t.product_id = p.product_id
left join {{ ref('stg_campaigns') }} c on t.campaign_id = c.campaign_id
