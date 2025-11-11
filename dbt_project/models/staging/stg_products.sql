with source as (
    select * from {{ source('raw', 'products') }}
),

cleaned as (
    select
        product_id,
        product_name,
        brand,
        category,
        -- Fix negative prices
        case 
            when price < 0 then abs(price)
            else price
        end as price,
        cost,
        stock_quantity,
        created_at,
        updated_at
    from source
    -- Filter out products with no ID
    where product_id is not null
)

select * from cleaned
