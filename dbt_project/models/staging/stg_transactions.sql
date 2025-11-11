with source as (
    select * from {{ source('raw', 'transactions') }}
),

cleaned as (
    select
        transaction_id,
        user_id,
        product_id,
        transaction_date,
        quantity,
        price,
        subtotal,
        tax,
        shipping,
        discount,
        discount_code,
        -- Fix negative totals
        case 
            when total < 0 then abs(total)
            else total
        end as total,
        payment_method,
        status,
        campaign_id,
        created_at
    from source
    where transaction_id is not null
        and user_id is not null
        and product_id is not null
)

select * from cleaned
