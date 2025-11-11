with source as (
    select * from {{ source('raw', 'users') }}
),

cleaned as (
    select
        user_id,
        email,
        first_name,
        last_name,
        city,
        state,
        -- Filter unrealistic ages but keep nulls
        case 
            when age < 18 or age > 100 then null
            else age
        end as age,
        customer_segment,
        created_at,
        last_login
    from source
    where user_id is not null
)

select * from cleaned
