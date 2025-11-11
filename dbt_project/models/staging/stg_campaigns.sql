with source as (
    select * from {{ source('raw', 'campaigns') }}
),

cleaned as (
    select
        campaign_id,
        campaign_name,
        campaign_type,
        platform,
        product_id,
        start_date,
        end_date,
        budget,
        actual_spend,
        -- Fix negative impressions
        case 
            when impressions < 0 then abs(impressions)
            else impressions
        end as impressions,
        -- Cap clicks at impressions (can't have more clicks than impressions)
        case 
            when clicks > impressions then impressions
            else clicks
        end as clicks,
        created_at
    from source
    where campaign_id is not null
)

select * from cleaned
