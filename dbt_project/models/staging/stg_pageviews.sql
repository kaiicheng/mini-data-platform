with source as (
    select * from {{ source('raw', 'pageviews') }}
),

cleaned as (
    select
        event_id,
        event_time,
        user_id,
        session_id,
        page_type,
        product_id,
        campaign_id,
        device,
        browser,
        -- Fix negative session durations
        case 
            when session_duration_seconds < 0 then abs(session_duration_seconds)
            else session_duration_seconds
        end as session_duration_seconds
    from source
    where event_id is not null
        -- Filter out future timestamps
        and event_time <= current_timestamp
)

select * from cleaned
