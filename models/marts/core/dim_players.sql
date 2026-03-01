{{ config(materialized='table') }}

with players as (
    select * from {{ ref('stg_players') }}
),

sessions_agg as (
    select
        player_id,
        count(*) as total_sessions,
        sum(session_duration_minutes) as total_playtime_minutes,
        avg(session_duration_minutes) as avg_session_duration_minutes,
        min(session_start_at) as first_session_at,
        max(session_start_at) as last_session_at,
        count(distinct date(session_start_at)) as active_days
    from {{ ref('stg_sessions') }}
    group by player_id
),

final as (
    select
        p.*,
        coalesce(s.total_sessions, 0) as total_sessions,
        coalesce(s.total_playtime_minutes, 0) as total_playtime_minutes,
        coalesce(s.avg_session_duration_minutes, 0) as avg_session_duration_minutes,
        s.first_session_at,
        s.last_session_at,
        coalesce(s.active_days, 0) as active_days,
        datediff('day', p.first_seen_at, current_timestamp()) as days_since_first_seen,
        case
            when s.last_session_at is not null then datediff('day', s.last_session_at, current_timestamp())
            else null
        end as days_since_last_session
    from players p
    left join sessions_agg s on p.player_id = s.player_id
)

select * from final