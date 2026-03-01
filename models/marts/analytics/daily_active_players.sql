{{ config(materialized='table') }}

with sessions as (
    select
        player_id,
        platform,
        date(session_start_at) as session_date,
        count(*) as sessions_count,
        sum(session_duration_minutes) as total_playtime_minutes
    from {{ ref('stg_sessions') }}
    group by 1, 2, 3
),

players as (
    select player_id, country_code, difficulty_selected
    from {{ ref('stg_players') }}
),

sessions_with_players as (
    select
        s.session_date,
        s.platform,
        p.country_code,
        p.difficulty_selected,
        s.player_id,
        s.sessions_count,
        s.total_playtime_minutes
    from sessions s
    left join players p on s.player_id = p.player_id
),

final as (
    select
        session_date,
        platform,
        country_code,
        difficulty_selected,
        count(distinct player_id) as active_players,
        sum(sessions_count) as total_sessions,
        sum(total_playtime_minutes) as total_playtime_minutes,
        sum(sessions_count)::float / nullif(count(distinct player_id), 0) as avg_sessions_per_player,
        sum(total_playtime_minutes)::float / nullif(count(distinct player_id), 0) as avg_playtime_minutes_per_player
    from sessions_with_players
    group by 1, 2, 3, 4
)

select * from final
order by session_date desc, platform, country_code, difficulty_selected