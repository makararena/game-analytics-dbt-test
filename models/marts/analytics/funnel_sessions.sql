{{ config(materialized='table') }}

with sessions as (
    select
        session_id,
        player_id,
        session_start_at,
        session_end_at,
        platform,
        session_duration_minutes,
        date(session_start_at) as session_date
    from {{ ref('stg_sessions') }}
),

events as (
    select player_id, event_name, event_at
    from {{ ref('stg_game_events') }}
),

session_events as (
    select
        s.session_id,
        s.player_id,
        s.session_date,
        s.platform,
        s.session_duration_minutes,
        e.event_name
    from sessions s
    left join events e
        on s.player_id = e.player_id
        and e.event_at >= s.session_start_at
        and e.event_at <= s.session_end_at
),

session_funnel as (
    select
        session_id,
        player_id,
        session_date,
        platform,
        session_duration_minutes,
        max(case when event_name = 'game_started' then 1 else 0 end) as has_game_started,
        max(case when event_name = 'chapter_started' then 1 else 0 end) as has_chapter_started,
        max(case when event_name = 'checkpoint_reached' then 1 else 0 end) as has_checkpoint_reached,
        max(case when event_name = 'chapter_completed' then 1 else 0 end) as has_chapter_completed,
        max(case when event_name = 'game_closed' then 1 else 0 end) as has_game_closed,
        count_if(event_name = 'game_started') as game_started_count,
        count_if(event_name = 'chapter_started') as chapters_started_count,
        count_if(event_name = 'checkpoint_reached') as checkpoints_reached_count,
        count_if(event_name = 'chapter_completed') as chapters_completed_count
    from session_events
    group by 1, 2, 3, 4, 5
),

players as (
    select player_id, country_code, difficulty_selected
    from {{ ref('stg_players') }}
),

session_funnel_with_players as (
    select
        f.*,
        p.country_code,
        p.difficulty_selected
    from session_funnel f
    left join players p on f.player_id = p.player_id
),

final as (
    select
        session_date,
        platform,
        country_code,
        difficulty_selected,
        count(*) as total_sessions,
        sum(has_game_started) as sessions_with_game_started,
        sum(has_chapter_started) as sessions_with_chapter_started,
        sum(has_checkpoint_reached) as sessions_with_checkpoint_reached,
        sum(has_chapter_completed) as sessions_with_chapter_completed,
        sum(has_game_closed) as sessions_with_game_closed,
        sum(has_game_started)::float / nullif(count(*), 0) * 100 as game_started_rate_pct,
        sum(has_chapter_started)::float / nullif(count(*), 0) * 100 as chapter_started_rate_pct,
        sum(has_checkpoint_reached)::float / nullif(count(*), 0) * 100 as checkpoint_reached_rate_pct,
        sum(has_chapter_completed)::float / nullif(count(*), 0) * 100 as chapter_completed_rate_pct,
        sum(has_game_closed)::float / nullif(count(*), 0) * 100 as game_closed_rate_pct,
        sum(chapters_started_count)::float / nullif(count(*), 0) as avg_chapters_started,
        sum(checkpoints_reached_count)::float / nullif(count(*), 0) as avg_checkpoints_reached,
        sum(chapters_completed_count)::float / nullif(count(*), 0) as avg_chapters_completed,
        avg(session_duration_minutes) as avg_session_duration_minutes
    from session_funnel_with_players
    group by 1, 2, 3, 4
)

select * from final
order by session_date desc, platform, country_code, difficulty_selected