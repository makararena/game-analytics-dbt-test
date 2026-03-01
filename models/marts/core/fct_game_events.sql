{{ config(materialized='table') }}

with events as (
    select * from {{ ref('stg_game_events') }}
),

sessions as (
    select session_id, player_id, session_start_at, session_end_at
    from {{ ref('stg_sessions') }}
),

events_with_sessions as (
    select
        e.*,
        s.session_id,
        s.session_start_at,
        s.session_end_at,
        row_number() over (
            partition by e.event_id
            order by s.session_start_at
        ) as rn
    from events e
    left join sessions s
        on e.player_id = s.player_id
        and e.event_at >= s.session_start_at
        and e.event_at <= s.session_end_at
),

events_with_one_session as (
    select
        event_id,
        event_at,
        player_id,
        event_name,
        platform,
        game_version,
        properties,
        session_id,
        session_start_at,
        session_end_at
    from events_with_sessions
    where rn = 1
),

players as (
    select player_id, country_code, language_code, difficulty_selected
    from {{ ref('stg_players') }}
),

final as (
    select
        e.event_id,
        e.event_at,
        e.player_id,
        e.event_name,
        e.platform,
        e.game_version,
        e.properties,
        e.session_id,
        e.session_start_at,
        e.session_end_at,
        p.country_code,
        p.language_code,
        p.difficulty_selected,
        case
            when e.session_id is not null then datediff('second', e.session_start_at, e.event_at)
            else null
        end as seconds_since_session_start
    from events_with_one_session e
    left join players p on e.player_id = p.player_id
)

select * from final