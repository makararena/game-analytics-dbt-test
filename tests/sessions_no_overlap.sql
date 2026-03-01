-- Singular test: returns rows where the same player has two sessions with overlapping time.
-- The test fails if any rows are returned.
-- Overlap: [s1_start, s1_end] and [s2_start, s2_end] overlap when s1_start < s2_end AND s1_end > s2_start.

select
    s1.session_id as session_id_1,
    s2.session_id as session_id_2,
    s1.player_id,
    s1.session_start_at as session_1_start_at,
    s1.session_end_at as session_1_end_at,
    s2.session_start_at as session_2_start_at,
    s2.session_end_at as session_2_end_at
from {{ ref('stg_sessions') }} s1
inner join {{ ref('stg_sessions') }} s2
    on s1.player_id = s2.player_id
    and s1.session_id < s2.session_id
    and s1.session_start_at < s2.session_end_at
    and s1.session_end_at > s2.session_start_at