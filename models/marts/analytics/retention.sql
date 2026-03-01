{{ config(materialized='table') }}
-- Материализуем как таблицу.
-- Ретеншен обычно используется в дашбордах и часто пересчитывается,
-- поэтому лучше хранить результат как физическую таблицу, а не view.

with cohorts as (

    -- ШАГ 1. Определяем когорту каждого игрока.
    -- Когорта = дата первого появления игрока в игре.
    -- Дополнительно сохраняем атрибуты, по которым будем сегментировать:
    -- страна и выбранная сложность.

    select
        player_id,
        date(first_seen_at) as cohort_date,      -- дата попадания в когорту (день 0)
        country_code,
        difficulty_selected
    from {{ ref('stg_players') }}
),

session_dates as (

    -- ШАГ 2. Получаем список уникальных дней,
    -- когда у игрока была хотя бы одна сессия.
    -- Нам важно не количество сессий,
    -- а факт активности в конкретный день.

    select 
        distinct player_id,
        date(session_start_at) as session_date
    from {{ ref('stg_sessions') }}
),

cohort_sizes as (

    -- ШАГ 3. Считаем размер каждой когорты.
    -- То есть: сколько уникальных игроков
    -- пришло в конкретный день,
    -- в конкретной стране,
    -- на конкретной сложности.

    select
        cohort_date,
        country_code,
        difficulty_selected,
        count(distinct player_id) as cohort_size  -- размер когорты (база для процента)
    from cohorts
    group by 1, 2, 3
),

retention_raw as (

    -- ШАГ 4. Соединяем когорты с сессиями.
    -- Для каждого игрока определяем:
    -- в какой день после прихода он был активен.

    select
        c.player_id,
        c.cohort_date,
        c.country_code,
        c.difficulty_selected,
        s.session_date,

        -- Считаем разницу в днях:
        -- сколько дней прошло с момента попадания в когорту.
        -- Это и есть D0, D1, D2 и т.д.
        datediff('day', c.cohort_date, s.session_date) as days_since_cohort

    from cohorts c
    inner join session_dates s
        on c.player_id = s.player_id
        -- Берём только те сессии,
        -- которые произошли в день когорты или позже.
        -- (Иначе могли бы попасть "старые" события.)
        and s.session_date >= c.cohort_date
),

retention_agg as (

    -- ШАГ 5. Агрегируем.
    -- Теперь считаем:
    -- сколько уникальных игроков было активно
    -- в каждый N-й день после когорты.

    select
        cohort_date,
        country_code,
        difficulty_selected,
        days_since_cohort,
        count(distinct player_id) as active_players  -- сколько игроков вернулось в этот день
    from retention_raw
    group by 1, 2, 3, 4
),

final as (

    -- ШАГ 6. Считаем retention rate.
    -- Делим количество активных игроков в день N
    -- на общий размер когорты.

    select
        r.cohort_date,
        r.country_code,
        r.difficulty_selected,
        r.days_since_cohort,
        r.active_players,
        cs.cohort_size,

        -- Процент удержания.
        -- nullif защищает от деления на 0.
        (r.active_players::float / nullif(cs.cohort_size, 0) * 100) 
            as retention_rate_pct

    from retention_agg r
    left join cohort_sizes cs
        on r.cohort_date = cs.cohort_date
        and r.country_code = cs.country_code
        and r.difficulty_selected = cs.difficulty_selected
)


select * 
from final
order by cohort_date desc, days_since_cohort, country_code, difficulty_selected