# game_analytics_dbt

This project contains dbt transformations for game analytics.

## Layers

- `sources/` — descriptions for `raw_players`, `raw_sessions`, `raw_game_events`.
- `models/staging/` — cleaning and normalizing raw data.
- `models/marts/core/` — fact/dimension models for analytics.
- `models/marts/analytics/` — aggregates for reporting.
- `tests/` — singular tests.
- `macros/` — macros (including `generate_schema_name`).

## Main commands

```bash
dbt parse
dbt run --select staging
dbt run --select marts
dbt test
dbt build
```

## Schemas

Managed via `dbt_project.yml` vars:
- `raw_schema`
- `staging_schema`
- `marts_schema`
- `ci_schema`

The `generate_schema_name` macro routes models by environment (including the `ci` target).

## CI

GitHub Actions runs:
- `dbt deps`
- `dbt compile --target ci`
- `dbt build --target ci`

Goal: prevent changes that break models or tests from reaching `main`.
