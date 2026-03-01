{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- elif custom_schema_name | trim | lower == 'staging' -%}
        {{ var('staging_schema', 'STAGING') }}
    {%- elif custom_schema_name | trim | lower == 'marts' -%}
        {{ var('marts_schema', 'MARTS') }}
    {%- else -%}
        {{ target.schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}