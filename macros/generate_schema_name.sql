{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set base_schema = target.schema -%}

    {# ---- CI ENV ---- #}
    {%- if target.name == 'ci' -%}

        {%- if custom_schema_name is none -%}
            {{ base_schema }}
        {%- else -%}
            {{ base_schema }}_{{ custom_schema_name | trim | upper }}
        {%- endif -%}

    {# ---- NON-CI ENV ---- #}
    {%- else -%}

        {%- if custom_schema_name is none -%}
            {{ base_schema }}
        {%- elif custom_schema_name | trim | lower == 'staging' -%}
            {{ var('staging_schema', 'STAGING') }}
        {%- elif custom_schema_name | trim | lower == 'marts' -%}
            {{ var('marts_schema', 'MARTS') }}
        {%- else -%}
            {{ base_schema }}_{{ custom_schema_name | trim | upper }}
        {%- endif -%}

    {%- endif -%}

{%- endmacro %}