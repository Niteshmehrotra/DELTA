#This macro overrides the macro of the same name built in to dbt, this allows custom schema naming

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}

#Hashing function
{% macro hashing(field) -%}
    {% if var('db_type') == 'postgres' %}
    	cast(digest(cast({{field}} as varchar), 'sha2') as {{dbt_utils.type_string()}})
    {% elif var ('db_type') == 'snowflake' %}
		cast(hash({{field}}) as {{dbt_utils.type_string()}})
    {% endif %}
{%- endmacro %}