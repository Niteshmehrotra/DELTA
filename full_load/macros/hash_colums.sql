{%- macro hash_columns(database_name, schema_name, table_name) -%}
    {# Get table metadata #}
{% if execute %}
    {%- set hash_query -%}
        {{ get_hash_query() }}
        where lower(table_name) = lower('{{table_name}}')
        and key_type is null
	{%- endset -%}

	{%- set hash_results = run_query(hash_query) -%}

    {% do hash_results.print_table() %}

    {{ return(hash_results[0].hash_cols) }}
{% endif %}
{%- endmacro -%}


{%- macro get_hash_query() -%}
  select array_to_string(array_agg(Col_Name),'||') as "hash_cols" from "LANDING"."METADATA"."TABLE_DEF"
{%- endmacro -%}