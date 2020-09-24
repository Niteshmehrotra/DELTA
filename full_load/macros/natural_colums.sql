{%- macro natural_columns(database_name, schema_name, table_name) -%}
    {# Get table metadata #}
{% if execute %}
    {%- set natural_query -%}
        {{ get_natural_query() }}
        where lower(table_name) = lower('{{table_name}}')
        and key_type is not null
	{%- endset -%}

	{%- set natural_key = run_query(natural_query) -%}

    { {% do natural_key.print_table() %} }

    {{ return(natural_key[0].nk_cols) }}
{% endif %}
{%- endmacro -%}


{%- macro get_natural_query() -%}
  select array_to_string(array_agg(Col_Name),',') as "nk_cols" from "LANDING"."METADATA"."TABLE_DEF"
{%- endmacro -%}