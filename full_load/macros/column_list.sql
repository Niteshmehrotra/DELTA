{%- macro column_list(database_name, schema_name, table_name) -%}
    {# Get table metadata #}
{% if execute %}
    {%- set column_list_query -%}
        {{ get_column_list_query() }}
        where lower(table_name) = lower('{{table_name}}')
	{%- endset -%}

	{%- set column_results = run_query(column_list_query) -%}

    { {% do column_results.print_table() %} }

    {{ return(column_results[0]) }}
{% endif %}
{%- endmacro -%}


{%- macro get_column_list_query() -%}
  select array_to_string(array_agg(Col_Name),',') as "col_list" from "LANDING"."METADATA"."TABLE_DEF"
{%- endmacro -%}