{%- macro metadata(database_name, schema_name, table_name) -%}
    {# Get table metadata #}
{% if execute %}
    {%- set metadata_query -%}
        {{ get_metadata_query() }}
        where   table_name   = '{{table_name}}'
	{%- endset -%}

	{%- set metadata_results = run_query(metadata_query) -%}

    { {% do metadata_results.print_table() %} }

    {{ return(metadata_results) }}
{% endif %}
{%- endmacro -%}

{%- macro get_metadata_query() -%}
    select  table_name as "table_name"
            ,col_name  as "col_name"
            ,col_type  as "col_type"
    from    LANDING.METADATA.TABLE_DEF
{%- endmacro -%}

