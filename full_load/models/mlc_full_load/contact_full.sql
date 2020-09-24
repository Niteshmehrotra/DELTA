--ref ('contact_stream')
{{ config(
    materialized='incremental',
    unique_key=var ('db_primary_key'),
	transient=false,
    post_hook=["truncate table {{ref('contact_stream')}}"]
) }}

{{ full_load_bkp(
	database_name="landing",
    schema_name="metadata",
    table_name="CONTACT",
    SOURCE=ref ('contact_stream')
) }}

