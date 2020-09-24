{{ config(
    materialized='incremental',
	transient=false,
) }}
select
	*
from
{{source('ods', 'contact_stream')}}