{{ config(
    materialized='incremental',
	transient=false,
) }}
select
	*
from
{{source('ods', 'customer_stream')}}