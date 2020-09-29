{{ config(
    materialized='incremental',
	transient=false,
	post_hook={
      "sql": "delete from landing.dev_sal.customer_delta_history where current_flag is NULL",
      "transaction": true
      }
) }}

select
	BATCHID,
	BATCH_DATE,
	ID,
	FIRST_NAME,
	LAST_NAME,
	AGE,
    cast ( NULL as TIMESTAMP_NTZ(9)) as START_TIME,
	cast (NULL as TIMESTAMP_NTZ(9)) as END_TIME,
	cast ( NULL as NUMBER(38,0)) as  CURRENT_FLAG
from
"LANDING"."ODS"."CUSTOMER_DELTA_CT"