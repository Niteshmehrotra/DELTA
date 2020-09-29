{{ config(
    materialized='table',
    	post_hook={
      "sql": "merge into landing.dev_sal.customer_delta_history ch using landing.dev_sal.customer_change_data ccd on ch.id = ccd.id and ch.start_time = ccd.start_time when matched and ccd.dml_type = 'U' then update  set ch.end_time = ccd.end_time, ch.current_flag = 0 when matched and ccd.dml_type = 'D' then update set ch.end_time = ccd.end_time, ch.current_flag = 0 when not matched and ccd.dml_type = 'I' then insert (BATCHID, BATCH_DATE,ID, FIRST_NAME, LAST_NAME, AGE, start_time, end_time, current_flag) values (ccd.BATCHID, ccd.BATCH_DATE,ID, ccd.FIRST_NAME, ccd.LAST_NAME, ccd.AGE, ccd.start_time, ccd.end_time, ccd.current_flag)",
      "transaction": true
      }
) }}

-- depends on: {{ ref('customer_delta_history') }}


select BATCHID,BATCH_DATE,ID, FIRST_NAME, LAST_NAME, AGE, start_time, end_time, current_flag, 'I' as dml_type
from (select BATCHID,ID, FIRST_NAME, LAST_NAME, AGE,BATCH_DATE,
             BATCH_DATE as start_time,
             lag(BATCH_DATE) over (partition by ID order by BATCH_DATE desc) as end_time_raw,

             case when end_time_raw is null then '9999-12-31'::timestamp_ntz else end_time_raw end as end_time,
             case when end_time_raw is null then 1 else 0 end as current_flag
      from (select BATCHID,ID, FIRST_NAME, LAST_NAME, AGE,BATCH_DATE
            from LANDING.ODS.customer_delta_changes
            where metadata$action = 'INSERT'
            and metadata$isupdate = 'FALSE'
            and CHANGE_OPERATION_TYPE='I' ))
union
-- This subquery figures out what to do when data is updated in the CUSTOMER_DELTA table
-- An update to the CUSTOMER_DELTA table results in an update AND an insert to the CUSTOMER_HISTORY table
-- The subquery below generates two records, each with a different dml_type
select BATCHID,BATCH_DATE,ID, FIRST_NAME, LAST_NAME, AGE, start_time, end_time, current_flag, dml_type
from (select BATCHID,BATCH_DATE,ID, FIRST_NAME, LAST_NAME, AGE,
             BATCH_DATE as start_time,
             lag(BATCH_DATE) over (partition by ID order by BATCH_DATE desc) as end_time_raw,
             case when end_time_raw is null then '9999-12-31'::timestamp_ntz else end_time_raw end as end_time,
             case when end_time_raw is null then 1 else 0 end as current_flag,
             dml_type
      from (-- Identify data to insert into nation_history table
            select ID,BATCHID, FIRST_NAME, LAST_NAME, AGE,BATCH_DATE, 'I' as dml_type
            from LANDING.ODS.customer_delta_changes
            where metadata$action = 'INSERT'
            and metadata$isupdate = 'FALSE'
            and  CHANGE_OPERATION_TYPE='U'
            union
            -- Identify data in NATION_HISTORY table that needs to be updated
            select ID, null, null, null, null, start_time, 'U' as dml_type
            from LANDING.DEV_SAL.customer_delta_history
            where ID in (select distinct ID
                                  from ODS.customer_delta_changes
                                  where metadata$action = 'INSERT'
                                  and metadata$isupdate = 'FALSE'
                                  and CHANGE_OPERATION_TYPE='U'  )
     and current_flag = 1))
union
-- This subquery figures out what to do when data is deleted from the CUSTOMER_DELTA table, soft deletes
-- A deletion from the CUSTOMER_DELTA table results in an update to the CUSTOMER_HISTORY_DELTA table
select cdc.batchid,null,cdc.id, null, null, null, ch.start_time, current_timestamp()::timestamp_ntz, null, 'D'
from LANDING.DEV_SAL.customer_delta_history ch
inner join LANDING.ODS.customer_delta_changes cdc
   on ch.ID = cdc.ID
where cdc.metadata$action = 'INSERT'
and   cdc.metadata$isupdate = 'FALSE'
and  cdc.CHANGE_OPERATION_TYPE='D'
and   ch.current_flag = 1