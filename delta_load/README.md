Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](http://slack.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices


# Setup on Snowflake : 

## Database : 
	LANDING
## Schema in Landing Database
	ODS
	DEV_SAL


## Def for Landing Table
------------------------ CREATE LANDING TABLE-----------------------------
create or replace TABLE LANDING.ODS.CUSTOMER_DELTA_CT (
	BATCHID NUMBER(38,0),
	BATCH_DATE TIMESTAMP_NTZ(9),
	ID NUMBER(38,0),
	FIRST_NAME VARCHAR(100),
	LAST_NAME VARCHAR(200),
	AGE NUMBER(38,0),
	CHANGE_OPERATION_TYPE VARCHAR(5)
);


## Create Stream on Sample Table 
----------------------CREATE STREAM --------------------------------------
 create or replace stream LANDING.ODS.customer_delta_changes on table LANDING.ODS.customer_delta_CT 
 APPEND_ONLY=TRUE


## Sample Records in Landing Table 
-------------------------------------------------------------------------------------------------------------
--------------------------------------DATA SETUP -----------------------------------------------------------------------
 
 ## Intial Full Load

insert into ods.customer_delta_CT
 select
	1 as batchid ,
	current_date -3 as batch_date,
	1 as id ,
	'john' as FIRST_NAME ,
    'travolta' as LAST_NAME,
	20 as age,
    'I'as Change_Operation_type
union all
select
	1 as batchid ,
	current_date -3 as batch_date,
	2 as id ,
	'mary' as FIRST_NAME ,
    'thomas' as LAST_NAME,
	23 as age,
    'I' as Change_Operation_type
union all
select
	1 as batchid ,
	current_date -3 as batch_date,
	3 as id ,
	'jane' as FIRST_NAME ,
    'turner' as LAST_NAME,
	23 as age,
    'I' as Change_Operation_type
	;
---------------------------------------------------------------------------------------
-- updating John age to 35 , will result in new row 
 insert into ods.customer_delta_CT
 select
	2 as batchid ,
	current_date -2 as batch_date,
	1 as id ,
	'john' as FIRST_NAME ,
    'travolta' as LAST_NAME,
	35 as age,
    'U' as Change_Operation_type
  
-----------------------------------------------------------------------------------------
-- ADDING JENNY and DELETING JANE , will result in new rows again 

 insert into ods.customer_delta_CT
 select
	3 as batchid ,
	current_date -1 as batch_date,
	4 as id ,
	'jenny' as FIRST_NAME ,
    'jhang' as LAST_NAME,
	30 as age,
    'I' as Change_Operation_type
    UNION
   select
	3 as batchid ,
	current_date -1 as batch_date,
	3 as id ,
	'jane' as FIRST_NAME ,
    'turner' as LAST_NAME,
	23 as age,
    'D' as Change_Operation_type  

-----------------------------------------------------------------------------------------
-- ADDING  JANE again , will result in new rows again 

 insert into ods.customer_delta_CT
  select
	4 as batchid ,
	current_date  as batch_date,
	3 as id ,
	'jane' as FIRST_NAME ,
    'turner' as LAST_NAME,
	23 as age,
    'I' as Change_Operation_type  
