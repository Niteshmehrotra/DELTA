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

### Setup on Snowflake : 

## Database : 
	LANDING
## Schema in Landing Database
	METADATA
	ODS
	DEV_SAL

## Def for Metadata Table
create or replace table LANDING.METADATA.TABLE_DEF (
  Table_name varchar(100),
  Col_Name varchar(100),
  Col_type varchar(100),
  key_type varchar(100) default null
)

## Def for Table 
create or replace TABLE LANDING.ODS.CONTACT (
	BATCHID NUMBER(38,0),
	BATCH_DATE TIMESTAMP_NTZ(9),
	ID NUMBER(38,0),
	NAME VARCHAR(100),
	AGE NUMBER(38,0)
);

## Create Stream on Sample Table 
CREATE OR REPLACE STREAM "LANDING"."ODS"."CONTACT_STREAM" ON TABLE "LANDING"."ODS"."CONTACT"
APPEND_ONLY=TRUE

## Sample Record in Metadata Table 
INSERT INTO LANDING.METADATA.TABLE_DEF
VALUES ('CONTACT','ID','NUMBER','Natural');

INSERT INTO LANDING.METADATA.TABLE_DEF
VALUES ('CONTACT','NAME','VARCHAR',null);

INSERT INTO LANDING.METADATA.TABLE_DEF
VALUES ('CONTACT','AGE','NUMBER',null);

## Sample Records in Table 

-- Initial FULL load in  Source Table 3 records 
insert into ods.contact
 select
	1 as batchid ,
	current_date -3 as batch_date,
	1 as id ,
	'john' as name ,
	20 as age
union all
select
	1 as batchid ,
	current_date -3 as batch_date,
	2 as id ,
	'mary' as name ,
	23 as age
union all
select
	1 as batchid ,
	current_date -3 as batch_date,
	3 as id ,
	'jane' as name ,
	23 as age
	;


---Second Full load ----- 
-- updating John's age from 20 to 23
-- adding New recod for Jenny 
-- Delete Mary 
-- Jane as it is
  
  insert into ods.contact
 select
	2 as batchid ,
	current_date -2 as batch_date,
	1 as id ,
	'john' as name ,
	23 as age
union all
select
	2 as batchid ,
	current_date -2  as batch_date,
	4 as id ,
	'jenny' as name ,
	23 as age
union all
select
	2 as batchid ,
	current_date -2  as batch_date,
	3 as id ,
	'jane' as name ,
	23 as age
	;        
---Third full Load ----- 
--adding Mary again 
-- Rest all records as is 
  
insert into ods.contact
 select
	3 as batchid ,
	current_date  as batch_date,
	1 as id ,
	'john' as name ,
	23 as age
union all
select
	3 as batchid ,
	current_date   as batch_date,
	4 as id ,
	'jenny' as name ,
	23 as age
union all
select
	3 as batchid ,
	current_date  as batch_date,
	3 as id ,
	'jane' as name ,
	23 as age
union all
select
	3 as batchid ,
	current_date  as batch_date,
	2 as id ,
	'mary' as name ,
	23 as age
    

