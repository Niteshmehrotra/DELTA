

{#-
NKHASH plus start date holds the columns for the primary key
NKHASH holds the columns for the natural key
DIFFHASH holds the non key columns for determining delta
SOURCE is the name of the source table
COLS is the list of columns being brought through, including key
-#}

{% macro full_load(NKHASH,DIFFHASH,SOURCE,COLS) %}

{% if is_incremental() %}
-- THIS IS INCREMENTAL MODE
{% endif %}
	WITH BATCH_LIST as (
		-- Picking up the list of batches, assigning current data a dummy batch (as it's a mix of batches)
		select
			{{var ('full_load_batch_id')}}::varchar(100) as tmp_bid,
			row_number() over (order by {{var ('full_load_batch_id')}} asc)::int as tmp_bsort,
			{{var ('full_load_start_date')}}::date as tmp_bdate
		from
			{{SOURCE}}
		group by {{var ('full_load_batch_id')}},{{var ('full_load_start_date')}}
		union all
		select
			'-1'::varchar(100) as tmp_bid,
			0::int as tmp_bsort,
			null::date as tmp_bdate
	)
	, BATCH_DATA as (
		-- Picking up the data for this run
		{% if is_incremental() %}
		-- Incremental load needs the current dataset
		select
			'-1'::varchar(100) as tmp_bid,
			0::int as tmp_bsort,
			{{COLS}} ,
			this.{{var ('full_load_batch_id')}},
			this.{{var ('full_load_start_date')}},
			this.{{var('db_primary_key')}},
			this.{{var('db_natural_key')}},
			this.{{var('db_hash_name')}},
			this.{{var('db_start_date_name')}},
			this.{{var('db_end_date_name')}},
			this.{{var('db_delete_flag_name')}}
		from
			{{this}} as this
		where
			this.{{var('db_end_date_name')}} is null -- current records
		union all
		{% endif %}
		select
			BATCH_LIST.tmp_bid,
			BATCH_LIST.tmp_bsort,
			{{COLS}} ,
			SRC.{{var ('full_load_batch_id')}},
			SRC.{{var ('full_load_start_date')}},
			{{hashing( ( NKHASH + '||' + var('full_load_start_date') ))}} as {{var('db_primary_key')}},
			{{hashing( NKHASH )}} as {{var('db_natural_key')}},
			{{hashing( DIFFHASH )}} as {{var('db_hash_name')}},
			{{var ('full_load_start_date')}} as {{var('db_start_date_name')}},
			null as {{var('db_end_date_name')}},
			0 as {{var('db_delete_flag_name')}}
		from
			{{SOURCE}} SRC
		inner join  BATCH_LIST on BATCH_LIST.tmp_bid = SRC.{{var ('full_load_batch_id')}}
	)
	, DELETE_CHECK as (
		select
			NEXT_BATCH.tmp_bid,
			NEXT_BATCH.tmp_bsort,
			BD.{{var('db_natural_key')}},
			BD.{{var('db_primary_key')}},
			NEXT_BATCH.tmp_bid as {{var ('full_load_batch_id')}},
			NEXT_BATCH.tmp_bdate as {{var ('full_load_start_date')}},
			NEXT_BATCH.tmp_bdate as {{var ('db_start_date_name')}},
			1 as {{var('db_delete_flag_name')}}
		from
			BATCH_DATA BD
		inner join BATCH_LIST as NEXT_BATCH on BD.tmp_bsort+1 = NEXT_BATCH.tmp_bsort
		left join BATCH_DATA BD_NEXT on BD.{{var('db_natural_key')}} = BD_NEXT.{{var('db_natural_key')}}
			and BD_NEXT.tmp_bid = NEXT_BATCH.tmp_bid
		where BD_NEXT.{{var('db_natural_key')}} is null
	)
	, DELETES as (
		select
			DC.tmp_bid,
			DC.tmp_bsort,
			{{COLS}} ,
			DC.{{var ('full_load_batch_id')}}, -- Batch it went missing in
			DC.{{var ('full_load_start_date')}},
			{{hashing(( NKHASH + '||DC.' + var('db_start_date_name')) )}} as {{var('db_primary_key')}}, -- New PK due to date change
			BD.{{var('db_natural_key')}},
			BD.{{var('db_hash_name')}}, -- Col hash is same
			DC.{{var('db_start_date_name')}}, -- New start date
			null::date as {{var('db_end_date_name')}},
			DC.{{var('db_delete_flag_name')}} -- New flag
		from
			BATCH_DATA BD
		inner join DELETE_CHECK DC on DC.{{var('db_primary_key')}} =BD.{{var('db_primary_key')}}
	)
	, FULL_DATASET as (
		select * from DELETES
		union all
		select * from BATCH_DATA
	)
	, DELTA_KEYS as (
		-- Selecting all the records changed from the prior record or next record is new
		select
			{{var('db_primary_key')}} as DELTA_KEY,
			lead({{var('db_start_date_name')}}) over (partition by {{var('db_natural_key')}} order by tmp_bsort asc) as DELTA_END_DATE,
			case when
			(
				-- Keep where the prior record has a different hash (meaning this record is an update)
			FD.{{var('db_hash_name')}} <> lag({{var('db_hash_name')}}) over (partition by {{var('db_natural_key')}} order by tmp_bsort asc)
			or
			FD.{{var('db_hash_name')}} <> lead({{var('db_hash_name')}}) over (partition by {{var('db_natural_key')}} order by tmp_bsort asc)
			or
			-- Keep records where the next records delete flag is different (as they need to be closed off - even a new insert may have a matching hash)
			{{var('db_delete_flag_name')}} <> lag({{var('db_delete_flag_name')}}) over (partition by {{var('db_natural_key')}} order by tmp_bsort asc)
			or
			{{var('db_delete_flag_name')}} <> lead({{var('db_delete_flag_name')}}) over (partition by {{var('db_natural_key')}} order by tmp_bsort asc)
			or
			-- A null lag (prior) indicates a new record (as long as it's not the current data)
			(
			lag({{var('db_hash_name')}}) over (partition by {{var('db_natural_key')}} order by tmp_bsort asc) is null
			and
			FD.tmp_bsort <> 0 -- Not already loaded data
			)
			) then 1 else 0 end as DELTA
		from
			FULL_DATASET FD
	)
	-- Final select filters to changes only for merging into main table
	select
		{{COLS}} ,
		{{var ('full_load_batch_id')}},
		{{var ('full_load_start_date')}},
		{{var('db_primary_key')}},
		{{var('db_natural_key')}},
		{{var('db_hash_name')}},
		FD.{{var('db_start_date_name')}},
		DK.DELTA_END_DATE as {{var('db_end_date_name')}},
		FD.{{var('db_delete_flag_name')}}
	from
		FULL_DATASET FD
	inner join DELTA_KEYS DK on DK.DELTA_KEY = FD.{{var('db_primary_key')}}
	where DK.DELTA=1
{%- endmacro %}