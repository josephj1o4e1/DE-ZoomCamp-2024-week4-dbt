{{ config(materialized='view') }}
 
with tripdata as 
(
  select *
  from {{ source('staging','fhv_tripdata') }}
  where PUlocationID is not null or DOlocationID is not null
)
select
    dispatching_base_num,
   -- identifiers
    {{ dbt.safe_cast("PUlocationID", api.Column.translate_type("integer")) }} as pickup_location_id,
    {{ dbt.safe_cast("DOlocationID", api.Column.translate_type("integer")) }} as dropoff_location_id,

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,
    SR_Flag,
    Affiliated_base_number

from tripdata

-- dbt build --select <model.sql> --vars '{'is_test_run: false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}