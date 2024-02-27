{{ config(materialized='table') }}

with trips_data as (
    select * from {{ ref('fact_fhv_trips') }}
)
    select 
    service_type, 
    from trips_data