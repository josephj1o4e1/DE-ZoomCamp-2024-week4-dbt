-- CREATE OR REPLACE TABLE `level-night-412601.trips_data_all.green_tripdata` AS
-- select * from `bigquery-public-data.new_york_taxi_trips.tlc_green_trips_2019`
-- ;

-- CREATE OR REPLACE TABLE `level-night-412601.trips_data_all.yellow_tripdata` AS
-- select * from `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2019`
-- ;

-- INSERT INTO `level-night-412601.trips_data_all.green_tripdata`
-- select * from `bigquery-public-data.new_york_taxi_trips.tlc_green_trips_2020`
-- ;

-- INSERT INTO `level-night-412601.trips_data_all.yellow_tripdata`
-- select * from `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2020`
-- ;

-- CREATE OR REPLACE EXTERNAL TABLE `level-night-412601.nytaxi.external_fhv_tripdata`
-- OPTIONS (
--   format = 'PARQUET',
--   uris = ['gs://level-night-412601-data-lake-bucket/fhv/fhv_tripdata_2019-01/chunk*.parquet',
--           'gs://level-night-412601-data-lake-bucket/fhv/fhv_tripdata_2019-02/chunk*.parquet',
--           'gs://level-night-412601-data-lake-bucket/fhv/fhv_tripdata_2019-03/chunk*.parquet',
--           'gs://level-night-412601-data-lake-bucket/fhv/fhv_tripdata_2019-04/chunk*.parquet',
--           'gs://level-night-412601-data-lake-bucket/fhv/fhv_tripdata_2019-05/chunk*.parquet',
--           'gs://level-night-412601-data-lake-bucket/fhv/fhv_tripdata_2019-06/chunk*.parquet',
--           'gs://level-night-412601-data-lake-bucket/fhv/fhv_tripdata_2019-07/chunk*.parquet',
--           'gs://level-night-412601-data-lake-bucket/fhv/fhv_tripdata_2019-08/chunk*.parquet',
--           'gs://level-night-412601-data-lake-bucket/fhv/fhv_tripdata_2019-09/chunk*.parquet',
--           'gs://level-night-412601-data-lake-bucket/fhv/fhv_tripdata_2019-10/chunk*.parquet',
--           'gs://level-night-412601-data-lake-bucket/fhv/fhv_tripdata_2019-11/chunk*.parquet',
--           'gs://level-night-412601-data-lake-bucket/fhv/fhv_tripdata_2019-12/chunk*.parquet'
--           ]
-- )
-- ;


-- !!!! chunk15 of 2019-05 DULocationID is Integer64, where others are FLOAT
-- CREATE OR REPLACE TABLE `level-night-412601.trips_data_all.fhv_tripdata` AS
-- select * from `level-night-412601.nytaxi.external_fhv_tripdata`
-- ;



select count(*) from `trips_data_all.green_tripdata`; 
-- 8035139
select count(*) from `trips_data_all.yellow_tripdata`; 
-- 109247514
select count(*) from `trips_data_all.fhv_tripdata`; 
-- 43244696

SELECT count(*) FROM `level-night-412601.dbt_schuang.fact_fhv_trips`;

SELECT count(*) FROM `level-night-412601.dbt_schuang.fact_fhv_trips` where DATE(pickup_datetime) between '2019-07-01' and '2019-07-31'; 
-- fhv: 290680
SELECT count(*) FROM `level-night-412601.dbt_schuang.fact_trips` where service_type='Green' and DATE(pickup_datetime) between '2019-07-01' and '2019-07-31'; 
-- green: 415404
SELECT count(*) FROM `level-night-412601.dbt_schuang.fact_trips` where service_type='Yellow' and DATE(pickup_datetime) between '2019-07-01' and '2019-07-31'; 
-- yellow: 3248521




