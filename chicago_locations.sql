WITH chicago as(
SELECT
  ROW_NUMBER() OVER(PARTITION BY unique_key,taxi_id) row_number,
  unique_key,
  taxi_id,
  trip_start_timestamp,
  trip_end_timestamp,
  trip_seconds,
  trip_miles,
  pickup_census_tract,
  dropoff_census_tract,
  pickup_community_area,
  dropoff_community_area,
  fare,
  tips,
  tolls,
  extras,
  trip_total,
  payment_type,
  company,
FROM `crack-descent-299314.chicago_ingest.chicago`
), locations as (

SELECT 
      ROW_NUMBER() OVER(PARTITION BY unique_key,taxi_id) row_number,
      unique_key, 
       taxi_id,
       company, 
       pickup_latitude, 
       pickup_longitude, 
       dropoff_latitude, 
       dropoff_longitude, 
       dropoff_location 
FROM `crack-descent-299314.chicago_ingest.chicago_manual`
)


SELECT c.unique_key,
    c.taxi_id,
    c.trip_start_timestamp,
    c.trip_end_timestamp,
    c.trip_seconds,
    c.trip_miles,
    c.pickup_census_tract,
    c.dropoff_census_tract,
    c.pickup_community_area,
    c.dropoff_community_area,
    c.fare,
    c.tips,
    c.tolls,
    c.extras,
    c.trip_total,
    c.payment_type,
    l.company,
    l.pickup_latitude,
    l.pickup_longitude,
    l.dropoff_latitude,
    l.dropoff_longitude,
    l.dropoff_location
FROM chicago AS C
LEFT join locations AS L ON C.unique_key = L.unique_key AND C.taxi_id = L.taxi_id AND L.row_number =1
where c.row_number=1