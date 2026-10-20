{{config(materialized = 'table')}}
SELECT CAST(LocationID AS INTEGER) as location_id,
        Borough as borough,
        Zone as zone,
        service_zone
FROM {{ref("taxi_zone_lookup")}} 
