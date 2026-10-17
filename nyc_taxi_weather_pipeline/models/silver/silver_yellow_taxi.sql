{{config(materialized = 'table')}}
SELECT 
    VendorID AS vendor_id,
    tpep_pickup_datetime AS pickup_datetime,
    tpep_dropoff_datetime AS dropoff_datetime,
    passenger_count,
    trip_distance,
    RatecodeID AS rate_code_id,
    PULocationID AS pu_location_id,
    DOLocationID AS do_location_id,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    Airport_fee AS airport_fee,
    cbd_congestion_fee,
    total_amount,
    CASE WHEN payment_type = 0 THEN 'flex_fare' ELSE 'standard' END AS trip_type,
    CASE WHEN total_amount = 0 THEN 'zero' WHEN total_amount < 0 THEN 'negative' WHEN total_amount IS NULL THEN 'missing' ELSE 'positive' END AS amount_status,
    _source_file,
    _ingested_at
FROM {{source('bronze','taxi_yellow')}}

WHERE tpep_pickup_datetime >= '2026-03-01 00:00:00'
  AND tpep_pickup_datetime <  '2026-04-01 00:00:00'
  AND trip_distance > 0
  AND tpep_pickup_datetime < tpep_dropoff_datetime