{{config(materialized = 'table')}}
SELECT 
    VendorID AS vendor_id,
    tpep_pickup_datetime AS pickup_datetime,
    tpep_dropoff_datetime AS dropoff_datetime,
    CAST(passenger_count AS INT) AS passenger_count,
    trip_distance,
    CAST(RatecodeID AS INT) AS rate_code_id,
    PULocationID AS pu_location_id,
    DOLocationID AS do_location_id,
    CAST(payment_type AS INT) AS payment_type,
    CAST(fare_amount  AS DECIMAL(12,2)) AS fare_amount,
    CAST(extra  AS DECIMAL(12,2)) AS extra,
    CAST(mta_tax  AS DECIMAL(12,2)) AS mta_tax,
    CAST(tip_amount AS DECIMAL(12,2)) AS tip_amount,
    CAST(tolls_amount  AS DECIMAL(12,2)) AS tolls_amount,
    CAST(improvement_surcharge  AS DECIMAL(12,2)) AS improvement_surcharge,
    CAST(congestion_surcharge AS DECIMAL(12,2)) AS congestion_surcharge,
    CAST(Airport_fee AS DECIMAL(12,2)) AS airport_fee,
    CAST(cbd_congestion_fee AS DECIMAL(12,2)) AS cbd_congestion_fee,
    CAST(total_amount  AS DECIMAL(12,2)) AS total_amount,
    CASE WHEN payment_type = 0 THEN 'flex_fare' ELSE 'standard' END AS trip_type,
    CASE WHEN total_amount = 0 THEN 'zero' WHEN total_amount < 0 THEN 'negative' WHEN total_amount IS NULL THEN 'missing' ELSE 'positive' END AS amount_status,
    _source_file,
    _ingested_at
FROM {{source('bronze','taxi_yellow')}}

WHERE date_format(tpep_pickup_datetime, 'yyyy-MM')
        = regexp_extract(_source_file, 'yellow_tripdata_([0-9]{4}-[0-9]{2})\\.parquet', 1)
  AND trip_distance > 0
  AND tpep_pickup_datetime < tpep_dropoff_datetime
  AND tpep_dropoff_datetime <= tpep_pickup_datetime + INTERVAL 24 HOURS