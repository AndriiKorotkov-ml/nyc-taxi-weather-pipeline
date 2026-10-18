{{config(materialized = 'table')}}
with stage_taxi as (
    SELECT 
    vendor_id,
    pickup_datetime,
    DATE_TRUNC('HOUR', pickup_datetime) AS pickup_hour,
    dropoff_datetime,
    TIMESTAMPDIFF(MINUTE, pickup_datetime, dropoff_datetime) AS trip_duration_minutes,
    passenger_count,
    trip_distance,
    rate_code_id,
    pu_location_id,
    do_location_id,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    airport_fee,
    cbd_congestion_fee,
    total_amount,
    trip_type,
    amount_status,
    (amount_status = 'positive') AS is_revenue_trip,
    CASE WHEN 
        trip_type = 'standard' THEN
        fare_amount + extra + mta_tax + tip_amount + tolls_amount
        + improvement_surcharge 
        + COALESCE(congestion_surcharge,0)
        + COALESCE(airport_fee,0)
        + cbd_congestion_fee
        END
    AS calc_total_amount
    FROM {{ref("silver_yellow_taxi")}}
)
SELECT f.*,w.temperature,w.precipitation,w.weather_code FROM stage_taxi as f
LEFT JOIN {{ref("silver_weather_hourly")}} as w
ON f.pickup_hour = w.weather_hour