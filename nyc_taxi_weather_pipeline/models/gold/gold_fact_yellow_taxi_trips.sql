{{config(materialized="table")}}
with stage as (
    SELECT *,
    ROW_NUMBER() OVER(PARTITION BY vendor_id,pickup_datetime,dropoff_datetime,pu_location_id,do_location_id,calc_total_amount ORDER BY pickup_datetime) as dup_rank
    FROM {{ref("gold_obt_taxi_weather")}}
)
SELECT {{dbt_utils.generate_surrogate_key(['vendor_id','pickup_datetime','dropoff_datetime',
                                            'pu_location_id','do_location_id','calc_total_amount','dup_rank'])}} as trip_id,
    vendor_id,
    payment_type,
    pu_location_id,
    do_location_id,
    weather_code,
    CAST(pickup_datetime AS DATE) AS pickup_date,
    HOUR(pickup_datetime) as pickup_hour,
    pickup_datetime,
    dropoff_datetime,
    trip_duration_minutes,
    trip_distance,
    passenger_count,
    calc_total_amount,
    temperature,
    precipitation,
    trip_type,
    amount_status,
    is_revenue_trip
    FROM stage
