{{config(materialized="table",schema="gold")}}
SELECT 
    pickup_date,
    pickup_hour,
    COUNT(*) as trip_count,
    ROUND(AVG(temperature),2) as avg_temperature,
    ROUND(MAX(precipitation)) as precipitation_mm,
    CASE WHEN
    MAX(precipitation) = 0 THEN 'none'
    WHEN MAX(precipitation) <2.5 THEN 'light'
    WHEN MAX(precipitation) <7.6 THEN 'moderate'
    ELSE 'heavy' END AS precipitation_level
FROM {{ref("gold_fact_yellow_taxi_trips")}}
GROUP BY pickup_date,pickup_hour
ORDER BY pickup_date,pickup_hour
