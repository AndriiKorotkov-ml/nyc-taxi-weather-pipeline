SELECT * 
FROM {{ref("gold_obt_taxi_weather")}}
WHERE trip_duration_minutes > 60 * 24