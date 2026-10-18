SELECT * 
FROM {{ref("gold_obt_taxi_weather")}}
WHERE trip_duration > 60 * 24