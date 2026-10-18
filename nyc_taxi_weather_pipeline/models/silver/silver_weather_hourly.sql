{{ config(
    materialized='table',
    tblproperties={'delta.feature.timestampNtz': 'supported'}
) }}
SELECT 
    to_timestamp_ntz(r.time) as weather_hour,
    r.temperature_2m as temperature,
    r.precipitation,
    CAST(r.weather_code AS INT) as weather_code 
FROM {{source('bronze','weather_conditions')}}
lateral view explode(arrays_zip(hourly.time,hourly.temperature_2m,hourly.precipitation,hourly.weather_code)) t as r