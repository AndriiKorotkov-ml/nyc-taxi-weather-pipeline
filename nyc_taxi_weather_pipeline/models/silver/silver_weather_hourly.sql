SELECT 
    to_timestamp(r.time) as weather_hour,
    r.temperature_2m as temperature,
    r.precipitation,
    r.weather_code
FROM {{source('bronze','weather_conditions')}}
lateral view explode(arrays_zip(hourly.time,hourly.temperature_2m,hourly.precipitation,hourly.weather_code)) t as r