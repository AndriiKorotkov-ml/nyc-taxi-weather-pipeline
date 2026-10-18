{{config(materialized = 'table')}}
SELECT CAST(weather_code AS INTEGER) as weather_code,weather_description,weather_group
FROM {{ref("weather_code")}}