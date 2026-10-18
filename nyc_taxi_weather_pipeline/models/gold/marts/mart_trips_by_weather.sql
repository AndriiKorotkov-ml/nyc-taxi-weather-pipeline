{{config(materialized="table",schema="gold")}}

SELECT w.weather_group,
       w.weather_description,
       COUNT(*) as trips_count,
       ROUND(AVG(t.trip_duration_minutes),2) as avg_trip_duration,
       ROUND(AVG(t.trip_distance),2) as avg_trip_distance,
       SUM(CASE WHEN t.is_revenue_trip THEN t.calc_total_amount END) as revenue,
       ROUND(AVG(CASE WHEN t.is_revenue_trip THEN t.calc_total_amount END)) as avg_fare
FROM {{ref("gold_fact_yellow_taxi_trips")}} AS t
JOIN {{ref("gold_dim_weather_code")}} AS w USING(weather_code)
GROUP BY w.weather_group,w.weather_description
