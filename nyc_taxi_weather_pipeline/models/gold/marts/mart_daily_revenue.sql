{{config(materialized="table",schema="gold")}}
with stage as(
SELECT pickup_date,
       COUNT(*) as trip_count,
       SUM(CASE WHEN is_revenue_trip THEN calc_total_amount END) as revenue,
       ROUND(AVG(CASE WHEN is_revenue_trip THEN calc_total_amount END ),2) as avg_fare,
       ROUND(100.0*(SUM(CASE WHEN trip_type='flex_fare' THEN 1 ELSE 0 END))/COUNT(*),2) as flex_fare_percentage
FROM {{ref("gold_fact_yellow_taxi_trips")}}
GROUP BY pickup_date)
SELECT s.pickup_date,
       s.trip_count,
       s.revenue,
       s.avg_fare,
       s.flex_fare_percentage,
       c.day_name,
       c.is_weekend
FROM stage AS s
JOIN {{ref("gold_dim_calendar")}} AS c ON s.pickup_date = c.date_id