with stage AS(SELECT t.trip_id,
       t.pu_location_id,
       t.calc_total_amount,
       t.is_revenue_trip,
       t.trip_distance,
       t.precipitation,
       l.borough,
       l.zone
FROM {{ref("gold_fact_yellow_taxi_trips")}} AS t
JOIN {{ref("gold_dim_taxi_zone_lookup")}} l
ON t.pu_location_id = l.location_id)
SELECT pu_location_id as location_id,
       borough,
       zone,
       COUNT(*) as trip_count,
       SUM(CASE WHEN is_revenue_trip THEN calc_total_amount END) AS revenue,
       ROUND(AVG(trip_distance),2) AS avg_trip_distance,
       ROUND(100.0*SUM(CASE WHEN precipitation > 0 THEN 1 ELSE 0 END)/COUNT(*),2) as precipitation_zone_trips_perc
FROM stage
GROUP BY pu_location_id,borough,zone
