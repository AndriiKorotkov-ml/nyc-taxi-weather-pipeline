with gold as(
SELECT COUNT(*) as row_count
FROM {{ref("gold_obt_taxi_weather")}}),
silver as(
    SELECT COUNT(*) as row_count 
    FROM {{ref("silver_yellow_taxi")}}
)

SELECT silver.row_count as silver_rows,
       gold.row_count as gold_rows
FROM silver,gold
WHERE silver.row_count <> gold.row_count

