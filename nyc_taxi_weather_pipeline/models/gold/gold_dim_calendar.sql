{{config(materialized = 'table')}}

SELECT 
    d AS date_id,
    YEAR(d) AS year,
    MONTH(d) AS month,
    DATE_FORMAT(d,'MMMM') AS month_name,
    DAY(d) AS day,
    DAYOFWEEK(d) AS day_of_week,
    DATE_FORMAT(d,'EEEE') AS day_name,
    DAYOFWEEK(d) IN (1,7) AS is_weekend


FROM (EXPLODE(SEQUENCE(DATE'2010-01-01',DATE'2030-12-31',INTERVAL 1 DAY)) ) AS dates(d)