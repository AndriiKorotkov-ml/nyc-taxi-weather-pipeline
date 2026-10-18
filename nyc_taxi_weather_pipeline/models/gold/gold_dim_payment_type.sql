{{config(materialized = 'table')}}
SELECT CAST(payment_type AS INTEGER) as payment_type,payment_name
FROM {{ref("payment_type")}}