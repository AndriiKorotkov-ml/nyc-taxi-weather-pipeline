{{config(materialized = 'table')}}
SELECT CAST(vendor_id AS INTEGER) as vendor_id,vendor_name 
FROM  {{ref("vendor")}}