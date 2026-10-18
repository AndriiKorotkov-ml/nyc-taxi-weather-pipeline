{{config(severity='warn')}}

SELECT * 
FROM {{ref("silver_yellow_taxi")}}
WHERE payment_type IN (1,2,3,4) AND vendor_id IN (1,2)
AND total_amount <> fare_amount + extra + mta_tax + tip_amount + tolls_amount + improvement_surcharge + CASE WHEN vendor_id = 2
             THEN COALESCE(congestion_surcharge, 0) + COALESCE(airport_fee, 0) + cbd_congestion_fee
             ELSE 0 END