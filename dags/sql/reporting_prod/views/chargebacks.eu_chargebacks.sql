CREATE OR REPLACE VIEW reporting_prod.chargebacks.eu_chargebacks AS
SELECT
    CASE WHEN store_type='Mobile App' THEN store_name || ' ' || store_type ELSE store_name END AS store_name,
    fc.customer_id AS customer_id,
    c.email AS email,
    a.lastname AS surname,
    a.firstname AS first_name,
    a.address1 AS billing_address,
    a.city AS billing_city,
    a.zip AS billing_zip ,
    DATE(o.order_local_datetime) AS order_date,
    DATE(convert_timezone('America/Chicago',fc.chargeback_datetime)) AS chargeback_date,
    ROUND(fc.chargeback_payment_transaction_local_amount*chargeback_date_eur_conversion_rate,'2') as order_value,
    fc.order_id,
    a.phone,
    umo.auth_payment_transaction_id AS payment_transaction_id,
    CONCAT('FLEU Collection Info: ',DATE(DATEADD(DAY, -9,  DATEADD(d,DATEDIFF(d,'1900-01-01',getdate()),'1900-01-01'))),' - ',DATE(DATEADD(DAY, -2,  DATEADD(d,DATEDIFF(d,'1900-01-01',getdate()),'1900-01-01')))) as title,
    dosc.order_classification_l1 AS order_type,
    'No' AS is_redeemed,
    CURRENT_TIMESTAMP AS snapshot_datetime
FROM edw_prod.data_model.fact_chargeback fc
    JOIN edw_prod.stg.dim_customer c ON fc.customer_id = c.customer_id
    JOIN edw_prod.stg.fact_order o ON fc.order_id = o.order_id
    JOIN lake_consolidated_view.ultra_merchant.address a ON a.address_id = o.billing_address_id
    JOIN edw_prod.stg.dim_store s ON s.store_id = fc.store_id
    JOIN lake_consolidated_view.ultra_merchant."ORDER" umo ON o.order_id= umo.order_id
    JOIN edw_prod.stg.dim_order_sales_channel dosc ON o.order_sales_channel_key=dosc.order_sales_channel_key
    JOIN edw_prod.stg.dim_order_status os ON os.order_status_key = o.order_status_key
WHERE dosc.order_classification_l1='Product Order'  AND os.order_status = 'Success' AND s.store_region = 'EU'
	AND lower(s.store_brand) IN ('justfab','fabletics', 'savage x')
	AND store_name NOT LIKE '%DM%'
	AND store_name NOT LIKE '%Sample Request%'
	AND store_name NOT LIKE '%Heels%'
	AND store_name NOT LIKE '%Wholesale%'
	AND store_name NOT LIKE '%Retail Replen%'
	AND store_name NOT LIKE '%SWAG%'
    AND NOT EXISTS (SELECT 1 FROM lake_consolidated.ultra_merchant.rma AS rma WHERE rma.order_id = o.order_id AND rma.statuscode IN (4670,4671))
    AND order_value != 0 AND chargeback_date >= '2018-06-12'
