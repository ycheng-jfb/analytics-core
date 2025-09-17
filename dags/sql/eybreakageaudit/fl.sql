USE EYBreakageAudit;

SET start_date = '2023-01-01';

----order

CREATE OR REPLACE TRANSIENT TABLE audit.order_2023_fl AS
SELECT o.*, CURRENT_TIMESTAMP as table_loaded_datetime
FROM lake_fl.ultra_merchant."ORDER" o
WHERE store_id IN (
    SELECT store_id
    FROM edw_prod.data_model.dim_store
    WHERE store_brand_abbr IN ('FL', 'YTY')
)
AND datetime_added>=$start_date;

----membership_store_credit

CREATE OR REPLACE TRANSIENT TABLE audit.membership_store_credit_2023_fl AS
SELECT msc.*, CURRENT_TIMESTAMP as table_loaded_datetime
FROM lake_fl.ultra_merchant.membership_store_credit msc
JOIN lake_fl.ultra_merchant.store_credit sc ON sc.store_credit_id = msc.store_credit_id
JOIN lake_fl.ultra_merchant.customer c ON sc.customer_id=c.customer_id
WHERE store_id IN (
    SELECT store_id
    FROM edw_prod.data_model.dim_store
    WHERE store_brand_abbr IN ('FL', 'YTY')
)
AND msc.datetime_added>=$start_date;

----membership_credit

CREATE OR REPLACE TRANSIENT TABLE audit.membership_credit_2023_fl AS
SELECT sc.*, CURRENT_TIMESTAMP as table_loaded_datetime
FROM lake_fl.ultra_merchant.store_credit sc
JOIN lake_fl.ultra_merchant.store_credit_reason sr
ON sc.STORE_CREDIT_REASON_ID=sr.STORE_CREDIT_REASON_ID
JOIN lake_fl.ultra_merchant.customer cust
ON sc.customer_id=cust.customer_id
WHERE sr.label = 'Membership Credit'
    AND store_id IN (
        SELECT store_id
        FROM edw_prod.data_model.dim_store
        WHERE store_brand_abbr IN ('FL', 'YTY')
    )
    AND sc.datetime_added>=$start_date;

----issued_orders

CREATE OR REPLACE TRANSIENT TABLE audit.issued_orders_2023_fl AS
SELECT edw_prod.stg.udf_unconcat_brand(fo.order_id) AS order_id,
       edw_prod.stg.udf_unconcat_brand(da.address_id) AS address_id,
       street_address_1, city, state, country_code, CURRENT_TIMESTAMP AS table_loaded_datetime
FROM edw_prod.data_model.fact_order fo
JOIN edw_prod.data_model.dim_store ds ON ds.store_id = fo.store_id
LEFT JOIN edw_prod.data_model.dim_address da
ON COALESCE(fo.billing_address_id,fo.shipping_address_id)=da.address_id
WHERE ds.store_id IN (
    SELECT store_id
    FROM edw_prod.data_model.dim_store
    WHERE store_brand_abbr IN ('FL', 'YTY')
)
    AND fo.order_local_datetime>=$start_date
    AND fo.order_Status_key = 1;

----address

CREATE OR REPLACE TRANSIENT TABLE audit.address_2023_fl AS
SELECT a.address_id, a.customer_id, a.address1, a.city, a.state, a.country_code, CURRENT_TIMESTAMP as table_loaded_datetime
FROM lake_fl.ultra_merchant.address a
WHERE a.address_id
		IN (SELECT shipping_address_id
		FROM eybreakageaudit.audit.order_2023_fl)
	OR a.address_id
		IN (SELECT billing_address_id
		FROM eybreakageaudit.audit.order_2023_fl);
