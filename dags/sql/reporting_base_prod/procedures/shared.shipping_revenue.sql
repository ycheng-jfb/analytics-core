SET process_to_date = DATE_TRUNC(MONTH, CURRENT_DATE);

CREATE OR REPLACE TEMPORARY TABLE _store AS
SELECT *
FROM edw_prod.data_model.dim_store st
WHERE store_full_name NOT LIKE '%SWAG%'
  AND store_full_name NOT LIKE '%DM%'
  AND store_full_name NOT LIKE '%Sample Request%'
  AND store_full_name NOT LIKE '%Heels%'
  AND store_full_name NOT LIKE '%Wholesale%'
  AND store_full_name NOT LIKE '%Retail Replen%'
  AND store_brand NOT IN ('Not Applicable', 'Unknown');

CREATE OR REPLACE TRANSIENT TABLE _ps_orders AS
SELECT order_id
FROM lake_consolidated_view.ultra_merchant.order_classification
WHERE order_type_id IN (25, 26); -- box subscription & box exchange)

ALTER TABLE _ps_orders
    SET DATA_RETENTION_TIME_IN_DAYS = 0;

INSERT INTO _ps_orders
SELECT o.reship_order_id
FROM lake_consolidated_view.ultra_merchant.reship o
WHERE EXISTS(SELECT 1 FROM _ps_orders ps WHERE ps.order_id = o.original_order_id);

CREATE OR REPLACE TRANSIENT TABLE reporting_base_prod.shared.shipping_revenue_detail AS
SELECT st.store_brand || ' ' || st.store_country AS store,
       d.month_date                              AS shipped_month,
       o.order_id,
       o.shipping                                AS shipping_amt
FROM lake_consolidated_view.ultra_merchant."ORDER" o
         JOIN _store st ON st.store_id = o.store_id
         JOIN edw_prod.data_model.dim_date d ON d.full_date = CAST(o.date_shipped AS DATE)
    AND d.month_date < $process_to_date
WHERE NOT EXISTS(SELECT 1 FROM _ps_orders ps WHERE ps.order_id = o.order_id)
  AND st.store_name <> 'PS by JustFab';

ALTER TABLE reporting_base_prod.shared.shipping_revenue_detail
    SET DATA_RETENTION_TIME_IN_DAYS = 0;

INSERT INTO reporting_base_prod.shared.shipping_revenue_detail_snapshot
SELECT store
     , shipped_month
     , order_id
     , shipping_amt
     , CURRENT_TIMESTAMP AS snapshot_timestamp
FROM reporting_base_prod.shared.shipping_revenue_detail;

DELETE
FROM reporting_base_prod.shared.shipping_revenue_detail_snapshot
WHERE snapshot_timestamp < DATEADD(MONTH, -12, getdate());
