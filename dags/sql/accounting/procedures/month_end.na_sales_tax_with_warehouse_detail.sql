SET process_to_date = DATE_TRUNC(MONTH, CURRENT_DATE); --to the report month
SET process_month_date = DATEADD(YEAR, -1, $process_to_date); -- a year before the report month
SET report_month = DATEADD(MONTH, -1, $process_to_date);

CREATE OR REPLACE TEMP TABLE _target_order AS
SELECT DISTINCT o.order_id
FROM lake_consolidated_view.ultra_merchant."ORDER" o
         JOIN lake_consolidated_view.ultra_merchant.store_classification sc ON sc.store_id = o.store_id
    AND sc.store_type_id <> 6
         JOIN edw_prod.data_model.dim_store lscm ON lscm.store_id = o.store_id
    AND lscm.store_region = 'NA'
WHERE o.date_shipped >= $process_month_date
  AND o.date_shipped < $process_to_date;

CREATE OR REPLACE TEMP TABLE _warehouse_info AS
SELECT DISTINCT ol.order_id, ol.warehouse_id, osl.order_shipment_id
FROM lake_consolidated_view.ultra_merchant.order_shipment_line osl
         JOIN lake_consolidated_view.ultra_merchant.order_line ol
              ON ol.order_line_id = osl.order_line_id
                  AND warehouse_id IS NOT NULL
         JOIN _target_order o ON o.order_id = ol.order_id;

CREATE OR REPLACE TEMP TABLE _order_shipment AS
SELECT a.order_id,
       a.order_shipment_id,
       a.warehouse_id,
       a.label,
       SUM(a.shipments) OVER (PARTITION BY a.order_id)                              AS total_shipments,
       CAST(a.shipments AS FLOAT) / SUM(a.shipments) OVER (PARTITION BY a.order_id) AS shipment_percent
FROM (SELECT info.order_id,
             info.order_shipment_id,
             w.warehouse_id,
             w.label,
             COUNT(info.order_shipment_id) AS shipments
      FROM _warehouse_info info
               JOIN lake_view.ultra_warehouse.warehouse w ON w.warehouse_id = info.warehouse_id
      --WHERE NOT EXISTS (SELECT 1 FROM ods.um.box b WHERE b.order_id = o.order_id)
      GROUP BY info.order_id,
               info.order_shipment_id,
               w.warehouse_id,
               w.label) a;

CREATE OR REPLACE TEMPORARY TABLE _na_sales_tax_with_warehouse_detail AS
SELECT 'SALES'                                                           segment,
       o.order_id,
       NULL         AS                                                   refund_id,
       IFF(o.store_id in (52,151,241), mb_st.store_name, st.store_name)  business_unit,
       UPPER(a.country_code)                                             country,
       CAST(NULL AS VARCHAR(100))                                        retail_location,
       d.month_date AS                                                   month,
       IFF(o.store_id in (52,151,241), mb_st.store_id, o.store_id)       store_id,
       st.store_type,                                                                     --st.store_type,
       a.state                                                           state,
       os.warehouse_id,
       label                                                             ship_from_warehouse_info,
       (o.subtotal * shipment_percent) - (o.discount * shipment_percent) product_dollars, -- collected sales
       o.shipping * shipment_percent                                     shipping,        -- collected_shipping
       o.tax * shipment_percent                                          tax,             -- collected_tax
       ((o.subtotal * shipment_percent) - (o.discount * shipment_percent)) + (o.tax * shipment_percent) +
       (o.shipping * shipment_percent)                                   total,           --total
       NULL                                                              refund_type
FROM lake_consolidated_view.ultra_merchant."ORDER" o
         JOIN edw_prod.data_model.dim_date d ON d.full_date = CAST(o.date_shipped AS DATE)
    AND d.month_date >= $process_month_date
    AND d.month_date < $process_to_date
    LEFT JOIN lake_consolidated_view.ultra_merchant.membership_plan_membership_brand AS mpmb
        ON mpmb.membership_brand_id = o.membership_brand_id
--JOIN edw_view.dbo.fact_order fo ON o.order_id = fo.order_id
         JOIN edw_prod.data_model.dim_store st ON st.store_id = o.store_id
    AND st.store_type IN ('Online', 'Mobile App')
    AND st.store_region = 'NA'
         LEFT JOIN edw_prod.data_model.dim_store mb_st ON mb_st.store_id = mpmb.store_id
    AND mb_st.store_type IN ('Online', 'Mobile App')
    AND mb_st.store_region = 'NA'
         JOIN _order_shipment os ON os.order_id = o.order_id
         JOIN lake_consolidated_view.ultra_merchant.address a ON o.shipping_address_id = a.address_id

-------------------------------------------------------------------
-- Online Refunds

UNION ALL

SELECT 'REFUNDS'                                                         segment,
       o.order_id,
       r.refund_id,
       IFF(o.store_id in (52,151,241), mb_st.store_name, st.store_name)  business_unit,
       UPPER(a.country_code)                                             country,
       CAST(NULL AS VARCHAR(100))                                        retail_location,
       d.month_date AS                                                   month,
       IFF(o.store_id in (52,151,241), mb_st.store_id, o.store_id)       store_id,
       st.store_type,
       a.state,
       os.warehouse_id,
       label                                                             ship_from_warehouse_info,
       (-1) * r.product_refund * os.shipment_percent                     product_dollars, -- refunded_product_dollars
       (-1) * r.shipping_refund * os.shipment_percent                    shipping,        -- refunded_shipping
       (-1) * r.tax_refund * os.shipment_percent                         tax,             -- refunded_tax
       (-1) * ((r.product_refund * os.shipment_percent) + (r.tax_refund * os.shipment_percent) +
               (r.shipping_refund * os.shipment_percent))                total,           -- refunded_total
       CASE
           WHEN r.payment_method = 'Check_request' THEN 'Check Request'
           WHEN r.payment_method = 'creditcard' THEN 'Credit Card'
           WHEN r.payment_method = 'moneyorder' THEN 'Money Order'
           WHEN r.payment_method = 'store_credit' THEN 'Store Credit'
           ELSE r.payment_method
           END                                                           refund_type
FROM lake_consolidated_view.ultra_merchant."ORDER" o
--JOIN edw_view.dbo.fact_order fo ON o.order_id = fo.order_id
         JOIN lake_consolidated_view.ultra_merchant.refund r ON o.order_id = r.order_id
         JOIN edw_prod.data_model.dim_date d ON d.full_date = CAST(r.datetime_refunded AS DATE)
    AND d.month_date >= $process_month_date
    AND d.month_date < $process_to_date
    LEFT JOIN lake_consolidated_view.ultra_merchant.membership_plan_membership_brand AS mpmb
        ON mpmb.membership_brand_id = o.membership_brand_id
         JOIN edw_prod.data_model.dim_store st ON st.store_id = o.store_id
    AND st.store_type IN ('Online', 'Mobile App')
    AND st.store_region = 'NA'
         LEFT JOIN edw_prod.data_model.dim_store mb_st ON mb_st.store_id = mpmb.store_id
    AND mb_st.store_type IN ('Online', 'Mobile App')
    AND mb_st.store_region = 'NA'
         JOIN _order_shipment os ON os.order_id = o.order_id
         JOIN lake_consolidated_view.ultra_merchant.address a ON o.shipping_address_id = a.address_id
WHERE o.date_shipped IS NOT NULL
  AND r.datetime_refunded IS NOT NULL

-----------------------------------------------------------------
-- RETAIL SALES
UNION ALL

SELECT 'SALES'                                        segment,
       o.order_id,
       NULL         AS                                refund_id,
       st.store_name                                  business_unit,
       UPPER(a.country_code)                          country,
       CASE
           WHEN st.store_retail_location = 'Kenwood Town Center' THEN 'Kenwood Towne Centre'
           WHEN st.store_retail_location = 'South Park' THEN 'SouthPark'
           ELSE st.store_retail_location
           END                                        retail_location,
       d.month_date AS                                month,
       o.store_id,
       st.store_type,
       st.store_retail_state                          state,
       NULL         AS                                warehouse_id,
       NULL         AS                                ship_from_warehouse_info,
       o.subtotal - o.discount                        product_dollars, -- collected sales
       o.shipping                                     shipping,        -- collected_shipping
       o.tax                                          tax,             -- collected_tax
       (o.subtotal - o.discount) + o.tax + o.shipping total,           --total
       NULL                                           refund_type
FROM lake_consolidated_view.ultra_merchant."ORDER" o
         JOIN edw_prod.data_model.dim_date d ON d.full_date = CAST(o.date_placed AS DATE)
    AND d.month_date >= $process_month_date
    AND d.month_date < $process_to_date
         JOIN edw_prod.data_model.dim_store st ON st.store_id = o.store_id
    AND st.store_type = 'Retail'
    AND st.store_region = 'NA'
         JOIN lake_consolidated_view.ultra_merchant.address a ON o.shipping_address_id = a.address_id
WHERE o.payment_statuscode >= 2600 -- make sure it's successful

-----------------------------------------------------------------
-- RETAIL REFUNDS

UNION ALL

SELECT 'REFUNDS'                                                  segment,
       o.order_id,
       r.refund_id,
       st.store_name                                              business_unit,   -- st.store_brand_name + ' ' + st.store_country_abbr,
       UPPER(a.country_code)                                      country,
       CASE
           WHEN st.store_retail_location = 'Kenwood Town Center' THEN 'Kenwood Towne Centre'
           WHEN st.store_retail_location = 'South Park' THEN 'SouthPark'
           ELSE st.store_retail_location
           END                                                    retail_location,
       d.month_date                                               month,
       o.store_id,
       st.store_type,
       st.store_retail_state                                      state,
       NULL    AS                                                 warehouse_id,
       NULL    AS                                                 ship_from_warehouse_info,
       (-1) * r.product_refund                                    product_dollars, -- refunded_product_dollars
       (-1) * r.shipping_refund                                   shipping,        -- refunded_shipping
       (-1) * r.tax_refund                                        tax,             -- refunded_tax
       (-1) * r.product_refund + r.tax_refund + r.shipping_refund total,           -- refunded_total
       CASE
           WHEN r.payment_method = 'Check_request' THEN 'Check Request'
           WHEN r.payment_method = 'creditcard' THEN 'Credit Card'
           WHEN r.payment_method = 'moneyorder' THEN 'Money Order'
           WHEN r.payment_method = 'store_credit' THEN 'Store Credit'
           ELSE r.payment_method
           END AS                                                 refund_type
FROM lake_consolidated_view.ultra_merchant."ORDER" o
         JOIN lake_consolidated_view.ultra_merchant.refund r ON o.order_id = r.order_id
    AND r.datetime_refunded IS NOT NULL
    AND r.payment_method <> 'store_credit'
         JOIN edw_prod.data_model.dim_date d ON d.full_date = CAST(r.datetime_refunded AS DATE)
    AND d.month_date >= $process_month_date
    AND d.month_date < $process_to_date
         JOIN edw_prod.data_model.dim_store st ON st.store_id = o.store_id
    AND st.store_type = 'Retail'
    AND st.store_region = 'NA'
         JOIN lake_consolidated_view.ultra_merchant.address a ON o.shipping_address_id = a.address_id;

CREATE OR REPLACE TABLE month_end.na_sales_tax_with_warehouse_detail AS
SELECT segment,
       order_id,
       refund_id,
       business_unit,
       month_end.udf_correct_state_country(state, country, 'country') AS country,
       retail_location,
       month,
       store_id,
       store_type,
       month_end.udf_correct_state_country(state, country, 'state')   AS state,
       warehouse_id,
       ship_from_warehouse_info,
       product_dollars,
       shipping,
       tax,
       total,
       refund_type
FROM _na_sales_tax_with_warehouse_detail;

DELETE
FROM month_end.na_sales_tax_with_warehouse_detail
WHERE product_dollars = 0
  AND shipping = 0
  AND tax = 0
  AND total = 0;

CREATE TRANSIENT TABLE IF NOT EXISTS month_end.na_sales_tax_with_warehouse_detail_snapshot (
	segment VARCHAR(7),
	order_id NUMBER(38,0),
	refund_id NUMBER(38,0),
	business_unit VARCHAR(50),
	country VARCHAR(6),
	retail_location VARCHAR(100),
	month DATE,
	store_id NUMBER(38,0),
	store_type VARCHAR(20),
	state VARCHAR(50),
	warehouse_id NUMBER(38,0),
	ship_from_warehouse_info VARCHAR(255),
	product_dollars FLOAT,
	shipping FLOAT,
	tax FLOAT,
	total FLOAT,
	refund_type VARCHAR(25),
	reporting_month VARCHAR(10),
	snapshot_runtime TIMESTAMP_LTZ(9)
);

INSERT INTO month_end.na_sales_tax_with_warehouse_detail_snapshot
SELECT segment,
       order_id,
       refund_id,
       business_unit,
       country,
       retail_location,
       month,
       store_id,
       store_type,
       state,
       warehouse_id,
       ship_from_warehouse_info,
       product_dollars,
       shipping,
       tax,
       total,
       refund_type,
       $report_month     reporting_month,
       CURRENT_TIMESTAMP snapshot_runtime
FROM month_end.na_sales_tax_with_warehouse_detail;

