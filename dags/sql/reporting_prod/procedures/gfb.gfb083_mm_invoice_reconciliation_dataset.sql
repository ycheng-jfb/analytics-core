CREATE OR REPLACE TEMPORARY TABLE _order_status AS
SELECT gmi.order_id,
       fol.order_status_key,
       dos.order_status,
       fol.order_local_datetime
FROM lake_view.sharepoint.gfb_mm_monthly_invoice gmi
         LEFT JOIN edw_prod.data_model_jfb.fact_order_line fol
                   ON fol.order_id = gmi.order_id
         JOIN edw_prod.data_model_jfb.dim_order_status dos
              ON dos.order_status_key = fol.order_status_key
GROUP BY 1, 2, 3, 4;

CREATE OR REPLACE TEMPORARY TABLE _mm_product_detail AS
SELECT fol.order_id,
       IFF(MAX(IFF(dp.membership_brand_id IN (10, 11, 12, 13), 1, 0)) = 1, TRUE, FALSE) AS has_mm_product
FROM lake_view.sharepoint.gfb_mm_monthly_invoice gmi
         LEFT JOIN edw_prod.data_model_jfb.fact_order_line fol
                   ON gmi.order_id = fol.order_id
         JOIN edw_prod.data_model_jfb.dim_product dp
              ON dp.product_id = fol.product_id
GROUP BY fol.order_id;

CREATE OR REPLACE TEMPORARY TABLE _orders_with_multiple_invoices AS
SELECT order_id, COUNT(DISTINCT invoice_date) AS count_distinct_invoices
FROM lake_view.sharepoint.gfb_mm_monthly_invoice
GROUP BY order_id
HAVING COUNT(DISTINCT invoice_date) > 1;

CREATE OR REPLACE TEMP TABLE _mm_version_sku AS
SELECT DISTINCT tfg_vendor_sku AS mm_sku
FROM lake_view.centric.ed_sku
WHERE tfg_division = 'tfgDivision:MM'
  AND tfg_vendor_sku IS NOT NULL;

  --Transforming and loading raw data from sharepoint gfb_mm_monthly_invoice sheet
CREATE OR REPLACE TRANSIENT TABLE gfb.jfb_mm_monthly_invoice AS
SELECT gmi.invoice_date::DATE                  AS invoice_month,
       gmi.platform,
       gmi.site,
       gmi.store,
       gmi.region,
       gmi.order_id,
       gmi.tracking_number,
       gmi.sku,
       mvs.mm_sku,
       gmi.style,
       gmi.quantity,
       gmi.product_cost,
       gmi.outbound_freight,
       gmi.warehouse_handling_charge,
       gmi.unit_price,
       gmi.performance_fees                           AS performance_fees,
       gmi.total,
       os.order_status_key,
       os.order_status,
       mpd.has_mm_product,
       IFF(omi.count_distinct_invoices IS NULL, 0, 1) AS multiple_invoices
FROM lake_view.sharepoint.gfb_mm_monthly_invoice gmi
         LEFT JOIN _mm_version_sku mvs
                   ON gmi.sku = mvs.mm_sku
         LEFT JOIN _order_status os
                   ON gmi.order_id = os.order_id
         LEFT JOIN _mm_product_detail mpd
                   ON gmi.order_id = mpd.order_id
         LEFT JOIN _orders_with_multiple_invoices omi
                   ON omi.order_id = gmi.order_id;

CREATE OR REPLACE TEMPORARY TABLE _original_mm_invoice AS
SELECT order_id,
       region,
       DATE_TRUNC(MONTH, invoice_month)            AS invoice_month,
       SUM(total)                                  AS total_cost_amount,
       SUM(product_cost)                           AS product_cost,
       SUM(COALESCE(outbound_freight, 0))          AS outbound_freight_cost,
       SUM(COALESCE(warehouse_handling_charge, 0)) AS warehouse_handling_charge,
       SUM(quantity)                               AS unit_sales
FROM reporting_prod.gfb.jfb_mm_monthly_invoice
GROUP BY 1, 2, 3;

CREATE OR REPLACE TEMPORARY TABLE _confirmed_mm_invoice AS
SELECT fol.order_id,
       DATE_TRUNC(MONTH, fol.order_local_datetime::DATE)      AS invoice_month,
       SUM(item_quantity)                                     AS unit_sales,
       SUM((COALESCE(fol.reporting_landed_cost_local_amount, 0) *
            COALESCE(fol.order_date_usd_conversion_rate, 1))) AS product_cost
FROM edw_prod.data_model_jfb.fact_order_line fol
         JOIN edw_prod.data_model_jfb.dim_product dp
                   ON dp.product_id = fol.product_id
                       AND dp.membership_brand_id IN (10, 11, 12, 13)
WHERE DATE_TRUNC(MONTH, fol.order_local_datetime::DATE) >= '2024-08-01'
GROUP BY 1, 2;

CREATE OR REPLACE TEMPORARY TABLE _cancels_mm_invoice AS
SELECT fol.order_id,
       DATE_TRUNC(MONTH, fol.order_local_datetime::DATE)      AS invoice_month,
       SUM(item_quantity)                                     AS unit_sales,
       SUM((COALESCE(fol.reporting_landed_cost_local_amount, 0) *
            COALESCE(fol.order_date_usd_conversion_rate, 1))) AS product_cost
FROM edw_prod.data_model_jfb.fact_order_line fol
         LEFT JOIN edw_prod.data_model_jfb.dim_product dp
                   ON dp.product_id = fol.product_id
WHERE dp.membership_brand_id IN (10, 11, 12, 13)
  AND fol.order_status_key = 3
  AND DATE_TRUNC(MONTH, fol.order_local_datetime::DATE) >= '2024-08-01'
GROUP BY 1, 2;

CREATE OR REPLACE TEMPORARY TABLE _returns_mm_invoice AS
SELECT fol.order_id,
       DATE_TRUNC(MONTH, frl.return_completion_local_datetime::DATE) AS invoice_month,
       SUM(frl.return_item_quantity)                                 AS unit_sales,
       SUM((frl.estimated_returned_product_cost_local_amount_resaleable) *
           COALESCE(return_receipt_date_usd_conversion_rate, 1))     AS product_cost
FROM edw_prod.data_model_jfb.fact_return_line frl
         JOIN edw_prod.data_model_jfb.fact_order_line fol
              ON frl.order_line_id = fol.order_line_id
                  AND frl.return_completion_local_datetime >= '2024-08-01'
                  AND frl.return_completion_local_datetime <= DATE_TRUNC(MONTH, CURRENT_DATE)
         LEFT JOIN edw_prod.data_model_jfb.dim_product dp
                   ON dp.product_id = fol.product_id
WHERE dp.membership_brand_id IN (10, 11, 12, 13)
GROUP BY 1, 2;

CREATE OR REPLACE TEMPORARY TABLE _main AS
SELECT omi.invoice_month,
       omi.region,
       SUM(COALESCE(omi.unit_sales, 0))                AS original_unit_sales,
       SUM(COALESCE(cmi.unit_sales, 0))                AS confirmed_unit_sales,
       SUM(COALESCE(rmi.unit_sales, 0))                AS return_unit_sales,
       SUM(COALESCE(cami.unit_sales, 0))               AS cancel_unit_sales,
       SUM(COALESCE(omi.total_cost_amount, 0))         AS original_total_cost_amount,
       SUM(COALESCE(omi.product_cost, 0))              AS original_product_cost,
       SUM(COALESCE(cmi.product_cost, 0))              AS confirmed_product_cost,
       SUM(COALESCE(rmi.product_cost, 0))              AS return_product_cost,
       SUM(COALESCE(cami.product_cost, 0))             AS cancel_product_cost,
       SUM(COALESCE(omi.outbound_freight_cost, 0))     AS original_outbound_freight_cost,
       SUM(COALESCE(omi.warehouse_handling_charge, 0)) AS original_warehouse_handling_charge
FROM _original_mm_invoice omi
         JOIN _confirmed_mm_invoice cmi ON omi.order_id = cmi.order_id
         LEFT JOIN _returns_mm_invoice rmi ON omi.order_id = rmi.order_id
         LEFT JOIN _cancels_mm_invoice cami ON omi.order_id = cami.order_id
GROUP BY 1, 2;

CREATE OR REPLACE TRANSIENT TABLE gfb.gfb083_mm_invoice_reconciliation_dataset AS
SELECT invoice_month,
       region,
       'Original MM Invoice'                   AS invoice_rollup,
       SUM(original_total_cost_amount)         AS total_cost_amount,
       SUM(original_product_cost)              AS product_cost,
       SUM(original_outbound_freight_cost)     AS outbound_freight_cost,
       SUM(original_warehouse_handling_charge) AS warehouse_handling_charge,
       SUM(original_unit_sales)                AS unit_sales
FROM _main
GROUP BY 1, 2
UNION ALL
SELECT invoice_month,
       region,
       'Confirmed MM Invoice'      AS invoice_rollup,
       0                           AS total_cost_amount,
       SUM(confirmed_product_cost) AS product_cost,
       0                           AS outbound_freight_cost,
       0                           AS warehouse_handling_charge,
       SUM(confirmed_unit_sales)
FROM _main
GROUP BY 1, 2
UNION ALL
SELECT invoice_month,
       region,
       'Cancels'                AS invoice_rollup,
       0                        AS total_cost_amount,
       SUM(cancel_product_cost) AS product_cost,
       0                        AS outbound_freight_cost,
       0                        AS warehouse_handling_charge,
       SUM(cancel_unit_sales)   AS unit_sales
FROM _main
GROUP BY 1, 2
UNION ALL
SELECT invoice_month,
       region,
       'Returns'                AS invoice_rollup,
       0                        AS total_cost_amount,
       SUM(return_product_cost) AS product_cost,
       0                        AS outbound_freight_cost,
       0                        AS warehouse_handling_charge,
       SUM(return_unit_sales)   AS unit_sales
FROM _main
GROUP BY 1, 2
ORDER BY 1;
