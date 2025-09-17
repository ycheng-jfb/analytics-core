SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);

CREATE OR REPLACE TEMP TABLE _ecom_returns__pitney_bowes_invoices_stg AS
SELECT pb.parcel_id             pitney_bowes_tracking_number,
       rl.tracking_number       rma_label_tracking_number,
       pb.return_id,
       rl.rma_id,
       rma.meta_original_rma_id,
       SUM(pb.total)         AS return_cost,
       SUM(pb.billed_weight) AS weight
FROM lake_view.sharepoint.gsc_pitney_bowes_return_invoices pb
         LEFT JOIN lake_consolidated_view.ultra_merchant.rma_label rl
                   ON pb.parcel_id = rl.tracking_number
                       AND
                      pb.return_id = edw_prod.stg.udf_unconcat_brand(rl.rma_id)
         LEFT JOIN lake_consolidated_view.ultra_merchant.rma rma
                   ON rl.rma_id = rma.rma_id
WHERE source = 'OLD FORMAT'
  AND rma.meta_original_rma_id = pb.return_id
GROUP BY pb.parcel_id,
         rl.tracking_number,
         pb.return_id,
         rl.rma_id,
         rma.meta_original_rma_id
UNION
SELECT pb.tracking_number         pitney_bowes_tracking_number,
       rl.carrier_tracking_number rma_label_tracking_number,
       pb.return_id,
       rl.rma_id,
       rma.meta_original_rma_id,
       SUM(pb.total)         AS   return_cost,
       SUM(pb.billed_weight) AS   weight
FROM lake_view.sharepoint.gsc_pitney_bowes_return_invoices pb
         LEFT JOIN lake_consolidated_view.ultra_merchant.rma_label rl
                   ON pb.tracking_number = rl.carrier_tracking_number
                       AND
                      pb.return_id = edw_prod.stg.udf_unconcat_brand(rl.rma_id)
         LEFT JOIN lake_consolidated_view.ultra_merchant.rma rma
                   ON rl.rma_id = rma.rma_id
WHERE source = 'RECENT FORMAT'
  AND rma.meta_original_rma_id = pb.return_id
GROUP BY pb.tracking_number,
         rl.carrier_tracking_number,
         pb.return_id,
         rl.rma_id,
         rma.meta_original_rma_id;

CREATE OR REPLACE TEMP TABLE _ecom_returns__pitney_bowes_invoices AS
SELECT pitney_bowes_tracking_number,
       rma_id,
       meta_original_rma_id,
       SUM(return_cost) total_return_cost,
       SUM(weight)      total_billed_weight
FROM _ecom_returns__pitney_bowes_invoices_stg
GROUP BY pitney_bowes_tracking_number,
         rma_id,
         meta_original_rma_id;

CREATE OR REPLACE TEMP TABLE _ecom_returns__fedex_invoices AS
SELECT rl.tracking_number,
       f.delivery_date,
       rl.rma_id,
       rma.meta_original_rma_id,
       SUM(f.total)         AS total_return_cost,
       SUM(f.billed_weight) AS total_billed_weight
FROM lake_view.sharepoint.gsc_fedex_return_invoices f
         LEFT JOIN lake_consolidated_view.ultra_merchant.rma_label rl
                   ON f.tracking_number = rl.tracking_number
         LEFT JOIN lake_consolidated_view.ultra_merchant.rma rma
                   ON rl.rma_id = rma.rma_id
GROUP BY rl.tracking_number,
         f.delivery_date,
         rl.rma_id,
         rma.meta_original_rma_id;

CREATE OR REPLACE TEMP TABLE _ecom_returns__combined_returned_invoices AS
SELECT DISTINCT pitney_bowes_tracking_number AS tracking_number,
                NULL                         AS delivery_date,
                rma_id,
                meta_original_rma_id,
                total_return_cost,
                total_billed_weight,
                'PITNEY BOWES'               AS carrier_source
FROM _ecom_returns__pitney_bowes_invoices
UNION ALL
SELECT DISTINCT tracking_number,
                delivery_date,
                rma_id,
                meta_original_rma_id,
                total_return_cost,
                total_billed_weight,
                'FEDEX' AS carrier_source
FROM _ecom_returns__fedex_invoices;

CREATE OR REPLACE TEMP TABLE _ecom_returns__pitney_bowes_zipcodes AS
SELECT return_id AS meta_original_rma_id,
       origin_zip
FROM lake_view.sharepoint.gsc_pitney_bowes_return_invoices pb
         LEFT JOIN lake_consolidated_view.ultra_merchant.rma_label rl
                   ON pb.parcel_id = rl.tracking_number
                       AND pb.return_id = edw_prod.stg.udf_unconcat_brand(rl.rma_id)
         LEFT JOIN lake_consolidated_view.ultra_merchant.rma rma
                   ON rl.rma_id = rma.rma_id
WHERE source = 'OLD FORMAT'
  AND rma.meta_original_rma_id = pb.return_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY return_id ORDER BY invoice_date DESC) = 1

UNION

SELECT return_id AS meta_original_rma_id,
       origin_zip
FROM lake_view.sharepoint.gsc_pitney_bowes_return_invoices pb
         LEFT JOIN lake_consolidated_view.ultra_merchant.rma_label rl
                   ON pb.tracking_number = rl.carrier_tracking_number
                       AND pb.return_id = edw_prod.stg.udf_unconcat_brand(rl.rma_id)
         LEFT JOIN lake_consolidated_view.ultra_merchant.rma rma
                   ON rl.rma_id = rma.rma_id
WHERE source = 'RECENT FORMAT'
  AND rma.meta_original_rma_id = pb.return_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY return_id ORDER BY invoice_date DESC) = 1;

CREATE OR REPLACE TEMP TABLE _ecom_returns__fedex_zipcodes AS
SELECT rma.meta_original_rma_id,
       origin_zip
FROM lake_view.sharepoint.gsc_fedex_return_invoices f
         LEFT JOIN lake_consolidated_view.ultra_merchant.rma_label rl
                   ON f.tracking_number = rl.tracking_number
         LEFT JOIN lake_consolidated_view.ultra_merchant.rma rma
                   ON rl.rma_id = rma.rma_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY rma.meta_original_rma_id ORDER BY invoice_date DESC) = 1;

CREATE OR REPLACE TEMP TABLE _ecom_returns__origin_zipcodes AS
SELECT meta_original_rma_id,
       origin_zip
FROM _ecom_returns__fedex_zipcodes
UNION
SELECT meta_original_rma_id,
       origin_zip
FROM _ecom_returns__pitney_bowes_zipcodes;

CREATE OR REPLACE TEMP TABLE _ecom_returns__refund_line_base AS
SELECT ds.store_brand,
       ds.store_region,
       fol.lpn_code,
       frl.order_line_id,
       frl.refund_completion_local_datetime                   refund_date,
       SUM(frl.product_cash_refund_local_amount)              cash_refund,
       SUM(frl.product_cash_store_credit_refund_local_amount) credit_refund
FROM edw_prod.data_model.fact_refund_line frl
         LEFT JOIN edw_prod.data_model.fact_refund ref
                   ON ref.refund_id = frl.refund_id
         LEFT JOIN edw_prod.data_model.fact_return_line frll
                   ON frll.return_id = ref.return_id
                       AND frl.order_line_id = frll.order_line_id
                       AND frll.order_line_id != -1
         JOIN edw_prod.data_model.fact_order fo
              ON frl.order_id = fo.order_id
         JOIN edw_prod.data_model.dim_order_sales_channel dosc
              ON fo.order_sales_channel_key = dosc.order_sales_channel_key
         JOIN edw_prod.data_model.dim_store ds
              ON frl.store_id = ds.store_id
         JOIN edw_prod.data_model.dim_order_status dos
              ON dos.order_status_key = fo.order_status_key
         JOIN edw_prod.data_model.fact_order_line fol
              ON frl.order_line_id = fol.order_line_id
WHERE dosc.order_classification_l1 = 'Product Order'
  AND ds.store_region = 'NA'
  AND dos.order_status = 'Success'
  AND frl.refund_completion_local_datetime >= '2023-01-01'
GROUP BY ds.store_brand,
         ds.store_region,
         fol.lpn_code,
         frl.order_line_id,
         frl.refund_completion_local_datetime
QUALIFY ROW_NUMBER() OVER (PARTITION BY frl.order_line_id ORDER BY frl.refund_completion_local_datetime) = 1;

CREATE OR REPLACE TEMP TABLE _ecom_returns__na_ecom_stores AS
SELECT store_id,
       store_full_name,
       store_brand,
       store_type,
       store_country,
       store_region,
       CASE
           WHEN store_brand = 'FabKids' THEN 'FK'
           WHEN store_brand = 'Fabletics' THEN 'FL'
           WHEN store_brand = 'JustFab' THEN 'JF'
           WHEN store_brand = 'Savage X' THEN 'SX'
           WHEN store_brand = 'ShoeDazzle' THEN 'SD'
           WHEN store_brand = 'Yitty' THEN 'YTY'
           ELSE 'MM'
           END bu
FROM edw_prod.data_model.dim_store
WHERE store_region = 'NA'
  AND store_type NOT IN ('Retail', 'Group Order');


CREATE OR REPLACE TEMP TABLE _ecom_returns__product_base AS
SELECT fol.order_line_id,
       fol.product_id,
       dp.product_sku,
       dp.product_alias
FROM edw_prod.data_model.fact_order_line fol
         LEFT JOIN edw_prod.data_model.dim_product dp
                   ON fol.product_id = dp.product_id
WHERE YEAR(fol.order_completion_local_datetime) >= 2023;

CREATE OR REPLACE TEMP TABLE _ecom_returns__unit_per_rma AS
SELECT rma_id,
       COUNT(DISTINCT rma_line_id) unit_counts
FROM lake_consolidated_view.ultra_merchant.rma_line
GROUP BY rma_id;

TRUNCATE TABLE gsc.ecom_returns;

INSERT INTO gsc.ecom_returns (order_date,
                                             lpn_code,
                                             order_delivered_date,
                                             returned_date,
                                             order_id,
                                             order_line_id,
                                             ecom_store_id,
                                             rma_id,
                                             rma_creation_date,
                                             days_to_return,
                                             returned_before_30_days,
                                             returned_before_45_days,
                                             returned_before_90_days,
                                             returned_after_90_days,
                                             customer_paid_value,
                                             cash_refund,
                                             credit_refund,
                                             sku,
                                             po_number,
                                             po_first_cost,
                                             plm_style,
                                             show_room,
                                             business_unit,
                                             gender,
                                             class,
                                             category,
                                             vendor,
                                             factory,
                                             return_carrier,
                                             return_carrier_shipping_cost,
                                             return_tracking_number,
                                             return_reason,
                                             return_disposition,
                                             total_billed_weight,
                                             return_carrier_delivery_date,
                                             returned_fc_received_date,
                                             is_retail_return,
                                             is_exchange,
                                             return_location,
                                             origin_zip,
                                             meta_create_datetime,
                                             meta_update_datetime)
SELECT DISTINCT fol.order_completion_local_datetime                                  AS order_date,
                fol.lpn_code,
                fos.carrier_delivered_local_datetime                                 AS order_delivered_date,
                IFF(YEAR(frl.return_completion_local_datetime) = 9999, frl.rma_resolution_local_datetime,
                    frl.return_completion_local_datetime)                            AS returned_date,
                fol.order_id,
                fol.order_line_id,
                nas.store_id                                                         AS ecom_store_id,
                frl.rma_id,
                COALESCE(rl.datetime_added, rma.datetime_added, rma.datetime_resolved,
                         rlb.refund_date)                                            AS rma_creation_date,

                IFF(YEAR(returned_date) = 9999, rma_creation_date, returned_date)::DATE -
                order_date::DATE                                                        days_to_return,
                IFF(days_to_return BETWEEN 0 AND 29, 1, 0)                           AS returned_before_30_days,
                IFF(days_to_return BETWEEN 30 AND 44, 1, 0)                          AS returned_before_45_days,
                IFF(days_to_return BETWEEN 45 AND 89, 1, 0)                          AS returned_before_90_days,
                IFF(days_to_return >= 90, 1, 0)                                      AS returned_after_90_days,
                fol.product_gross_revenue_excl_shipping_local_amount                 AS customer_paid_value,
                rlb.cash_refund,
                rlb.credit_refund, -- we don't give money back on tariffs

                COALESCE(lpn.sku, pb.product_sku)                                    AS sku,
                lpn.po                                                               AS po_number,
                pdd.cost                                                             AS po_first_cost,
                pdd.plm_style,
                pdd.show_room,
                UPPER(TRIM(pdd.division_id))                                         AS business_unit,
                UPPER(TRIM(pdd.gender))                                              AS gender,
                UPPER(TRIM(pdd.centric_class))                                       AS class,
                UPPER(TRIM(pdd.centric_category))                                    AS category,
                UPPER(TRIM(pdd.vend_name))                                           AS vendor,
                UPPER(TRIM(pdd.official_factory_name))                               AS factory,

                IFF(CONTAINS(image_url, 'happyreturns') AND LEFT(rl.tracking_number, 2) = 'HR', 'HAPPY RETURNS',
                    cri.carrier_source)                                              AS return_carrier,
                IFF(CONTAINS(image_url, 'happyreturns') AND LEFT(rl.tracking_number, 2) = 'HR', hr.monthly_rate,
                    cri.total_return_cost /
                    upr.unit_counts)                                                 AS return_carrier_shipping_cost,
                COALESCE(cri.tracking_number, rl.tracking_number,
                         rl.carrier_tracking_number)                                 AS return_tracking_number,
                drr.return_reason,
                drc.return_disposition,
                cri.total_billed_weight,
                COALESCE(cri.delivery_date, rma.datetime_resolved)                   AS return_carrier_delivery_date,
                rma.datetime_resolved                                                AS returned_fc_received_date,
                IFF(ds.store_type = 'Retail', TRUE, FALSE)                           AS is_retail_return,
                frl.is_exchange,
                IFF(ds.store_type = 'Retail', ds.store_full_name, dw.warehouse_name) AS return_location,
                oz.origin_zip,
                $execution_start_time                                                AS meta_create_datetime,
                $execution_start_time                                                AS meta_update_datetime
FROM edw_prod.data_model.fact_return_line frl
         LEFT JOIN edw_prod.data_model.fact_order_line fol
                   ON frl.order_line_id = fol.order_line_id
         LEFT JOIN edw_prod.data_model.fact_order_shipment fos
                   ON fos.order_id = fol.order_id
         LEFT JOIN lake_consolidated_view.ultra_merchant.rma_label rl
                   ON frl.rma_id = rl.rma_id
         LEFT JOIN lake_consolidated_view.ultra_merchant.rma rma
                   ON rma.rma_id = rl.rma_id
         LEFT JOIN edw_prod.data_model.dim_return_reason drr
                   ON drr.return_reason_id = frl.return_reason_id
         LEFT JOIN edw_prod.data_model.dim_return_condition drc
                   ON drc.return_condition_key = frl.return_condition_key
         LEFT JOIN edw_prod.data_model.dim_store ds -- for retail return
                   ON frl.retail_return_store_id = ds.store_id
         LEFT JOIN reporting_prod.gsc.lpn_detail_dataset lpn
                   ON fol.lpn_code = lpn.lpn
         LEFT JOIN reporting_prod.gsc.po_detail_dataset pdd
                   ON lpn.po = pdd.po_number
                       AND lpn.sku = pdd.sku
         LEFT JOIN edw_prod.data_model.dim_warehouse dw
                   ON frl.warehouse_id = dw.warehouse_id
         LEFT JOIN _ecom_returns__refund_line_base rlb -- getting return credit/cash
                   ON rlb.order_line_id = frl.order_line_id
         LEFT JOIN _ecom_returns__combined_returned_invoices cri -- invoice data
                   ON rma.meta_original_rma_id = cri.meta_original_rma_id
         LEFT JOIN _ecom_returns__unit_per_rma upr
                   ON upr.rma_id = frl.rma_id
         LEFT JOIN _ecom_returns__origin_zipcodes oz -- origin zipcodes
                   ON rma.meta_original_rma_id = oz.meta_original_rma_id
         JOIN _ecom_returns__na_ecom_stores nas -- focus on NA ecom only
              ON fol.store_id = nas.store_id
         LEFT JOIN lake_view.sharepoint.gsc_happy_returns_monthly_rate hr
                   ON hr.year_month = DATE_TRUNC(MONTH, frl.return_completion_local_datetime)::DATE
                       AND nas.bu = hr.bu
         LEFT JOIN _ecom_returns__product_base pb -- we have a sku at least
                   ON pb.order_line_id = frl.order_line_id
WHERE YEAR(fol.order_completion_local_datetime) >= 2023;
