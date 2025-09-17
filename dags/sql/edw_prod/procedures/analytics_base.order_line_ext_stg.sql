SET target_table = 'analytics_base.order_line_ext_stg';
SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
SET is_full_refresh = (SELECT IFF(stg.udf_get_watermark($target_table, NULL) = '1900-01-01'::TIMESTAMP_LTZ(3), TRUE, FALSE));
ALTER SESSION SET QUERY_TAG = $target_table;

/*
-- Initial Load / Full Refresh
UPDATE stg.meta_table_dependency_watermark
SET high_watermark_datetime = '1900-01-01',
    meta_update_datetime = CURRENT_TIMESTAMP()
WHERE table_name = $target_table;
SET is_full_refresh = TRUE;
*/

-- Handling Watermarks
MERGE INTO stg.meta_table_dependency_watermark AS w USING
    (SELECT $target_table                             AS table_name,
            t.dependent_table_name,
            IFF(t.dependent_table_name IS NOT NULL, t.new_high_watermark_datetime,
                (SELECT MAX(meta_update_datetime) AS new_high_watermark_datetime
                 FROM analytics_base.order_line_ext_stg)) AS new_high_watermark_datetime

     FROM (SELECT -- For self table
                  NULL AS dependent_table_name,
                  NULL AS new_high_watermark_datetime
           UNION ALL
           SELECT 'edw_prod.data_model.fact_order_line' AS dependent_table_name,
                  MAX(meta_update_datetime)        AS new_high_watermark_datetime
           FROM data_model.fact_order_line
           UNION ALL
           SELECT 'edw_prod.data_model.fact_order_line_discount' AS dependent_table_name,
                  MAX(meta_update_datetime)                 AS new_high_watermark_datetime
           FROM data_model.fact_order_line_discount
           UNION ALL
           SELECT 'edw_prod.data_model.dim_order_line_status' AS dependent_table_name,
                  MAX(meta_update_datetime)              AS new_high_watermark_datetime
           FROM data_model.dim_order_line_status
           UNION ALL
           SELECT 'lake_consolidated.ultra_merchant.promo' AS dependent_table_name,
                  MAX(meta_update_datetime)        AS new_high_watermark_datetime
           FROM lake_consolidated.ultra_merchant.promo
           UNION ALL
           SELECT 'lake_consolidated.ultra_merchant.reship' AS dependent_table_name,
                  MAX(meta_update_datetime)        AS new_high_watermark_datetime
           FROM lake_consolidated.ultra_merchant.reship
           UNION ALL
           SELECT 'lake_consolidated.ultra_merchant.exchange' AS dependent_table_name,
                  MAX(meta_update_datetime)        AS new_high_watermark_datetime
           FROM lake_consolidated.ultra_merchant.exchange

           UNION ALL
           SELECT 'edw_prod.data_model.dim_warehouse' AS dependent_table_name,
                  MAX(meta_update_datetime)      AS new_high_watermark_datetime
           FROM data_model.dim_warehouse
           UNION ALL
           SELECT 'edw_prod.data_model.dim_store' AS dependent_table_name,
                  MAX(meta_update_datetime)  AS new_high_watermark_datetime
           FROM data_model.dim_store
           UNION ALL
           SELECT 'edw_prod.data_model.dim_customer' AS dependent_table_name,
                  MAX(meta_update_datetime)     AS new_high_watermark_datetime
           FROM data_model.dim_customer
           UNION ALL
           SELECT 'edw_prod.data_model.dim_product' AS dependent_table_name,
                  MAX(meta_update_datetime)    AS new_high_watermark_datetime
           FROM data_model.dim_product
           UNION ALL
           SELECT 'edw_prod.data_model.dim_order_membership_classification' AS dependent_table_name,
                  MAX(meta_update_datetime)                            AS new_high_watermark_datetime
           FROM data_model.dim_order_membership_classification
           UNION ALL
           SELECT 'edw_prod.data_model.dim_order_sales_channel' AS dependent_table_name,
                  MAX(meta_update_datetime)                AS new_high_watermark_datetime
           FROM data_model.dim_order_sales_channel
           UNION ALL
           SELECT 'edw_prod.data_model.dim_product_type' AS dependent_table_name,
                  MAX(meta_update_datetime)         AS new_high_watermark_datetime
           FROM data_model.dim_product_type
           UNION ALL
           SELECT 'edw_prod.data_model.dim_product_price_history' AS dependent_table_name,
                  MAX(meta_update_datetime)                  AS new_high_watermark_datetime
           FROM data_model.dim_product_price_history
           UNION ALL
           SELECT 'edw_prod.data_model.dim_bundle_component_history' AS dependent_table_name,
                  MAX(meta_update_datetime)                     AS new_high_watermark_datetime
           FROM data_model.dim_bundle_component_history
           UNION ALL
           SELECT 'edw_prod.data_model.fact_order' AS dependent_table_name,
                  MAX(meta_update_datetime)   AS new_high_watermark_datetime
           FROM data_model.fact_order
           UNION ALL
           SELECT 'edw_prod.data_model.dim_order_status' AS dependent_table_name,
                  MAX(meta_update_datetime)         AS new_high_watermark_datetime
           FROM data_model.dim_order_status
           UNION ALL
           SELECT 'edw_prod.data_model.dim_order_processing_status' AS dependent_table_name,
                  MAX(meta_update_datetime)         AS new_high_watermark_datetime
           FROM data_model.dim_order_processing_status
           UNION ALL
           SELECT 'edw_prod.data_model.dim_return_status' AS dependent_table_name,
                  MAX(meta_update_datetime)          AS new_high_watermark_datetime
           FROM data_model.dim_return_status
           UNION ALL
           SELECT 'edw_prod.data_model.fact_return_line' AS dependent_table_name,
                  MAX(meta_update_datetime)         AS new_high_watermark_datetime
           FROM data_model.fact_return_line) AS t
     ORDER BY COALESCE(t.dependent_table_name, '')) AS s
    ON w.table_name = s.table_name AND w.dependent_table_name IS NOT DISTINCT FROM s.dependent_table_name
    WHEN NOT MATCHED THEN INSERT (table_name,
                                  dependent_table_name,
                                  high_watermark_datetime,
                                  new_high_watermark_datetime)
        VALUES (s.table_name,
                s.dependent_table_name,
                '1900-01-01', -- current high_watermark_datetime
                s.new_high_watermark_datetime)
    WHEN MATCHED AND w.new_high_watermark_datetime IS DISTINCT FROM s.new_high_watermark_datetime
        THEN UPDATE SET w.new_high_watermark_datetime = s.new_high_watermark_datetime, w.meta_update_datetime = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);

SET wm_self = (SELECT stg.udf_get_watermark($target_table, NULL));
SET wm_edw_data_model_fact_order_line = stg.udf_get_watermark($target_table, 'edw_prod.data_model.fact_order_line');
SET wm_edw_data_model_fact_order_line_discount = stg.udf_get_watermark($target_table, 'edw_prod.data_model.fact_order_line_discount');
SET wm_edw_data_model_fact_return_line = stg.udf_get_watermark($target_table, 'edw_prod.data_model.fact_return_line');
SET wm_edw_data_model_dim_order_line_status = stg.udf_get_watermark($target_table,'edw_prod.data_model.dim_order_line_status');
SET wm_lake_view_ultra_merchant_promo = stg.udf_get_watermark($target_table,'lake_consolidated.ultra_merchant.promo');
SET wm_lake_ultra_merchant_exchange = stg.udf_get_watermark($target_table,'lake_consolidated.ultra_merchant.exchange');
SET wm_lake_ultra_merchant_reship = stg.udf_get_watermark($target_table,'lake_consolidated.ultra_merchant.reship');

SET wm_edw_data_model_dim_warehouse = stg.udf_get_watermark($target_table,'edw_prod.data_model.dim_warehouse');
SET wm_edw_data_model_dim_store = stg.udf_get_watermark($target_table,'edw_prod.data_model.dim_store');
SET wm_edw_data_model_dim_customer = stg.udf_get_watermark($target_table,'edw_prod.data_model.dim_customer');
SET wm_edw_data_model_dim_product = stg.udf_get_watermark($target_table,'edw_prod.data_model.dim_product');
SET wm_edw_data_model_dim_order_membership_classification = stg.udf_get_watermark($target_table,'edw_prod.data_model.dim_order_membership_classification');
SET wm_edw_data_model_dim_order_sales_channel = stg.udf_get_watermark($target_table,'edw_prod.data_model.dim_order_sales_channel');
SET wm_edw_data_model_dim_product_type = stg.udf_get_watermark($target_table,'edw_prod.data_model.dim_product_type');
SET wm_edw_data_model_dim_product_price_history = stg.udf_get_watermark($target_table,'edw_prod.data_model.dim_product_price_history');
SET wm_edw_data_model_dim_bundle_component_history = stg.udf_get_watermark($target_table,'edw_prod.data_model.dim_bundle_component_history');
SET wm_edw_data_model_fact_order = stg.udf_get_watermark($target_table,'edw_prod.data_model.fact_order');
SET wm_edw_data_model_dim_order_status = stg.udf_get_watermark($target_table,'edw_prod.data_model.dim_order_status');
SET wm_edw_data_model_dim_order_processing_status = stg.udf_get_watermark($target_table,'edw_prod.data_model.dim_order_processing_status');
SET wm_edw_data_model_dim_return_status = stg.udf_get_watermark($target_table,'edw_prod.data_model.dim_return_status');

/*
SELECT
    $wm_self,
    $wm_edw_data_model_fact_order_line,
    $wm_edw_data_model_fact_order_line_discount,
    $wm_edw_data_model_fact_return_line,
    $wm_edw_data_model_dim_order_line_status,
    $wm_lake_view_ultra_merchant_promo,
    $wm_edw_data_model_dim_warehouse,
    $wm_edw_data_model_dim_store,
    $wm_edw_data_model_dim_customer,
    $wm_edw_data_model_dim_product,
    $wm_edw_data_model_dim_order_membership_classification,
    $wm_edw_data_model_dim_order_sales_channel,
    $wm_edw_data_model_dim_product_type,
    $wm_edw_data_model_dim_product_price_history,
    $wm_edw_data_model_dim_bundle_component_history,
    $wm_edw_data_model_fact_order,
    $wm_edw_data_model_dim_order_status,
    $wm_edw_data_model_dim_order_processing_status,
    $wm_edw_data_model_dim_return_status;
*/

--  Create base table
CREATE OR REPLACE TEMPORARY TABLE _order_line_ext__base (order_line_id INT);

-- Full Refresh
INSERT INTO _order_line_ext__base (order_line_id)
SELECT DISTINCT order_line_id
FROM data_model.fact_order_line
WHERE $is_full_refresh = TRUE
ORDER BY order_line_id;

-- Incremental
INSERT INTO _order_line_ext__base (order_line_id)
SELECT DISTINCT order_line_id
FROM (
      SELECT order_line_id
      FROM analytics_base.order_line_ext_stg
      WHERE meta_update_datetime > $wm_self
      UNION ALL
      -- Promo
      SELECT fold.order_line_id
      FROM data_model.fact_order_line_discount fold
               JOIN data_model.fact_order_line fol ON fol.order_line_id = fold.order_line_id
               JOIN data_model.dim_order_line_status dols
                    ON fol.order_line_status_key = dols.order_line_status_key AND dols.order_line_status != 'Cancelled'
               JOIN lake_consolidated.ultra_merchant.promo p ON p.promo_id = fold.promo_id
      WHERE fold.meta_update_datetime > $wm_edw_data_model_fact_order_line_discount
         OR fol.meta_update_datetime > $wm_edw_data_model_fact_order_line
         OR dols.meta_update_datetime > $wm_edw_data_model_dim_order_line_status
         OR p.meta_update_datetime > $wm_lake_view_ultra_merchant_promo
      UNION ALL
      SELECT ol.order_line_id
      FROM lake_consolidated.ultra_merchant.exchange e
      JOIN lake_consolidated.ultra_merchant.order_line ol
      ON e.exchange_order_id = ol.order_id
      WHERE e.meta_update_datetime > $wm_lake_ultra_merchant_exchange
      UNION ALL
      SELECT ol.order_line_id
      FROM lake_consolidated.ultra_merchant.reship r
      JOIN lake_consolidated.ultra_merchant.order_line ol
      ON r.reship_order_id = ol.order_id
      WHERE r.meta_update_datetime > $wm_lake_ultra_merchant_reship
      UNION ALL
      SELECT fol.order_line_id
      FROM data_model.fact_order_line fol
               LEFT JOIN data_model.dim_warehouse dw ON fol.warehouse_id = dw.warehouse_id
               JOIN data_model.dim_store ds ON fol.store_id = ds.store_id
               JOIN data_model.dim_customer dc ON fol.customer_id = dc.customer_id
               JOIN data_model.dim_product dp ON fol.product_id = dp.product_id
               LEFT JOIN data_model.dim_product dp_byo ON fol.bundle_product_id = dp_byo.product_id
               JOIN data_model.dim_order_line_status dols ON fol.order_line_status_key = dols.order_line_status_key
               JOIN data_model.dim_order_membership_classification domc
                    ON fol.order_membership_classification_key = domc.order_membership_classification_key
               JOIN data_model.dim_order_sales_channel dosc
                    ON fol.order_sales_channel_key = dosc.order_sales_channel_key
               JOIN data_model.dim_product_type dpt ON fol.product_type_key = dpt.product_type_key AND
                                                       LOWER(dpt.product_type_name) NOT IN
                                                       ('offer gift', 'gift certificate', 'membership gift',
                                                        'membership reward points item')
               JOIN data_model.dim_product_price_history dpph
                    ON fol.product_price_history_key = dpph.product_price_history_key
               LEFT JOIN data_model.dim_bundle_component_history dbch
                         ON fol.bundle_component_history_key = dbch.bundle_component_history_key
               JOIN data_model.fact_order fo ON fol.order_id = fo.order_id
               JOIN data_model.dim_order_status dos ON fo.order_status_key = dos.order_status_key
               JOIN data_model.dim_order_processing_status dops ON fo.order_processing_status_key = dops.order_processing_status_key
      WHERE fol.order_local_datetime :: DATE >= '2021-01-01'
        AND fol.order_completion_local_datetime :: DATE >= '2021-01-01'
        AND (NOT (ds.store_brand = 'Fabletics' AND dp.product_category = 'Savage'))
        AND (fol.meta_update_datetime > $wm_edw_data_model_fact_order_line
          OR dw.meta_update_datetime > $wm_edw_data_model_dim_warehouse
          OR ds.meta_update_datetime > $wm_edw_data_model_dim_store
          OR dc.meta_update_datetime > $wm_edw_data_model_dim_customer
          OR dp.meta_update_datetime > $wm_edw_data_model_dim_product
          OR dols.meta_update_datetime > $wm_edw_data_model_dim_order_line_status
          OR domc.meta_update_datetime > $wm_edw_data_model_dim_order_membership_classification
          OR dosc.meta_update_datetime > $wm_edw_data_model_dim_order_sales_channel
          OR dpt.meta_update_datetime > $wm_edw_data_model_dim_product_type
          OR dpph.meta_update_datetime > $wm_edw_data_model_dim_product_price_history
          OR dbch.meta_update_datetime > $wm_edw_data_model_dim_bundle_component_history
          OR fo.meta_update_datetime > $wm_edw_data_model_fact_order
          OR dos.meta_update_datetime > $wm_edw_data_model_dim_order_status
          OR dops.meta_update_datetime > $wm_edw_data_model_dim_order_processing_status)
      UNION ALL
      SELECT frl.order_line_id
      FROM data_model.fact_order_line fol
               JOIN data_model.fact_return_line frl ON fol.order_line_id = frl.order_line_id
               LEFT JOIN data_model.dim_warehouse dw
                         ON frl.warehouse_id = dw.warehouse_id AND LOWER(dw.warehouse_type) = 'we ship' AND
                            LOWER(dw.is_active) = 'true'
               JOIN data_model.dim_customer dc ON fol.customer_id = dc.customer_id
               JOIN data_model.dim_product dp ON frl.product_id = dp.product_id
               LEFT JOIN data_model.dim_product dp_byo ON fol.bundle_product_id = dp_byo.product_id
               JOIN data_model.fact_order fo ON fol.order_id = fo.order_id
               JOIN data_model.dim_order_status dos ON fo.order_status_key = dos.order_status_key
               JOIN data_model.dim_order_processing_status dops ON fo.order_processing_status_key = dops.order_processing_status_key
               JOIN data_model.dim_order_sales_channel dosc
                    ON fol.order_sales_channel_key = dosc.order_sales_channel_key
               JOIN data_model.dim_return_status drs ON frl.return_status_key = drs.return_status_key
               JOIN data_model.dim_product_type dpt ON fol.product_type_key = dpt.product_type_key AND
                                                       LOWER(dpt.product_type_name) NOT IN
                                                       ('offer gift', 'gift certificate', 'membership gift',
                                                        'membership reward points item')
               JOIN data_model.dim_order_membership_classification domc
                    ON fol.order_membership_classification_key = domc.order_membership_classification_key
               JOIN data_model.dim_store ds ON fol.store_id = ds.store_id
               JOIN data_model.dim_order_line_status dols ON fol.order_line_status_key = dols.order_line_status_key
               JOIN data_model.dim_product_price_history dpph
                    ON fol.product_price_history_key = dpph.product_price_history_key
               LEFT JOIN data_model.dim_bundle_component_history dbch
                         ON fol.bundle_component_history_key = dbch.bundle_component_history_key
      WHERE LOWER(drs.return_status) = 'resolved'
        AND LOWER(dosc.order_classification_l1) IN ('product order', 'reship', 'exchange')
        AND NOT (ds.store_brand = 'Fabletics' AND dp.product_category = 'Savage')
        AND frl.return_receipt_local_datetime :: DATE >= '2020-01-01'
        AND (fol.meta_update_datetime > $wm_edw_data_model_fact_order_line
          OR frl.meta_update_datetime > $wm_edw_data_model_fact_return_line
          OR dw.meta_update_datetime > $wm_edw_data_model_dim_warehouse
          OR ds.meta_update_datetime > $wm_edw_data_model_dim_store
          OR dc.meta_update_datetime > $wm_edw_data_model_dim_customer
          OR dp.meta_update_datetime > $wm_edw_data_model_dim_product
          OR domc.meta_update_datetime > $wm_edw_data_model_dim_order_membership_classification
          OR dosc.meta_update_datetime > $wm_edw_data_model_dim_order_sales_channel
          OR dols.meta_update_datetime > $wm_edw_data_model_dim_order_line_status
          OR dpt.meta_update_datetime > $wm_edw_data_model_dim_product_type
          OR dpph.meta_update_datetime > $wm_edw_data_model_dim_product_price_history
          OR dbch.meta_update_datetime > $wm_edw_data_model_dim_bundle_component_history
          OR fo.meta_update_datetime > $wm_edw_data_model_fact_order
          OR dos.meta_update_datetime > $wm_edw_data_model_dim_order_status
          OR dops.meta_update_datetime > $wm_edw_data_model_dim_order_processing_status
          OR drs.meta_update_datetime > $wm_edw_data_model_dim_return_status))
WHERE NOT $is_full_refresh
ORDER BY order_line_id;

CREATE OR REPLACE TEMP TABLE _order_line_ext__promo AS
SELECT LISTAGG(p.label, ' | ') WITHIN GROUP (ORDER BY fol.order_line_id, p.promo_id) AS promo,
       MIN(p.refunds_allowed)                                                        AS refunds_allowed,
       fol.order_line_id
FROM _order_line_ext__base base
         JOIN data_model.fact_order_line_discount fold ON base.order_line_id = fold.order_line_id
         JOIN data_model.fact_order_line fol ON fol.order_line_id = fold.order_line_id
         JOIN data_model.dim_order_line_status dols
              ON fol.order_line_status_key = dols.order_line_status_key AND dols.order_line_status != 'Cancelled'
         JOIN lake_consolidated.ultra_merchant.promo p ON p.promo_id = fold.promo_id
GROUP BY fol.order_line_id;

CREATE OR REPLACE TEMP TABLE _order_line_ext__base_stg AS
SELECT
    fol.order_line_id,
    edw_prod.stg.udf_unconcat_brand(fol.order_line_id) AS meta_original_order_line_id,
    fol.order_id,
    fol.order_local_datetime :: DATE AS placed_date,
    fol.order_completion_local_datetime :: DATE AS order_completion_date,
    fol.shipped_local_datetime :: DATE AS order_shipped_date,
    fol.warehouse_id,
    fol.product_id,
    fol.customer_id,
    fol.order_line_status_key,
    fol.order_membership_classification_key,
    fol.order_sales_channel_key,
    fol.item_quantity,
    IFF(fol.bundle_product_id != -1, fol.bundle_product_id, NULL) AS bundle_product_id,
    fol.subtotal_excl_tariff_local_amount,
    fol.tariff_revenue_local_amount,
    fol.token_count,
    fol.product_gross_revenue_excl_shipping_local_amount,
    fol.product_margin_pre_return_excl_shipping_local_amount,
    pc.reporting_landed_cost_local_amount,
    fol.product_discount_local_amount,
    fo.order_status_key,
    fo.order_processing_status_key,
    fo.order_date_usd_conversion_rate,
    fo.bops_store_id,
    fol.bundle_component_history_key,
    dpt.product_type_name,
    dpt.is_free,
    dpph.vip_unit_price,
    dpph.retail_unit_price,
    ds.store_id,
    ds.store_name,
    ds.store_full_name,
    ds.store_brand,
    ds.store_type,
    ds.store_region,
    ds.store_country,
    dp.master_product_id,
    dp.base_sku,
    dp.sku,
    dp.product_sku,
    dp.product_name,
    dp.product_alias,
    dp.size,
    dp.color,
    dp.department,
    dp.current_showroom_date,
    dp.product_type,
    dp.product_category,
    dp.store_id AS product_store_id,
    dc.gender,
    dc.is_cross_promo,
    dc.finance_specialty_store,
    domc.membership_order_type_l1,
    domc.membership_order_type_l2,
    domc.membership_order_type_l3,
    domc.is_vip,
    dosc.order_sales_channel_l1,
    dosc.order_sales_channel_l2,
    dosc.order_classification_l1,
    dosc.is_retail_ship_only_order,
    dosc.is_bops_order,
    dols.order_line_status,
    dp_byo.product_type AS byo_product_type
FROM _order_line_ext__base base
    JOIN data_model.fact_order_line fol
         ON base.order_line_id = fol.order_line_id
    LEFT JOIN data_model.fact_order_line_product_cost pc
         ON fol.order_line_id = pc.order_line_id
    JOIN data_model.fact_order fo
         ON fol.order_id = fo.order_id
    JOIN data_model.dim_product dp
         ON fol.product_id = dp.product_id
    JOIN data_model.dim_product_price_history dpph
         ON fol.product_price_history_key = dpph.product_price_history_key
    JOIN data_model.dim_store ds
         ON fol.store_id = ds.store_id
    JOIN data_model.dim_customer dc
         ON fol.customer_id = dc.customer_id
    JOIN data_model.dim_order_line_status dols
         ON fol.order_line_status_key = dols.order_line_status_key
    JOIN data_model.dim_order_membership_classification domc
         ON fo.order_membership_classification_key = domc.order_membership_classification_key
    JOIN data_model.dim_order_sales_channel dosc
         ON fo.order_sales_channel_key = dosc.order_sales_channel_key
    JOIN data_model.dim_product_type dpt
         ON fol.product_type_key = dpt.product_type_key AND
            LOWER(dpt.product_type_name) NOT IN ('offer gift', 'gift certificate', 'membership gift',
                                                 'membership reward points item')
    LEFT JOIN data_model.dim_product dp_byo
              ON fol.bundle_product_id = dp_byo.product_id;

CREATE OR REPLACE TEMP TABLE _order_line_ext__basic_sales AS
SELECT base.order_line_id,
       base.meta_original_order_line_id,
       base.order_id,
       base.placed_date,
       base.order_completion_date,
       base.order_shipped_date,
       dos.order_status,
       dops.order_processing_status,
       base.order_line_status,
       base.store_id,
       base.store_name,
       base.store_full_name,
       base.store_brand,
       base.store_type,
       base.store_region,
       base.store_country,
       base.warehouse_id                                                AS fulfillment_warehouse_id,
       dw.warehouse_code,
       dw.warehouse_name,
       dw.warehouse_type,
       dw.carrier_service,
       base.bops_store_id                                               AS pickup_store_id,
       base.product_id,
       IFF(base.master_product_id != -1, base.master_product_id,
           base.product_id)                                             AS master_product_id,
       base.sku,
       base.product_sku,
       base.base_sku,
       base.product_name,
       IFF(base.bundle_product_id != -1, base.bundle_product_id, NULL)  AS bundle_product_id,
       base.product_alias                                               AS bundle_alias,
       CASE WHEN LOWER(base.byo_product_type) = 'byo' THEN 1 ELSE 0 END AS is_byo_component,
       CASE
           WHEN LOWER(base.product_type_name) = 'bundle component' THEN 1
           ELSE 0 END                                                   AS is_bundle_component,
       CASE
           WHEN LOWER(base.product_name) != 'not applicable' AND
                (LOWER(base.product_name) NOT LIKE '%box%' OR LOWER(base.product_name) LIKE '%set%') AND
                LOWER(base.product_type) != 'byo' THEN 1
           ELSE 0 END                                                   AS is_pre_made_set,
       CASE
           WHEN LOWER(base.product_name) LIKE '%box%' AND LOWER(base.product_name) NOT LIKE '%set%' THEN 1
           ELSE 0 END                                                   AS is_vip_box,
       base.is_free,
       base.color,
       base.size,
       IFF(LOWER(base.department) = 'mens', 'mens', 'womens')           AS product_gender,
       base.membership_order_type_l1,
       base.membership_order_type_l2,
       base.membership_order_type_l3,
       base.order_sales_channel_l1,
       base.order_sales_channel_l2,
       base.order_classification_l1,
       base.is_retail_ship_only_order,
       base.is_bops_order,
       base.customer_id,
       base.gender                                                      AS customer_gender,
       base.is_cross_promo,
       base.finance_specialty_store,
       IFF(LOWER(base.is_vip) = 'true', 'vip', 'retail')                AS price_sold_as_type,
       0 :: NUMBER(38,2)                                                AS first_item_vip_price,
       0 :: NUMBER(38,2)                                                AS current_item_vip_price,
       COALESCE(base.vip_unit_price, 0) *
       COALESCE(base.order_date_usd_conversion_rate, 1)                 AS item_vip_price,
       COALESCE(base.retail_unit_price, 0) *
       COALESCE(base.order_date_usd_conversion_rate, 1)                 AS item_retail_price,
       COALESCE(base.item_quantity, 0) * COALESCE(base.vip_unit_price, 0) *
       COALESCE(base.order_date_usd_conversion_rate, 1)                 AS non_reduced_vip_price,
       COALESCE(base.item_quantity, 0)                                  AS unit_count,
       ZEROIFNULL(IFF(p.refunds_allowed = 0, 1, 0))                     AS finalsales_quantity,
       COALESCE(base.subtotal_excl_tariff_local_amount, 0) *
       COALESCE(base.order_date_usd_conversion_rate, 1)                 AS subtotal_excl_tariff,
       COALESCE(base.tariff_revenue_local_amount, 0) *
       COALESCE(base.order_date_usd_conversion_rate, 1)                 AS tariff_revenue,
       CASE
           WHEN base.token_count > 0 THEN COALESCE(base.subtotal_excl_tariff_local_amount, 0) *
                                          COALESCE(base.order_date_usd_conversion_rate, 1)
           ELSE 0 END                                                   AS token_subtotal,
       COALESCE(base.product_gross_revenue_excl_shipping_local_amount, 0) *
       COALESCE(base.order_date_usd_conversion_rate, 1)                 AS product_gross_revenue_excl_shipping,
       COALESCE(base.product_margin_pre_return_excl_shipping_local_amount, 0) *
       COALESCE(base.order_date_usd_conversion_rate, 1)                 AS product_margin_pre_return_excl_shipping,
       COALESCE(base.reporting_landed_cost_local_amount, 0) *
       COALESCE(base.order_date_usd_conversion_rate, 1)                 AS product_cost,
       CASE
           WHEN base.token_count > 0 THEN COALESCE(base.reporting_landed_cost_local_amount, 0) *
                                          COALESCE(base.order_date_usd_conversion_rate, 1)
           ELSE 0 END                                                   AS token_product_cost,
       COALESCE(base.token_count, 0)                                    AS token_count,
       COALESCE(base.product_discount_local_amount, 0) *
       COALESCE(base.order_date_usd_conversion_rate, 1)                 AS product_discount,
       CASE
           WHEN base.token_count > 0 THEN COALESCE(base.product_discount_local_amount, 0) *
                                          COALESCE(base.order_date_usd_conversion_rate, 1)
           ELSE 0 END                                                   AS token_discount
FROM _order_line_ext__base_stg base
         JOIN data_model.dim_order_status dos ON base.order_status_key = dos.order_status_key
         JOIN data_model.dim_order_processing_status dops ON base.order_processing_status_key = dops.order_processing_status_key
         LEFT JOIN data_model.dim_warehouse dw ON base.warehouse_id = dw.warehouse_id
         LEFT JOIN data_model.dim_bundle_component_history dbch
                   ON base.bundle_component_history_key = dbch.bundle_component_history_key
         LEFT JOIN _order_line_ext__promo p ON base.order_line_id = p.order_line_id
WHERE base.placed_date >= '2021-01-01'
  AND base.order_completion_date >= '2021-01-01'
  AND (NOT (base.store_brand = 'Fabletics' AND base.product_category = 'Savage'));

CREATE OR REPLACE TEMP TABLE _order_line_ext__base_price AS
SELECT DISTINCT sku, product_store_id
FROM _order_line_ext__base_stg;

CREATE OR REPLACE TEMP TABLE _order_line_ext__price AS
SELECT DISTINCT
       ds.store_id,
       dp.sku,
       ds.store_country AS country,
       COALESCE(FIRST_VALUE(dp.vip_unit_price)
           OVER (PARTITION BY dp.sku, ds.store_brand, ds.store_country ORDER BY dp.current_showroom_date, dp.vip_unit_price DESC), 0) AS first_item_vip_price,
       COALESCE(FIRST_VALUE(dp.vip_unit_price)
           OVER (PARTITION BY dp.sku, ds.store_brand, ds.store_country ORDER BY dp.current_showroom_date DESC, dp.vip_unit_price DESC), 0) AS current_item_vip_price
FROM _order_line_ext__base_price base
    JOIN data_model.dim_product dp
    ON base.sku = dp.sku AND base.product_store_id = dp.store_id
    JOIN data_model.dim_store ds
         ON ds.store_id = dp.store_id;

UPDATE _order_line_ext__basic_sales a
SET a.first_item_vip_price = b.first_item_vip_price,
    a.current_item_vip_price = b.current_item_vip_price
FROM _order_line_ext__price b
WHERE a.sku = b.sku AND a.store_id = b.store_id;

CREATE OR REPLACE TEMP TABLE _order_line_ext__product_order AS
SELECT 'product order'       AS object_type,
       order_line_id,
       meta_original_order_line_id,
       order_id,
       NULL                  AS original_order_id,
       placed_date,
       order_completion_date,
       order_shipped_date,
       NULL                  AS reship_placed_date,
       NULL                  AS exchange_placed_date,
       NULL                  AS return_date,
       order_status,
       order_processing_status,
       order_line_status,
       store_id,
       store_name,
       store_full_name,
       store_brand,
       store_type,
       store_region,
       store_country,
       fulfillment_warehouse_id,
       warehouse_code,
       warehouse_name,
       warehouse_type,
       carrier_service,
       pickup_store_id,
       product_id,
       master_product_id,
       sku,
       product_sku,
       base_sku,
       product_name,
       bundle_product_id,
       bundle_alias,
       is_byo_component,
       is_bundle_component,
       is_pre_made_set,
       is_vip_box,
       is_free,
       color,
       size,
       product_gender,
       membership_order_type_l1,
       membership_order_type_l2,
       membership_order_type_l3,
       order_sales_channel_l1,
       order_sales_channel_l2,
       order_classification_l1,
       is_retail_ship_only_order,
       is_bops_order,
       customer_id,
       customer_gender,
       is_cross_promo,
       finance_specialty_store,
       price_sold_as_type,
       first_item_vip_price,
       current_item_vip_price,
       item_vip_price,
       item_retail_price,
       non_reduced_vip_price,
       unit_count,
       0                     AS reship_unit_count,
       0                     AS exchange_unit_count,
       finalsales_quantity,
       token_count,
       subtotal_excl_tariff,
       tariff_revenue,
       token_subtotal,
       product_gross_revenue_excl_shipping,
       product_margin_pre_return_excl_shipping,
       product_cost,
       0                     AS reship_product_cost,
       0                     AS exchange_product_cost,
       token_product_cost,
       product_discount,
       token_discount,
       0                     AS return_unit_count,
       0                     AS return_product_gross_revenue_excl_shipping,
       0                     AS return_product_cost_returned_resalable
FROM _order_line_ext__basic_sales
WHERE LOWER(order_classification_l1) = 'product order'
  AND ( LOWER(order_status) = 'success'
  OR (LOWER(order_status) = 'pending' AND LOWER(order_processing_status) IN ('placed','fulfillment (batching)','fulfillment (in progress)')) )
  AND LOWER(order_line_status) != 'cancelled';

CREATE OR REPLACE TEMP TABLE _order_line_ext__reship AS
SELECT 'reship'              AS object_type,
       bs.order_line_id,
       bs.meta_original_order_line_id,
       bs.order_id,
       r.original_order_id   AS original_order_id,
       NULL                  AS placed_date,
       NULL                  AS order_completion_date,
       NULL                  AS order_shipped_date,
       bs.placed_date        AS reship_placed_date,
       NULL                  AS exchange_placed_date,
       NULL                  AS return_date,
       bs.order_status,
       bs.order_processing_status,
       bs.order_line_status,
       bs.store_id,
       bs.store_name,
       bs.store_full_name,
       bs.store_brand,
       bs.store_type,
       bs.store_region,
       bs.store_country,
       bs.fulfillment_warehouse_id,
       bs.warehouse_code,
       bs.warehouse_name,
       bs.warehouse_type,
       bs.carrier_service,
       bs.pickup_store_id,
       bs.product_id,
       bs.master_product_id,
       bs.sku,
       bs.product_sku,
       bs.base_sku,
       bs.product_name,
       bs.bundle_product_id,
       bs.bundle_alias,
       bs.is_byo_component,
       bs.is_bundle_component,
       bs.is_pre_made_set,
       bs.is_vip_box,
       bs.is_free,
       bs.color,
       bs.size,
       bs.product_gender,
       bs.membership_order_type_l1,
       bs.membership_order_type_l2,
       bs.membership_order_type_l3,
       bs.order_sales_channel_l1,
       bs.order_sales_channel_l2,
       bs.order_classification_l1,
       bs.is_retail_ship_only_order,
       bs.is_bops_order,
       bs.customer_id,
       bs.customer_gender,
       bs.is_cross_promo,
       bs.finance_specialty_store,
       bs.price_sold_as_type,
       bs.first_item_vip_price,
       bs.current_item_vip_price,
       bs.item_vip_price,
       bs.item_retail_price,
       bs.non_reduced_vip_price,
       0                     AS unit_count,
       bs.unit_count         AS reship_unit_count,
       0                     AS exchange_unit_count,
       bs.finalsales_quantity,
       bs.token_count,
       bs.subtotal_excl_tariff,
       bs.tariff_revenue,
       bs.token_subtotal,
       bs.product_gross_revenue_excl_shipping,
       bs.product_margin_pre_return_excl_shipping,
       0                     AS product_cost,
       bs.product_cost       AS reship_product_cost,
       0                     AS exchange_product_cost,
       bs.token_product_cost,
       bs.product_discount,
       bs.token_discount,
       0                     AS return_unit_count,
       0                     AS return_product_gross_revenue_excl_shipping,
       0                     AS return_product_cost_returned_resalable
FROM _order_line_ext__basic_sales bs
         JOIN lake_consolidated.ultra_merchant.reship r ON bs.order_id = r.reship_order_id
WHERE LOWER(bs.order_classification_l1) = 'reship'
  AND LOWER(bs.order_status) = 'success';

CREATE OR REPLACE TEMP TABLE _order_line_ext__exchange AS
SELECT 'exchange'            AS object_type,
       bse.order_line_id,
       bse.meta_original_order_line_id,
       bse.order_id,
       e.original_order_id   AS original_order_id,
       NULL                  AS placed_date,
       NULL                  AS order_completion_date,
       NULL                  AS order_shipped_date,
       NULL                  AS reship_placed_date,
       bse.placed_date       AS exchange_placed_date,
       NULL                  AS return_date,
       bse.order_status,
       bse.order_processing_status,
       bse.order_line_status,
       bse.store_id,
       bse.store_name,
       bse.store_full_name,
       bse.store_brand,
       bse.store_type,
       bse.store_region,
       bse.store_country,
       bse.fulfillment_warehouse_id,
       bse.warehouse_code,
       bse.warehouse_name,
       bse.warehouse_type,
       bse.carrier_service,
       bse.pickup_store_id,
       bse.product_id,
       bse.master_product_id,
       bse.sku,
       bse.product_sku,
       bse.base_sku,
       bse.product_name,
       bse.bundle_product_id,
       bse.bundle_alias,
       bse.is_byo_component,
       bse.is_bundle_component,
       bse.is_pre_made_set,
       bse.is_vip_box,
       bse.is_free,
       bse.color,
       bse.size,
       bse.product_gender,
       bse.membership_order_type_l1,
       bse.membership_order_type_l2,
       bse.membership_order_type_l3,
       bse.order_sales_channel_l1,
       bse.order_sales_channel_l2,
       bse.order_classification_l1,
       bse.is_retail_ship_only_order,
       bse.is_bops_order,
       bse.customer_id,
       bse.customer_gender,
       bse.is_cross_promo,
       bse.finance_specialty_store,
       bse.price_sold_as_type,
       bse.first_item_vip_price,
       bse.current_item_vip_price,
       bse.item_vip_price,
       bse.item_retail_price,
       bse.non_reduced_vip_price,
       0                     AS unit_count,
       0                     AS reship_unit_count,
       bse.unit_count        AS exchange_unit_count,
       bse.finalsales_quantity,
       bse.token_count,
       bse.subtotal_excl_tariff,
       bse.tariff_revenue,
       bse.token_subtotal,
       bse.product_gross_revenue_excl_shipping,
       bse.product_margin_pre_return_excl_shipping,
       0                     AS product_cost,
       0                     AS reship_product_cost,
       bse.product_cost      AS exchange_product_cost,
       bse.token_product_cost,
       bse.product_discount,
       bse.token_discount,
       0                     AS return_unit_count,
       0                     AS return_product_gross_revenue_excl_shipping,
       0                     AS return_product_cost_returned_resalable
FROM _order_line_ext__basic_sales bse
         JOIN lake_consolidated.ultra_merchant.exchange e ON bse.order_id = e.exchange_order_id
WHERE LOWER(bse.order_classification_l1) = 'exchange'
  AND LOWER(bse.order_status) = 'success';

CREATE OR REPLACE TEMP TABLE _order_line_ext__return AS
SELECT 'return'                                                         AS object_type,
       base.order_line_id,
       base.meta_original_order_line_id,
       base.order_id,
       NULL                                                             AS original_order_id,
       NULL                                                             AS placed_date,
       NULL                                                             AS order_completion_date,
       NULL                                                             AS order_shipped_date,
       NULL                                                             AS reship_placed_date,
       NULL                                                             AS exchange_placed_date,
       frl.return_receipt_local_datetime :: DATE                        AS return_date,
       dos.order_status,
       dops.order_processing_status,
       base.order_line_status,
       base.store_id,
       base.store_name,
       base.store_full_name,
       base.store_brand,
       base.store_type,
       base.store_region,
       base.store_country,
       frl.warehouse_id                                                 AS fulfillment_warehouse_id,
       dw.warehouse_code,
       dw.warehouse_name,
       dw.warehouse_type,
       dw.carrier_service,
       base.bops_store_id                                               AS pickup_store_id,
       frl.product_id,
       IFF(base.master_product_id != -1, base.master_product_id,
           base.product_id)                                             AS master_product_id,
       base.sku,
       base.product_sku,
       base.base_sku,
       base.product_name,
       0                                                                AS bundle_product_id,
       base.product_alias                                               AS bundle_alias,
       CASE WHEN LOWER(base.byo_product_type) = 'byo' THEN 1 ELSE 0 END AS is_byo_component,
       CASE
           WHEN LOWER(base.product_type_name) = 'bundle component' THEN 1
           ELSE 0 END                                                   AS is_bundle_component,
       CASE
           WHEN LOWER(base.product_name) != 'not applicable' AND
                (LOWER(base.product_name) NOT LIKE '%box%' OR LOWER(base.product_name) LIKE '%set%') AND
                LOWER(base.product_type) != 'byo' THEN 1
           ELSE 0 END                                                   AS is_pre_made_set,
       CASE
           WHEN LOWER(base.product_name) LIKE '%box%' AND LOWER(base.product_name) NOT LIKE '%set%' THEN 1
           ELSE 0 END                                                   AS is_vip_box,
       base.is_free,
       base.color,
       base.size,
       IFF(LOWER(base.department) = 'mens', 'mens', 'womens')           AS product_gender,
       base.membership_order_type_l1,
       base.membership_order_type_l2,
       base.membership_order_type_l3,
       base.order_sales_channel_l1,
       base.order_sales_channel_l2,
       base.order_classification_l1,
       base.is_retail_ship_only_order,
       base.is_bops_order,
       base.customer_id,
       base.gender                                                      AS customer_gender,
       base.is_cross_promo,
       base.finance_specialty_store,
       IFF(LOWER(base.is_vip) = 'true', 'vip', 'retail')                AS price_sold_as_type,
       0                                                                AS first_item_vip_price,
       0                                                                AS current_item_vip_price,
       COALESCE(base.vip_unit_price, 0)                                 AS item_vip_price,
       COALESCE(base.retail_unit_price, 0)                              AS item_retail_price,
       0                                                                AS non_reduced_vip_price,
       0                                                                AS unit_count,
       0                                                                AS reship_unit_count,
       0                                                                AS exchange_unit_count,
       0                                                                AS finalsales_quantity,
       0                                                                AS token_count,
       0                                                                AS subtotal_excl_tariff,
       0                                                                AS tariff_revenue,
       0                                                                AS token_subtotal,
       0                                                                AS product_gross_revenue_excl_shipping,
       0                                                                AS product_margin_pre_return_excl_shipping,
       0                                                                AS product_cost,
       0                                                                AS reship_product_cost,
       0                                                                AS exchange_product_cost,
       0                                                                AS token_product_cost,
       0                                                                AS product_discount,
       0                                                                AS token_discount,
       COALESCE(frl.return_item_quantity, 0)                            AS return_unit_count,
       (COALESCE(frl.return_subtotal_local_amount, 0) - COALESCE(frl.return_discount_local_amount, 0)) *
       COALESCE(frl.return_receipt_date_usd_conversion_rate, 1)         AS return_product_gross_revenue_excl_shipping,
       COALESCE(frl.estimated_returned_product_cost_local_amount_resaleable, 0) *
       COALESCE(frl.return_receipt_date_usd_conversion_rate, 1)         AS return_product_cost_returned_resalable
FROM _order_line_ext__base_stg base
         JOIN _order_line_ext__price p ON base.sku = p.sku AND base.product_store_id = p.store_id
         JOIN data_model.fact_return_line frl ON base.order_line_id = frl.order_line_id
         JOIN data_model.dim_return_status drs ON frl.return_status_key = drs.return_status_key
         JOIN data_model.dim_order_status dos ON base.order_status_key = dos.order_status_key
         JOIN data_model.dim_order_processing_status dops ON base.order_processing_status_key = dops.order_processing_status_key
         LEFT JOIN data_model.dim_warehouse dw
                   ON frl.warehouse_id = dw.warehouse_id AND LOWER(dw.warehouse_type) = 'we ship' AND
                      LOWER(dw.is_active) = 'true'
         LEFT JOIN data_model.dim_bundle_component_history dbch
                   ON base.bundle_component_history_key = dbch.bundle_component_history_key
WHERE LOWER(drs.return_status) = 'resolved'
  AND LOWER(base.order_classification_l1) IN ('product order', 'reship', 'exchange')
  AND NOT (base.store_brand = 'Fabletics' AND base.product_category = 'Savage')
  AND frl.return_receipt_local_datetime :: DATE >= '2020-01-01'
QUALIFY ROW_NUMBER() OVER (PARTITION BY frl.order_line_id ORDER BY frl.return_line_id DESC) = 1;

CREATE OR REPLACE TEMP TABLE _order_line_ext__pre_stg AS
SELECT
    object_type,
    order_line_id,
    meta_original_order_line_id,
    order_id,
    original_order_id,
    placed_date,
    order_completion_date,
    order_shipped_date,
    reship_placed_date,
    exchange_placed_date,
    return_date,
    order_status,
    order_processing_status,
    order_line_status,
    store_id,
    store_name,
    store_full_name,
    store_brand,
    store_type,
    store_region,
    store_country,
    fulfillment_warehouse_id,
    warehouse_code,
    warehouse_name,
    warehouse_type,
    carrier_service,
    pickup_store_id,
    product_id,
    master_product_id,
    sku,
    product_sku,
    base_sku,
    product_name,
    bundle_product_id,
    bundle_alias,
    is_byo_component,
    is_bundle_component,
    is_pre_made_set,
    is_vip_box,
    is_free,
    color,
    size,
    product_gender,
    membership_order_type_l1,
    membership_order_type_l2,
    membership_order_type_l3,
    order_sales_channel_l1,
    order_sales_channel_l2,
    order_classification_l1,
    is_retail_ship_only_order,
    is_bops_order,
    customer_id,
    customer_gender,
    is_cross_promo,
    finance_specialty_store,
    price_sold_as_type,
    first_item_vip_price,
    current_item_vip_price,
    item_vip_price,
    item_retail_price,
    non_reduced_vip_price,
    unit_count,
    reship_unit_count,
    exchange_unit_count,
    finalsales_quantity,
    token_count,
    subtotal_excl_tariff,
    tariff_revenue,
    token_subtotal,
    product_gross_revenue_excl_shipping,
    product_margin_pre_return_excl_shipping,
    product_cost,
    reship_product_cost,
    exchange_product_cost,
    token_product_cost,
    product_discount,
    token_discount,
    return_unit_count,
    return_product_gross_revenue_excl_shipping,
    return_product_cost_returned_resalable
FROM _order_line_ext__product_order
UNION ALL
SELECT
    object_type,
    order_line_id,
    meta_original_order_line_id,
    order_id,
    original_order_id,
    placed_date,
    order_completion_date,
    order_shipped_date,
    reship_placed_date,
    exchange_placed_date,
    return_date,
    order_status,
    order_processing_status,
    order_line_status,
    store_id,
    store_name,
    store_full_name,
    store_brand,
    store_type,
    store_region,
    store_country,
    fulfillment_warehouse_id,
    warehouse_code,
    warehouse_name,
    warehouse_type,
    carrier_service,
    pickup_store_id,
    product_id,
    master_product_id,
    sku,
    product_sku,
    base_sku,
    product_name,
    bundle_product_id,
    bundle_alias,
    is_byo_component,
    is_bundle_component,
    is_pre_made_set,
    is_vip_box,
    is_free,
    color,
    size,
    product_gender,
    membership_order_type_l1,
    membership_order_type_l2,
    membership_order_type_l3,
    order_sales_channel_l1,
    order_sales_channel_l2,
    order_classification_l1,
    is_retail_ship_only_order,
    is_bops_order,
    customer_id,
    customer_gender,
    is_cross_promo,
    finance_specialty_store,
    price_sold_as_type,
    first_item_vip_price,
    current_item_vip_price,
    item_vip_price,
    item_retail_price,
    non_reduced_vip_price,
    unit_count,
    reship_unit_count,
    exchange_unit_count,
    finalsales_quantity,
    token_count,
    subtotal_excl_tariff,
    tariff_revenue,
    token_subtotal,
    product_gross_revenue_excl_shipping,
    product_margin_pre_return_excl_shipping,
    product_cost,
    reship_product_cost,
    exchange_product_cost,
    token_product_cost,
    product_discount,
    token_discount,
    return_unit_count,
    return_product_gross_revenue_excl_shipping,
    return_product_cost_returned_resalable
FROM _order_line_ext__reship
UNION ALL
SELECT
    object_type,
    order_line_id,
    meta_original_order_line_id,
    order_id,
    original_order_id,
    placed_date,
    order_completion_date,
    order_shipped_date,
    reship_placed_date,
    exchange_placed_date,
    return_date,
    order_status,
    order_processing_status,
    order_line_status,
    store_id,
    store_name,
    store_full_name,
    store_brand,
    store_type,
    store_region,
    store_country,
    fulfillment_warehouse_id,
    warehouse_code,
    warehouse_name,
    warehouse_type,
    carrier_service,
    pickup_store_id,
    product_id,
    master_product_id,
    sku,
    product_sku,
    base_sku,
    product_name,
    bundle_product_id,
    bundle_alias,
    is_byo_component,
    is_bundle_component,
    is_pre_made_set,
    is_vip_box,
    is_free,
    color,
    size,
    product_gender,
    membership_order_type_l1,
    membership_order_type_l2,
    membership_order_type_l3,
    order_sales_channel_l1,
    order_sales_channel_l2,
    order_classification_l1,
    is_retail_ship_only_order,
    is_bops_order,
    customer_id,
    customer_gender,
    is_cross_promo,
    finance_specialty_store,
    price_sold_as_type,
    first_item_vip_price,
    current_item_vip_price,
    item_vip_price,
    item_retail_price,
    non_reduced_vip_price,
    unit_count,
    reship_unit_count,
    exchange_unit_count,
    finalsales_quantity,
    token_count,
    subtotal_excl_tariff,
    tariff_revenue,
    token_subtotal,
    product_gross_revenue_excl_shipping,
    product_margin_pre_return_excl_shipping,
    product_cost,
    reship_product_cost,
    exchange_product_cost,
    token_product_cost,
    product_discount,
    token_discount,
    return_unit_count,
    return_product_gross_revenue_excl_shipping,
    return_product_cost_returned_resalable
FROM _order_line_ext__exchange
UNION ALL
SELECT
    object_type,
    order_line_id,
    meta_original_order_line_id,
    order_id,
    original_order_id,
    placed_date,
    order_completion_date,
    order_shipped_date,
    reship_placed_date,
    exchange_placed_date,
    return_date,
    order_status,
    order_processing_status,
    order_line_status,
    store_id,
    store_name,
    store_full_name,
    store_brand,
    store_type,
    store_region,
    store_country,
    fulfillment_warehouse_id,
    warehouse_code,
    warehouse_name,
    warehouse_type,
    carrier_service,
    pickup_store_id,
    product_id,
    master_product_id,
    sku,
    product_sku,
    base_sku,
    product_name,
    bundle_product_id,
    bundle_alias,
    is_byo_component,
    is_bundle_component,
    is_pre_made_set,
    is_vip_box,
    is_free,
    color,
    size,
    product_gender,
    membership_order_type_l1,
    membership_order_type_l2,
    membership_order_type_l3,
    order_sales_channel_l1,
    order_sales_channel_l2,
    order_classification_l1,
    is_retail_ship_only_order,
    is_bops_order,
    customer_id,
    customer_gender,
    is_cross_promo,
    finance_specialty_store,
    price_sold_as_type,
    first_item_vip_price,
    current_item_vip_price,
    item_vip_price,
    item_retail_price,
    non_reduced_vip_price,
    unit_count,
    reship_unit_count,
    exchange_unit_count,
    finalsales_quantity,
    token_count,
    subtotal_excl_tariff,
    tariff_revenue,
    token_subtotal,
    product_gross_revenue_excl_shipping,
    product_margin_pre_return_excl_shipping,
    product_cost,
    reship_product_cost,
    exchange_product_cost,
    token_product_cost,
    product_discount,
    token_discount,
    return_unit_count,
    return_product_gross_revenue_excl_shipping,
    return_product_cost_returned_resalable
FROM _order_line_ext__return;

CREATE OR REPLACE TEMP TABLE _order_line_ext__stg AS
SELECT
    object_type,
    order_line_id,
    meta_original_order_line_id,
    order_id,
    original_order_id,
    placed_date,
    order_completion_date,
    order_shipped_date,
    reship_placed_date,
    exchange_placed_date,
    return_date,
    order_status,
    order_processing_status,
    order_line_status,
    store_id,
    store_name,
    store_full_name,
    store_brand,
    store_type,
    store_region,
    store_country,
    fulfillment_warehouse_id,
    warehouse_code,
    warehouse_name,
    warehouse_type,
    carrier_service,
    pickup_store_id,
    product_id,
    master_product_id,
    sku,
    product_sku,
    base_sku,
    product_name,
    bundle_product_id,
    bundle_alias,
    is_byo_component,
    is_bundle_component,
    is_pre_made_set,
    is_vip_box,
    is_free,
    color,
    size,
    product_gender,
    membership_order_type_l1,
    membership_order_type_l2,
    membership_order_type_l3,
    order_sales_channel_l1,
    order_sales_channel_l2,
    order_classification_l1,
    is_retail_ship_only_order,
    is_bops_order,
    customer_id,
    customer_gender,
    is_cross_promo,
    finance_specialty_store,
    price_sold_as_type,
    first_item_vip_price,
    current_item_vip_price,
    item_vip_price,
    item_retail_price,
    non_reduced_vip_price,
    unit_count,
    reship_unit_count,
    exchange_unit_count,
    finalsales_quantity,
    token_count,
    subtotal_excl_tariff,
    tariff_revenue,
    token_subtotal,
    product_gross_revenue_excl_shipping,
    product_margin_pre_return_excl_shipping,
    product_cost,
    reship_product_cost,
    exchange_product_cost,
    token_product_cost,
    product_discount,
    token_discount,
    return_unit_count,
    return_product_gross_revenue_excl_shipping,
    return_product_cost_returned_resalable,
    HASH(
        object_type,
        order_line_id,
        meta_original_order_line_id,
        order_id,
        original_order_id,
        placed_date::TIMESTAMP_NTZ,
        order_completion_date::TIMESTAMP_NTZ,
        order_shipped_date::TIMESTAMP_NTZ,
        reship_placed_date::TIMESTAMP_NTZ,
        exchange_placed_date::TIMESTAMP_NTZ,
        return_date::TIMESTAMP_NTZ,
        order_status,
        order_processing_status,
        order_line_status,
        store_id,
        store_name,
        store_full_name,
        store_brand,
        store_type,
        store_region,
        store_country,
        fulfillment_warehouse_id,
        warehouse_code,
        warehouse_name,
        warehouse_type,
        carrier_service,
        pickup_store_id,
        product_id,
        master_product_id,
        sku,
        product_sku,
        base_sku,
        product_name,
        bundle_product_id,
        bundle_alias,
        is_byo_component,
        is_bundle_component,
        is_pre_made_set,
        is_vip_box,
        is_free,
        color,
        size,
        product_gender,
        membership_order_type_l1,
        membership_order_type_l2,
        membership_order_type_l3,
        order_sales_channel_l1,
        order_sales_channel_l2,
        order_classification_l1,
        is_retail_ship_only_order,
        is_bops_order,
        customer_id,
        customer_gender,
        is_cross_promo,
        finance_specialty_store,
        price_sold_as_type,
        first_item_vip_price,
        current_item_vip_price,
        item_vip_price,
        item_retail_price,
        non_reduced_vip_price,
        unit_count,
        reship_unit_count,
        exchange_unit_count,
        finalsales_quantity,
        token_count,
        subtotal_excl_tariff,
        tariff_revenue,
        token_subtotal,
        product_gross_revenue_excl_shipping,
        product_margin_pre_return_excl_shipping,
        product_cost,
        reship_product_cost,
        exchange_product_cost,
        token_product_cost,
        product_discount,
        token_discount,
        return_unit_count,
        return_product_gross_revenue_excl_shipping,
        return_product_cost_returned_resalable
        ) AS meta_row_hash,
    $execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM _order_line_ext__pre_stg
ORDER BY
    order_line_id,
    object_type;

--Updates the deleted flag wrt FOL is_deleted column for those orderlines n
UPDATE analytics_base.order_line_ext_stg stg
SET stg.is_deleted = TRUE
FROM stg.fact_order_line fol
WHERE stg.order_line_id = fol.order_line_id
and fol.is_deleted = TRUE and stg.is_deleted = FALSE;

MERGE INTO analytics_base.order_line_ext_stg AS t
USING (
    SELECT
        object_type,
        order_line_id,
        meta_original_order_line_id,
        order_id,
        original_order_id,
        placed_date,
        order_completion_date,
        order_shipped_date,
        reship_placed_date,
        exchange_placed_date,
        return_date,
        order_status,
        order_processing_status,
        order_line_status,
        store_id,
        store_name,
        store_full_name,
        store_brand,
        store_type,
        store_region,
        store_country,
        fulfillment_warehouse_id,
        warehouse_code,
        warehouse_name,
        warehouse_type,
        carrier_service,
        pickup_store_id,
        product_id,
        master_product_id,
        sku,
        product_sku,
        base_sku,
        product_name,
        bundle_product_id,
        bundle_alias,
        is_byo_component,
        is_bundle_component,
        is_pre_made_set,
        is_vip_box,
        is_free,
        color,
        size,
        product_gender,
        membership_order_type_l1,
        membership_order_type_l2,
        membership_order_type_l3,
        order_sales_channel_l1,
        order_sales_channel_l2,
        order_classification_l1,
        is_retail_ship_only_order,
        is_bops_order,
        customer_id,
        customer_gender,
        is_cross_promo,
        finance_specialty_store,
        price_sold_as_type,
        first_item_vip_price,
        current_item_vip_price,
        item_vip_price,
        item_retail_price,
        non_reduced_vip_price,
        unit_count,
        reship_unit_count,
        exchange_unit_count,
        finalsales_quantity,
        token_count,
        subtotal_excl_tariff,
        tariff_revenue,
        token_subtotal,
        product_gross_revenue_excl_shipping,
        product_margin_pre_return_excl_shipping,
        product_cost,
        reship_product_cost,
        exchange_product_cost,
        token_product_cost,
        product_discount,
        token_discount,
        return_unit_count,
        return_product_gross_revenue_excl_shipping,
        return_product_cost_returned_resalable,
        meta_row_hash,
        meta_create_datetime,
        meta_update_datetime
    FROM _order_line_ext__stg
    ORDER BY
        order_line_id,
        object_type
    ) AS s
    ON s.order_line_id = t.order_line_id
    AND s.object_type = t.object_type
WHEN NOT MATCHED THEN
    INSERT (
        object_type,
        order_line_id,
        meta_original_order_line_id,
        order_id,
        original_order_id,
        placed_date,
        order_completion_date,
        order_shipped_date,
        reship_placed_date,
        exchange_placed_date,
        return_date,
        order_status,
        order_processing_status,
        order_line_status,
        store_id,
        store_name,
        store_full_name,
        store_brand,
        store_type,
        store_region,
        store_country,
        fulfillment_warehouse_id,
        warehouse_code,
        warehouse_name,
        warehouse_type,
        carrier_service,
        pickup_store_id,
        product_id,
        master_product_id,
        sku,
        product_sku,
        base_sku,
        product_name,
        bundle_product_id,
        bundle_alias,
        is_byo_component,
        is_bundle_component,
        is_pre_made_set,
        is_vip_box,
        is_free,
        color,
        size,
        product_gender,
        membership_order_type_l1,
        membership_order_type_l2,
        membership_order_type_l3,
        order_sales_channel_l1,
        order_sales_channel_l2,
        order_classification_l1,
        is_retail_ship_only_order,
        is_bops_order,
        customer_id,
        customer_gender,
        is_cross_promo,
        finance_specialty_store,
        price_sold_as_type,
        first_item_vip_price,
        current_item_vip_price,
        item_vip_price,
        item_retail_price,
        non_reduced_vip_price,
        unit_count,
        reship_unit_count,
        exchange_unit_count,
        finalsales_quantity,
        token_count,
        subtotal_excl_tariff,
        tariff_revenue,
        token_subtotal,
        product_gross_revenue_excl_shipping,
        product_margin_pre_return_excl_shipping,
        product_cost,
        reship_product_cost,
        exchange_product_cost,
        token_product_cost,
        product_discount,
        token_discount,
        return_unit_count,
        return_product_gross_revenue_excl_shipping,
        return_product_cost_returned_resalable,
        meta_row_hash,
        meta_create_datetime,
        meta_update_datetime
        )
    VALUES (
        s.object_type,
        s.order_line_id,
        s.meta_original_order_line_id,
        s.order_id,
        s.original_order_id,
        s.placed_date,
        s.order_completion_date,
        s.order_shipped_date,
        s.reship_placed_date,
        s.exchange_placed_date,
        s.return_date,
        s.order_status,
        s.order_processing_status,
        s.order_line_status,
        s.store_id,
        s.store_name,
        s.store_full_name,
        s.store_brand,
        s.store_type,
        s.store_region,
        s.store_country,
        s.fulfillment_warehouse_id,
        s.warehouse_code,
        s.warehouse_name,
        s.warehouse_type,
        s.carrier_service,
        s.pickup_store_id,
        s.product_id,
        s.master_product_id,
        s.sku,
        s.product_sku,
        s.base_sku,
        s.product_name,
        s.bundle_product_id,
        s.bundle_alias,
        s.is_byo_component,
        s.is_bundle_component,
        s.is_pre_made_set,
        s.is_vip_box,
        s.is_free,
        s.color,
        s.size,
        s.product_gender,
        s.membership_order_type_l1,
        s.membership_order_type_l2,
        s.membership_order_type_l3,
        s.order_sales_channel_l1,
        s.order_sales_channel_l2,
        s.order_classification_l1,
        s.is_retail_ship_only_order,
        s.is_bops_order,
        s.customer_id,
        s.customer_gender,
        s.is_cross_promo,
        s.finance_specialty_store,
        s.price_sold_as_type,
        s.first_item_vip_price,
        s.current_item_vip_price,
        s.item_vip_price,
        s.item_retail_price,
        s.non_reduced_vip_price,
        s.unit_count,
        s.reship_unit_count,
        s.exchange_unit_count,
        s.finalsales_quantity,
        s.token_count,
        s.subtotal_excl_tariff,
        s.tariff_revenue,
        s.token_subtotal,
        s.product_gross_revenue_excl_shipping,
        s.product_margin_pre_return_excl_shipping,
        s.product_cost,
        s.reship_product_cost,
        s.exchange_product_cost,
        s.token_product_cost,
        s.product_discount,
        s.token_discount,
        s.return_unit_count,
        s.return_product_gross_revenue_excl_shipping,
        s.return_product_cost_returned_resalable,
        s.meta_row_hash,
        s.meta_create_datetime,
        s.meta_update_datetime
        )
WHEN MATCHED AND t.meta_row_hash != s.meta_row_hash THEN
    UPDATE
    SET
        --t.object_type = s.object_type,
        --t.order_line_id = s.order_line_id,
        t.meta_original_order_line_id = s.meta_original_order_line_id,
        t.order_id = s.order_id,
        t.original_order_id = s.original_order_id,
        t.placed_date = s.placed_date,
        t.order_completion_date = s.order_completion_date,
        t.order_shipped_date = s.order_shipped_date,
        t.reship_placed_date = s.reship_placed_date,
        t.exchange_placed_date = s.exchange_placed_date,
        t.return_date = s.return_date,
        t.order_status = s.order_status,
        t.order_processing_status = s.order_processing_status,
        t.order_line_status = s.order_line_status,
        t.store_id = s.store_id,
        t.store_name = s.store_name,
        t.store_full_name = s.store_full_name,
        t.store_brand = s.store_brand,
        t.store_type = s.store_type,
        t.store_region = s.store_region,
        t.store_country = s.store_country,
        t.fulfillment_warehouse_id = s.fulfillment_warehouse_id,
        t.warehouse_code = s.warehouse_code,
        t.warehouse_name = s.warehouse_name,
        t.warehouse_type = s.warehouse_type,
        t.carrier_service = s.carrier_service,
        t.pickup_store_id = s.pickup_store_id,
        t.product_id = s.product_id,
        t.master_product_id = s.master_product_id,
        t.sku = s.sku,
        t.product_sku = s.product_sku,
        t.base_sku = s.base_sku,
        t.product_name = s.product_name,
        t.bundle_product_id = s.bundle_product_id,
        t.bundle_alias = s.bundle_alias,
        t.is_byo_component = s.is_byo_component,
        t.is_bundle_component = s.is_bundle_component,
        t.is_pre_made_set = s.is_pre_made_set,
        t.is_vip_box = s.is_vip_box,
        t.is_free = s.is_free,
        t.color = s.color,
        t.size = s.size,
        t.product_gender = s.product_gender,
        t.membership_order_type_l1 = s.membership_order_type_l1,
        t.membership_order_type_l2 = s.membership_order_type_l2,
        t.membership_order_type_l3 = s.membership_order_type_l3,
        t.order_sales_channel_l1 = s.order_sales_channel_l1,
        t.order_sales_channel_l2 = s.order_sales_channel_l2,
        t.order_classification_l1 = s.order_classification_l1,
        t.is_retail_ship_only_order = s.is_retail_ship_only_order,
        t.is_bops_order = s.is_bops_order,
        t.customer_id = s.customer_id,
        t.customer_gender = s.customer_gender,
        t.is_cross_promo = s.is_cross_promo,
        t.finance_specialty_store = s.finance_specialty_store,
        t.price_sold_as_type = s.price_sold_as_type,
        t.first_item_vip_price = s.first_item_vip_price,
        t.current_item_vip_price = s.current_item_vip_price,
        t.item_vip_price = s.item_vip_price,
        t.item_retail_price = s.item_retail_price,
        t.non_reduced_vip_price = s.non_reduced_vip_price,
        t.unit_count = s.unit_count,
        t.reship_unit_count = s.reship_unit_count,
        t.exchange_unit_count = s.exchange_unit_count,
        t.finalsales_quantity = s.finalsales_quantity,
        t.token_count = s.token_count,
        t.subtotal_excl_tariff = s.subtotal_excl_tariff,
        t.tariff_revenue = s.tariff_revenue,
        t.token_subtotal = s.token_subtotal,
        t.product_gross_revenue_excl_shipping = s.product_gross_revenue_excl_shipping,
        t.product_margin_pre_return_excl_shipping = s.product_margin_pre_return_excl_shipping,
        t.product_cost = s.product_cost,
        t.reship_product_cost = s.reship_product_cost,
        t.exchange_product_cost = s.exchange_product_cost,
        t.token_product_cost = s.token_product_cost,
        t.product_discount = s.product_discount,
        t.token_discount = s.token_discount,
        t.return_unit_count = s.return_unit_count,
        t.return_product_gross_revenue_excl_shipping = s.return_product_gross_revenue_excl_shipping,
        t.return_product_cost_returned_resalable = s.return_product_cost_returned_resalable,
        t.meta_row_hash = s.meta_row_hash,
        --t.meta_create_datetime = s.meta_create_datetime,
        t.meta_update_datetime = s.meta_update_datetime;

--  Success
UPDATE stg.meta_table_dependency_watermark
SET high_watermark_datetime = IFF(dependent_table_name IS NOT NULL, new_high_watermark_datetime,
        (SELECT MAX(meta_update_datetime) AS new_high_watermark_datetime FROM analytics_base.order_line_ext_stg)),
    meta_update_datetime = CURRENT_TIMESTAMP()::TIMESTAMP_LTZ(3)
WHERE table_name = $target_table;
-- SELECT * FROM stg.meta_table_dependency_watermark WHERE table_name = $target_table;
