SET start_date = DATE_TRUNC(MONTH, DATEADD(MONTH, -36, CURRENT_DATE()));

CREATE OR REPLACE TEMPORARY TABLE _order_line_promo_id AS
SELECT a.order_line_id
     , a.promo_id
     , RANK() OVER (PARTITION BY a.order_line_id ORDER BY a.code) AS promo_rank
FROM (
         SELECT fol.order_line_id
              , prom.promo_id
              , prom.code
         FROM edw_prod.data_model_jfb.fact_order_line fol
                  JOIN reporting_prod.gfb.vw_store st
                       ON st.store_id = fol.store_id
                  JOIN edw_prod.data_model_jfb.fact_order fo
                       ON fo.order_id = fol.order_id
                  JOIN edw_prod.data_model_jfb.dim_order_processing_status dops
                       ON dops.order_processing_status_key = fo.order_processing_status_key
                  JOIN lake_jfb_view.ultra_merchant.order_line_discount old
                       ON old.order_line_id = fol.order_line_id
                  JOIN lake_jfb_view.ultra_merchant.promo prom
                       ON prom.promo_id = old.promo_id
                  JOIN edw_prod.data_model_jfb.dim_order_sales_channel dosc
                       ON dosc.order_sales_channel_key = fol.order_sales_channel_key
                           AND dosc.is_test_order = 0
         WHERE (
                     fol.order_status_key = 1
                 OR
                     (fol.order_status_key = 2 AND
                      dops.order_processing_status IN ('FulFillment (Batching)', 'FulFillment (In Progress)', 'Placed'))
             )
           AND fol.order_line_status_key != 4
           AND dosc.order_classification_l1 IN ('Product Order')
           AND CAST(fol.order_local_datetime AS DATE) >= $start_date

         UNION

         SELECT fol.order_line_id
              , prom.promo_id
              , prom.code
         FROM edw_prod.data_model_jfb.fact_order_line fol
                  JOIN reporting_prod.gfb.vw_store st
                       ON st.store_id = fol.store_id
                  JOIN edw_prod.data_model_jfb.fact_order fo
                       ON fo.order_id = fol.order_id
                  JOIN edw_prod.data_model_jfb.dim_order_processing_status dops
                       ON dops.order_processing_status_key = fo.order_processing_status_key
                  JOIN lake_jfb_view.ultra_merchant.order_line_discount old
                       ON old.order_line_id = fol.bundle_order_line_id -- this is for bundle products
                  JOIN lake_jfb_view.ultra_merchant.promo prom
                       ON prom.promo_id = old.promo_id
                  JOIN edw_prod.data_model_jfb.dim_order_sales_channel dosc
                       ON dosc.order_sales_channel_key = fol.order_sales_channel_key
                           AND dosc.is_test_order = 0
         WHERE (
                     fol.order_status_key = 1
                 OR
                     (fol.order_status_key = 2 AND
                      dops.order_processing_status IN ('FulFillment (Batching)', 'FulFillment (In Progress)', 'Placed'))
             )
           AND fol.order_line_status_key != 4
           AND dosc.order_classification_l1 IN ('Product Order')
           AND CAST(fol.order_local_datetime AS DATE) >= $start_date
     ) a;

CREATE OR REPLACE TEMPORARY TABLE _clearance_skus AS
SELECT DISTINCT pph.product_price_history_key
              , pph.sale_price
              , pph.effective_start_datetime::DATE AS effective_start_datetime
              , pph.effective_end_datetime::DATE   AS effective_end_datetime
FROM edw_prod.data_model_jfb.dim_product_price_history pph
         LEFT JOIN reporting_prod.gfb.vw_store s ON s.store_id = pph.store_id
WHERE s.store_brand_abbr IN ('JF', 'SD', 'FK')
  AND pph.sale_price > 0;

CREATE OR REPLACE TEMPORARY TABLE _two_for_one_bundle AS
SELECT *
FROM edw_prod.data_model_jfb.dim_product
WHERE product_type = 'BYO'
  AND product_name ILIKE '2 for 1%';

CREATE OR REPLACE TEMPORARY TABLE _order_line_detail_place_date AS
SELECT DISTINCT UPPER(st.store_brand)                                                            AS business_unit
              , UPPER(st.store_region)                                                           AS region
              , (CASE
                     WHEN dc.specialty_country_code = 'GB' THEN 'UK'
                     WHEN dc.specialty_country_code != 'Unknown' THEN dc.specialty_country_code
                     ELSE st.store_country END)                                                  AS country
              , fol.order_id
              , fol.order_line_id
              , fo.session_id
              , dc.customer_id
              , dp.product_id
              , dp.product_type
              , dpt.product_type_name
              , dp.product_sku
              , dp.sku
              , dp.product_name
              , dp.color                                                                         AS dp_color
              , LOWER(dp.size)                                                                   AS dp_size
              , CAST(fol.order_local_datetime AS DATE)                                           AS order_date
              , (CASE
                     WHEN dosc.order_classification_l1 = 'Product Order' THEN 'product order'
                     WHEN dosc.order_classification_l1 = 'Billing Order' THEN 'credit billing'
                     WHEN dosc.order_classification_l1 = 'Exchange' THEN 'exchange'
                     WHEN dosc.order_classification_l1 = 'Reship' THEN 'reship'
    END)                                                                                         AS order_classification
              , (CASE
                     WHEN domc.membership_order_type_l2 = 'Guest' THEN 'ecom'
                     WHEN domc.membership_order_type_l1 = 'Activating VIP' THEN 'vip activating'
                     ELSE 'vip repeat' END)                                                      AS order_type
              , (CASE
                     WHEN st.store_brand_abbr IN ('JF', 'SD') AND ROUND(mcsl.sale_price) - mcsl.sale_price = 0.03
                         THEN 'clearance'
                     WHEN st.store_brand_abbr IN ('FK') AND ROUND(mcsl.sale_price) - mcsl.sale_price = 0.01
                         THEN 'clearance'
                     WHEN mcsl.sale_price > 0 THEN 'markdown'
                     ELSE 'regular' END)                                                         AS clearance_flag
              , mcsl.sale_price                                                                  AS clearance_price
              , (CASE
                     WHEN lop.product_sku IS NOT NULL THEN 'lead only'
                     ELSE 'not lead only' END)                                                   AS lead_only_flag
              , prom_1.promo_id                                                                  AS promo_id_1
              , UPPER(IFF(CONTAINS(prom_1.code, 'REV_'), up_1.code, prom_1.code))                AS promo_code_1
              , prom_2.promo_id                                                                  AS promo_id_2
              , UPPER(IFF(CONTAINS(prom_2.code, 'REV_'), up_2.code, prom_2.code))                AS promo_code_2
              , dpm.is_prepaid_creditcard
              , dpm.raw_creditcard_type                                                          AS creditcard_type
              , (CASE
                     WHEN order_date >= '2020-01-01' AND dw.country_code = 'CA'
                         THEN 'Just Fabulous'
                     ELSE dw.warehouse_name END)                                                 AS warehouse_name
              , (CASE
                     WHEN order_date >= '2020-01-01' AND dw.country_code = 'CA'
                         THEN 'Louisville'
                     ELSE dw.city END)                                                           AS warehouse_city
              , (CASE
                     WHEN order_date >= '2020-01-01' AND dw.country_code = 'CA'
                         THEN 'KY'
                     ELSE dw.state END)                                                          AS warehouse_state
              , (CASE
                     WHEN order_date >= '2020-01-01' AND dw.country_code = 'CA'
                         THEN 'US'
                     ELSE dw.country_code END)                                                   AS warehouse_country
              , fol.bundle_product_id
              , bch.bundle_component_product_id
              , fol.bundle_order_line_id
              , ops.order_product_source_name
              , (CASE
                     WHEN ops.order_product_source_name LIKE '%look%' AND business_unit != 'FABKIDS'
                         THEN SPLIT_PART(ops.order_product_source_name, ':', -1)
                     ELSE NULL END)                                                              AS bundle_product_id_jfsd
              , fol.warehouse_id

              --Sales
              , fol.item_quantity                                                                AS total_qty_sold
              , (fol.subtotal_excl_tariff_local_amount - fol.product_discount_local_amount) *
                COALESCE(fol.order_date_usd_conversion_rate, 1)                                  AS total_product_revenue
              , COALESCE(fol.reporting_landed_cost_local_amount, 0) *
                COALESCE(fol.order_date_usd_conversion_rate, 1)                                  AS total_cogs
              , COALESCE(fol.reporting_landed_cost_local_amount_accounting, 0) *
                COALESCE(fol.order_date_usd_conversion_rate, 1)                                  AS total_cogs_without_tariff
              , fol.product_discount_local_amount *
                COALESCE(fol.order_date_usd_conversion_rate, 1)                                  AS total_discount
              , fol.tax_local_amount * COALESCE(fol.order_date_usd_conversion_rate, 1)           AS total_tax
              , fol.subtotal_excl_tariff_local_amount *
                COALESCE(fol.order_date_usd_conversion_rate, 1)                                  AS order_line_subtotal
              , fo.subtotal_excl_tariff_local_amount *
                COALESCE(fol.order_date_usd_conversion_rate, 1)                                  AS order_subtotal
              , (CASE
                     WHEN fo.subtotal_excl_tariff_local_amount = 0 THEN 0
                     ELSE fol.subtotal_excl_tariff_local_amount / fo.subtotal_excl_tariff_local_amount
    END)                                                                                         AS order_line_revenue_percent
              , fol.cash_credit_local_amount *
                COALESCE(fol.order_date_usd_conversion_rate, 1)                                  AS total_cash_credit_amount
              , fol.non_cash_credit_local_amount *
                COALESCE(fol.order_date_usd_conversion_rate, 1)                                  AS total_non_cash_credit_amount
              , fo.shipping_revenue_local_amount *
                COALESCE(fol.order_date_usd_conversion_rate, 1)                                  AS order_shipping_revenue
              , fo.shipping_cost_local_amount *
                COALESCE(fol.order_date_usd_conversion_rate, 1)                                  AS order_shipping_cost
              ,
        (fol.product_subtotal_local_amount - fol.product_discount_local_amount + fol.shipping_revenue_local_amount -
         fol.non_cash_credit_local_amount - fol.cash_credit_local_amount) *
        COALESCE(fol.order_date_usd_conversion_rate, 1)                                          AS cash_collected_amount
              ,SUM(fol.cash_credit_local_amount) OVER(PARTITION BY fol.order_id) AS order_cash_credit_local_amount
              ,SUM(cash_collected_amount) OVER(PARTITION BY fol.order_id) AS order_cash_collected_amount

              ,   CASE
                    WHEN IFNULL(order_cash_credit_local_amount, 0) = 0 THEN 'Only Cash'
                    WHEN IFNULL(order_cash_collected_amount, 0)= 0 THEN 'Credits Only'
                    ELSE 'Cash & Credits' END                                                   AS credit_order_type
              , IFNULL(IFNULL(fol.subtotal_excl_tariff_local_amount, 0)
                           + IFNULL(fol.tariff_revenue_local_amount, 0)
                           - IFNULL(fol.product_discount_local_amount, 0)
                           - IFNULL(fol.non_cash_credit_local_amount, 0)
    ,
                       0)                                                                        AS total_product_revenue_with_tariff
              , fol.token_count                                                                  AS tokens_applied
              , fol.token_local_amount
              , IFF(fol.token_count > 0, (fol.subtotal_excl_tariff_local_amount - fol.product_discount_local_amount) *
                                         COALESCE(fol.order_date_usd_conversion_rate, 1),
                    0)                                                                           AS token_product_revenue
              , IFF(fol.token_count > 0, IFNULL(IFNULL(fol.subtotal_excl_tariff_local_amount, 0)
                                                    + IFNULL(fol.tariff_revenue_local_amount, 0)
                                                    - IFNULL(fol.product_discount_local_amount, 0)
                                                    - IFNULL(fol.non_cash_credit_local_amount, 0)
    ,
                                                0),
                    0)                                                                           AS token_product_revenue_with_tariff
              , IFF(fol.token_count > 0,
                    COALESCE(fol.reporting_landed_cost_local_amount, 0) *
                    COALESCE(fol.order_date_usd_conversion_rate, 1), 0)                          AS token_cogs
              , IFF(fol.token_count > 0, fol.product_discount_local_amount *
                                         COALESCE(fol.order_date_usd_conversion_rate, 1), 0)     AS token_discount
              , IFF(tfo.product_type = 'BYO', fol.token_count, 0)                                AS two_for_one_applied
              , IFF(tfo.product_type = 'BYO', fol.token_local_amount, 0)                         AS two_for_one_amount
              , IFF(two_for_one_applied > 0,
                    (fol.subtotal_excl_tariff_local_amount - fol.product_discount_local_amount) *
                    COALESCE(fol.order_date_usd_conversion_rate, 1),
                    0)                                                                           AS two_for_one_product_revenue
              , IFF(two_for_one_applied > 0, IFNULL(IFNULL(fol.subtotal_excl_tariff_local_amount, 0)
                                                        + IFNULL(fol.tariff_revenue_local_amount, 0)
                                                        - IFNULL(fol.product_discount_local_amount, 0)
                                                        - IFNULL(fol.non_cash_credit_local_amount, 0)
    ,
                                                    0),
                    0)                                                                           AS two_for_one_product_revenue_with_tariff
              , IFF(two_for_one_applied > 0,
                    COALESCE(fol.reporting_landed_cost_local_amount, 0) *
                    COALESCE(fol.order_date_usd_conversion_rate, 1), 0)                          AS two_for_one_cogs
              , IFF(two_for_one_applied > 0, fol.product_discount_local_amount *
                                             COALESCE(fol.order_date_usd_conversion_rate, 1), 0) AS two_for_one_discount
FROM edw_prod.data_model_jfb.fact_order_line fol
         JOIN reporting_prod.gfb.vw_store st
              ON st.store_id = fol.store_id
         JOIN edw_prod.data_model_jfb.dim_order_status dos
              ON dos.order_status_key = fol.order_status_key
         JOIN edw_prod.data_model_jfb.dim_order_line_status dols
              ON dols.order_line_status_key = fol.order_line_status_key
         JOIN edw_prod.data_model_jfb.dim_order_membership_classification domc
              ON domc.order_membership_classification_key = fol.order_membership_classification_key
         JOIN edw_prod.data_model_jfb.dim_product dp
              ON dp.product_id = fol.product_id
         JOIN edw_prod.data_model_jfb.dim_product_type dpt
              ON dpt.product_type_key = fol.product_type_key
                  AND
                 dpt.product_type_name NOT IN ('Gift Certificate', 'Membership Gift', 'Membership Reward Points Item')
         JOIN edw_prod.data_model_jfb.dim_customer dc
              ON dc.customer_id = fol.customer_id
                  AND dc.is_test_customer = 0
         JOIN edw_prod.data_model_jfb.fact_order fo
              ON fo.order_id = fol.order_id
         JOIN edw_prod.data_model_jfb.dim_order_processing_status dops
              ON dops.order_processing_status_key = fo.order_processing_status_key
         JOIN edw_prod.data_model_jfb.dim_order_sales_channel dosc
              ON dosc.order_sales_channel_key = fol.order_sales_channel_key
                  AND dosc.is_border_free_order = 0
                  AND dosc.is_ps_order = 0
                  AND dosc.is_test_order = 0
         JOIN edw_prod.data_model_jfb.dim_payment dpm
              ON dpm.payment_key = fo.payment_key
         LEFT JOIN _clearance_skus mcsl
                   ON mcsl.product_price_history_key = fol.product_price_history_key
                       AND
                      (CAST(fol.order_local_datetime AS DATE) >= mcsl.effective_start_datetime AND
                       CAST(fol.order_local_datetime AS DATE) <= COALESCE(mcsl.effective_end_datetime, CURRENT_DATE()))
         LEFT JOIN lake_view.sharepoint.gfb_lead_only_product lop
                   ON LOWER(lop.brand) = LOWER(st.store_brand)
                       AND LOWER(lop.region) = LOWER(st.store_region)
                       AND LOWER(lop.product_sku) = LOWER(dp.product_sku)
                       AND
                      (CAST(fol.order_local_datetime AS DATE) >= lop.date_added AND
                       CAST(fol.order_local_datetime AS DATE) <= COALESCE(lop.date_removed, CURRENT_DATE()))
         LEFT JOIN _order_line_promo_id old_1
                   ON old_1.order_line_id = fol.order_line_id
                       AND old_1.promo_rank = 1
         LEFT JOIN lake_jfb_view.ultra_merchant.promo prom_1
                   ON prom_1.promo_id = old_1.promo_id
         LEFT JOIN lake_jfb_view.ultra_cms.ui_promo_management_promos up_1
                   ON CAST(up_1.ui_promo_management_promo_id AS VARCHAR(100)) = SPLIT_PART(prom_1.code, '_', -1)
         LEFT JOIN _order_line_promo_id old_2
                   ON old_2.order_line_id = fol.order_line_id
                       AND old_2.promo_rank = 2
         LEFT JOIN lake_jfb_view.ultra_merchant.promo prom_2
                   ON prom_2.promo_id = old_2.promo_id
         LEFT JOIN lake_jfb_view.ultra_cms.ui_promo_management_promos up_2
                   ON CAST(up_2.ui_promo_management_promo_id AS VARCHAR(100)) = SPLIT_PART(prom_2.code, '_', -1)
         LEFT JOIN edw_prod.data_model_jfb.dim_warehouse dw
                   ON dw.warehouse_id = fol.warehouse_id
         LEFT JOIN edw_prod.data_model_jfb.dim_bundle_component_history bch
                   ON bch.bundle_component_history_key = fol.bundle_component_history_key
         LEFT JOIN edw_prod.data_model_jfb.dim_order_product_source ops
                   ON ops.order_product_source_key = fol.order_product_source_key
         LEFT JOIN _two_for_one_bundle tfo
                   ON tfo.product_id = fol.bundle_product_id
WHERE dos.order_status IN ('Success', 'Pending')
  AND dols.order_line_status != 'Cancelled'
  AND dosc.order_classification_l1 IN ('Billing Order', 'Exchange', 'Product Order', 'Reship')
  AND fol.order_local_datetime IS NOT NULL
  AND CAST(fol.order_local_datetime AS DATE) >= $start_date;


CREATE OR REPLACE TEMPORARY TABLE _order_line_detail_ship_date AS
SELECT DISTINCT UPPER(st.store_brand)                                                            AS business_unit
              , UPPER(st.store_region)                                                           AS region
              , (CASE
                     WHEN dc.specialty_country_code = 'GB' THEN 'UK'
                     WHEN dc.specialty_country_code != 'Unknown' THEN dc.specialty_country_code
                     ELSE st.store_country END)                                                  AS country
              , fol.order_id
              , fol.order_line_id
              , fo.session_id
              , dc.customer_id
              , dp.product_id
              , dp.product_type
              , dp.product_sku
              , dp.sku
              , dp.product_name
              , dp.color                                                                         AS dp_color
              , LOWER(dp.size)                                                                   AS dp_size
              , (CASE
                     WHEN dosc.order_classification_l1 = 'Billing Order'
                         THEN CAST(fol.payment_transaction_local_datetime AS DATE)
                     ELSE CAST(fol.shipped_local_datetime AS DATE) END)                          AS ship_date -- for credit billing
              , (CASE
                     WHEN dosc.order_classification_l1 = 'Product Order' THEN 'product order'
                     WHEN dosc.order_classification_l1 = 'Billing Order' THEN 'credit billing'
                     WHEN dosc.order_classification_l1 = 'Exchange' THEN 'exchange'
                     WHEN dosc.order_classification_l1 = 'Reship' THEN 'reship'
    END)                                                                                         AS order_classification
              , (CASE
                     WHEN domc.membership_order_type_l2 = 'Guest' THEN 'ecom'
                     WHEN domc.membership_order_type_l1 = 'Activating VIP' THEN 'vip activating'
                     ELSE 'vip repeat' END)                                                      AS order_type
              , (CASE
                     WHEN st.store_brand_abbr IN ('JF', 'SD') AND ROUND(mcsl.sale_price) - mcsl.sale_price = 0.03
                         THEN 'clearance'
                     WHEN st.store_brand_abbr IN ('FK') AND ROUND(mcsl.sale_price) - mcsl.sale_price = 0.01
                         THEN 'clearance'
                     WHEN mcsl.sale_price > 0 THEN 'markdown'
                     ELSE 'regular' END)                                                         AS clearance_flag
              , mcsl.sale_price                                                                  AS clearance_price
              , (CASE
                     WHEN lop.product_sku IS NOT NULL THEN 'lead only'
                     ELSE 'not lead only' END)                                                   AS lead_only_flag
              , prom_1.promo_id                                                                  AS promo_id_1
              , UPPER(IFF(CONTAINS(prom_1.code, 'REV_'), up_1.code, prom_1.code))                AS promo_code_1
              , prom_2.promo_id                                                                  AS promo_id_2
              , UPPER(IFF(CONTAINS(prom_2.code, 'REV_'), up_2.code, prom_2.code))                AS promo_code_2
              , dpm.is_prepaid_creditcard
              , dpm.raw_creditcard_type                                                          AS creditcard_type
              , (CASE
                     WHEN ship_date >= '2020-01-01' AND dw.country_code = 'CA'
                         THEN 'Just Fabulous'
                     ELSE dw.warehouse_name END)                                                 AS warehouse_name
              , (CASE
                     WHEN ship_date >= '2020-01-01' AND dw.country_code = 'CA'
                         THEN 'Louisville'
                     ELSE dw.city END)                                                           AS warehouse_city
              , (CASE
                     WHEN ship_date >= '2020-01-01' AND dw.country_code = 'CA'
                         THEN 'KY'
                     ELSE dw.state END)                                                          AS warehouse_state
              , (CASE
                     WHEN ship_date >= '2020-01-01' AND dw.country_code = 'CA'
                         THEN 'US'
                     ELSE dw.country_code END)                                                   AS warehouse_country
              , fol.bundle_product_id
              , bch.bundle_component_product_id
              , fol.bundle_order_line_id
              , ops.order_product_source_name
              , (CASE
                     WHEN ops.order_product_source_name LIKE '%look%' AND business_unit != 'FABKIDS'
                         THEN SPLIT_PART(ops.order_product_source_name, ':', -1)
                     ELSE NULL END)                                                              AS bundle_product_id_jfsd
              , fol.warehouse_id

              --Sales
              , fol.item_quantity                                                                AS total_qty_sold
              , (fol.subtotal_excl_tariff_local_amount - fol.product_discount_local_amount) *
                COALESCE(fol.order_date_usd_conversion_rate, 1)                                  AS total_product_revenue
              , COALESCE(fol.reporting_landed_cost_local_amount, 0) *
                COALESCE(fol.order_date_usd_conversion_rate, 1)                                  AS total_cogs
              , COALESCE(fol.reporting_landed_cost_local_amount_accounting, 0) *
                COALESCE(fol.order_date_usd_conversion_rate, 1)                                  AS total_cogs_without_tariff
              , fol.product_discount_local_amount *
                COALESCE(fol.order_date_usd_conversion_rate, 1)                                  AS total_discount
              , fol.tax_local_amount * COALESCE(fol.order_date_usd_conversion_rate, 1)           AS total_tax
              , fol.subtotal_excl_tariff_local_amount *
                COALESCE(fol.order_date_usd_conversion_rate, 1)                                  AS order_line_subtotal
              , fo.subtotal_excl_tariff_local_amount *
                COALESCE(fol.order_date_usd_conversion_rate, 1)                                  AS order_subtotal
              , (CASE
                     WHEN fo.subtotal_excl_tariff_local_amount = 0 THEN 0
                     ELSE fol.subtotal_excl_tariff_local_amount / fo.subtotal_excl_tariff_local_amount
    END)                                                                                         AS order_line_revenue_percent
              , fol.cash_credit_local_amount *
                COALESCE(fol.order_date_usd_conversion_rate, 1)                                  AS total_cash_credit_amount
              , fol.non_cash_credit_local_amount *
                COALESCE(fol.order_date_usd_conversion_rate, 1)                                  AS total_non_cash_credit_amount
              , fo.shipping_revenue_local_amount *
                COALESCE(fol.order_date_usd_conversion_rate, 1)                                  AS order_shipping_revenue
              , fo.shipping_cost_local_amount *
                COALESCE(fol.order_date_usd_conversion_rate, 1)                                  AS order_shipping_cost
              ,
        (fol.product_subtotal_local_amount - fol.product_discount_local_amount + fol.shipping_revenue_local_amount -
         fol.non_cash_credit_local_amount - fol.cash_credit_local_amount) *
        COALESCE(fol.order_date_usd_conversion_rate, 1)                                          AS cash_collected_amount
              ,SUM(fol.cash_credit_local_amount) OVER(PARTITION BY fol.order_id) AS order_cash_credit_local_amount
              ,SUM(cash_collected_amount) OVER(PARTITION BY fol.order_id) AS order_cash_collected_amount

              ,   CASE
                    WHEN IFNULL(order_cash_credit_local_amount, 0) = 0 THEN 'Only Cash'
                    WHEN IFNULL(order_cash_collected_amount, 0)= 0 THEN 'Credits Only'
                    ELSE 'Cash & Credits' END                                                   AS credit_order_type
              , IFNULL(IFNULL(fol.subtotal_excl_tariff_local_amount, 0)
                           + IFNULL(fol.tariff_revenue_local_amount, 0)
                           - IFNULL(fol.product_discount_local_amount, 0)
                           - IFNULL(fol.non_cash_credit_local_amount, 0)
    ,
                       0)                                                                        AS total_product_revenue_with_tariff
              , fol.token_count                                                                  AS tokens_applied
              , fol.token_local_amount
              , IFF(fol.token_count > 0, (fol.subtotal_excl_tariff_local_amount - fol.product_discount_local_amount) *
                                         COALESCE(fol.order_date_usd_conversion_rate, 1),
                    0)                                                                           AS token_product_revenue
              , IFF(fol.token_count > 0, IFNULL(IFNULL(fol.subtotal_excl_tariff_local_amount, 0)
                                                    + IFNULL(fol.tariff_revenue_local_amount, 0)
                                                    - IFNULL(fol.product_discount_local_amount, 0)
                                                    - IFNULL(fol.non_cash_credit_local_amount, 0)
    ,
                                                0),
                    0)                                                                           AS token_product_revenue_with_tariff
              , IFF(fol.token_count > 0,
                    COALESCE(fol.reporting_landed_cost_local_amount, 0) *
                    COALESCE(fol.order_date_usd_conversion_rate, 1), 0)                          AS token_cogs
              , IFF(fol.token_count > 0, fol.product_discount_local_amount *
                                         COALESCE(fol.order_date_usd_conversion_rate, 1), 0)     AS token_discount
              , IFF(tfo.product_type = 'BYO', fol.token_count, 0)                                AS two_for_one_applied
              , IFF(tfo.product_type = 'BYO', fol.token_local_amount, 0)                         AS two_for_one_amount
              , IFF(two_for_one_applied > 0,
                    (fol.subtotal_excl_tariff_local_amount - fol.product_discount_local_amount) *
                    COALESCE(fol.order_date_usd_conversion_rate, 1),
                    0)                                                                           AS two_for_one_product_revenue
              , IFF(two_for_one_applied > 0, IFNULL(IFNULL(fol.subtotal_excl_tariff_local_amount, 0)
                                                        + IFNULL(fol.tariff_revenue_local_amount, 0)
                                                        - IFNULL(fol.product_discount_local_amount, 0)
                                                        - IFNULL(fol.non_cash_credit_local_amount, 0)
    ,
                                                    0),
                    0)                                                                           AS two_for_one_product_revenue_with_tariff
              , IFF(two_for_one_applied > 0,
                    COALESCE(fol.reporting_landed_cost_local_amount, 0) *
                    COALESCE(fol.order_date_usd_conversion_rate, 1), 0)                          AS two_for_one_cogs
              , IFF(two_for_one_applied > 0, fol.product_discount_local_amount *
                                             COALESCE(fol.order_date_usd_conversion_rate, 1), 0) AS two_for_one_discount
FROM edw_prod.data_model_jfb.fact_order_line fol
         JOIN reporting_prod.gfb.vw_store st
              ON st.store_id = fol.store_id
         JOIN edw_prod.data_model_jfb.dim_order_status dos
              ON dos.order_status_key = fol.order_status_key
         JOIN edw_prod.data_model_jfb.dim_order_line_status dols
              ON dols.order_line_status_key = fol.order_line_status_key
         JOIN edw_prod.data_model_jfb.dim_order_membership_classification domc
              ON domc.order_membership_classification_key = fol.order_membership_classification_key
         JOIN edw_prod.data_model_jfb.dim_product dp
              ON dp.product_id = fol.product_id
         JOIN edw_prod.data_model_jfb.dim_product_type dpt
              ON dpt.product_type_key = fol.product_type_key
                  AND
                 dpt.product_type_name NOT IN ('Gift Certificate', 'Membership Gift', 'Membership Reward Points Item')
         JOIN edw_prod.data_model_jfb.dim_customer dc
              ON dc.customer_id = fol.customer_id
                  AND dc.is_test_customer = 0
         JOIN edw_prod.data_model_jfb.fact_order fo
              ON fo.order_id = fol.order_id
         JOIN edw_prod.data_model_jfb.dim_order_processing_status dops
              ON dops.order_processing_status_key = fo.order_processing_status_key
         JOIN edw_prod.data_model_jfb.dim_order_sales_channel dosc
              ON dosc.order_sales_channel_key = fol.order_sales_channel_key
                  AND dosc.is_border_free_order = 0
                  AND dosc.is_ps_order = 0
                  AND dosc.is_test_order = 0
         JOIN edw_prod.data_model_jfb.dim_payment dpm
              ON dpm.payment_key = fo.payment_key
         LEFT JOIN _clearance_skus mcsl
                   ON mcsl.product_price_history_key = fol.product_price_history_key
                       AND
                      (CAST(fol.shipped_local_datetime AS DATE) >= mcsl.effective_start_datetime AND
                       CAST(fol.shipped_local_datetime AS DATE) <=
                       COALESCE(mcsl.effective_end_datetime, CURRENT_DATE()))
         LEFT JOIN lake_view.sharepoint.gfb_lead_only_product lop
                   ON LOWER(lop.brand) = LOWER(st.store_brand)
                       AND LOWER(lop.region) = LOWER(st.store_region)
                       AND LOWER(lop.product_sku) = LOWER(dp.product_sku)
                       AND
                      (CAST(fol.shipped_local_datetime AS DATE) >= lop.date_added AND
                       CAST(fol.shipped_local_datetime AS DATE) <= COALESCE(lop.date_removed, CURRENT_DATE()))
         LEFT JOIN _order_line_promo_id old_1
                   ON old_1.order_line_id = fol.order_line_id
                       AND old_1.promo_rank = 1
         LEFT JOIN lake_jfb_view.ultra_merchant.promo prom_1
                   ON prom_1.promo_id = old_1.promo_id
         LEFT JOIN lake_jfb_view.ultra_cms.ui_promo_management_promos up_1
                   ON CAST(up_1.ui_promo_management_promo_id AS VARCHAR(100)) = SPLIT_PART(prom_1.code, '_', -1)
         LEFT JOIN _order_line_promo_id old_2
                   ON old_2.order_line_id = fol.order_line_id
                       AND old_2.promo_rank = 2
         LEFT JOIN lake_jfb_view.ultra_merchant.promo prom_2
                   ON prom_2.promo_id = old_2.promo_id
         LEFT JOIN lake_jfb_view.ultra_cms.ui_promo_management_promos up_2
                   ON CAST(up_2.ui_promo_management_promo_id AS VARCHAR(100)) = SPLIT_PART(prom_2.code, '_', -1)
         LEFT JOIN edw_prod.data_model_jfb.dim_warehouse dw
                   ON dw.warehouse_id = fol.warehouse_id
         LEFT JOIN edw_prod.data_model_jfb.dim_bundle_component_history bch
                   ON bch.bundle_component_history_key = fol.bundle_component_history_key
         LEFT JOIN edw_prod.data_model_jfb.dim_order_product_source ops
                   ON ops.order_product_source_key = fol.order_product_source_key
         LEFT JOIN _two_for_one_bundle tfo
                   ON tfo.product_id = fol.bundle_product_id
WHERE dos.order_status = 'Success'
  AND dols.order_line_status != 'Cancelled'
  AND dosc.order_classification_l1 IN ('Billing Order', 'Exchange', 'Product Order', 'Reship')
  AND COALESCE(fol.shipped_local_datetime, fol.payment_transaction_local_datetime) IS NOT NULL
  AND ship_date >= $start_date;


CREATE OR REPLACE TEMPORARY TABLE _chargeback AS
SELECT fo.order_id
     , MAX(CAST(fc.chargeback_datetime AS DATE))                                             AS chargeback_date
     , SUM(fc.chargeback_local_amount * COALESCE(fc.chargeback_date_usd_conversion_rate, 1)) AS total_chargeback_amount
FROM edw_prod.data_model_jfb.fact_chargeback fc
         JOIN reporting_prod.gfb.vw_store st
              ON st.store_id = fc.store_id
         JOIN edw_prod.data_model_jfb.fact_order fo
              ON fo.order_id = fc.order_id
WHERE fo.order_id IN (
    SELECT DISTINCT old.order_id
    FROM _order_line_detail_place_date old
)
GROUP BY fo.order_id;


CREATE OR REPLACE TEMPORARY TABLE _refund AS
SELECT fo.order_id
     , MAX(CAST(fr.refund_completion_local_datetime AS DATE))          AS refund_date
     , SUM(fr.refund_total_local_amount *
           COALESCE(fr.refund_completion_date_usd_conversion_rate, 1)) AS total_refund_amount
     , SUM(CASE
               WHEN drpm.refund_payment_method != 'Store Credit'
                   THEN fr.refund_total_local_amount * COALESCE(fr.refund_completion_date_usd_conversion_rate, 1)
               ELSE 0 END)                                             AS total_refund_cash_amount
     , SUM(CASE
               WHEN drpm.refund_payment_method = 'Store Credit'
                   THEN fr.refund_total_local_amount * COALESCE(fr.refund_completion_date_usd_conversion_rate, 1)
               ELSE 0 END)                                             AS total_refund_credit_amount
FROM edw_prod.data_model_jfb.fact_refund fr
         JOIN reporting_prod.gfb.vw_store st
              ON st.store_id = fr.store_id
         JOIN edw_prod.data_model_jfb.dim_refund_status drs
              ON drs.refund_status_key = fr.refund_status_key
         JOIN edw_prod.data_model_jfb.dim_refund_payment_method drpm
              ON drpm.refund_payment_method_key = fr.refund_payment_method_key
         JOIN edw_prod.data_model_jfb.fact_order fo
              ON fo.order_id = fr.order_id
WHERE drs.refund_status = 'Refunded'
  AND fo.order_id IN (
    SELECT DISTINCT old.order_id
    FROM _order_line_detail_place_date old
)
GROUP BY fo.order_id;


CREATE OR REPLACE TEMPORARY TABLE _return_detail AS
SELECT a.*
FROM (
         SELECT fol.order_line_id
              , CAST(frl.return_completion_local_datetime AS DATE)                                                            AS return_date
              , drr.return_reason
              , RANK() OVER (PARTITION BY fol.order_line_id ORDER BY CAST(frl.return_completion_local_datetime AS DATE) DESC) AS return_line_rank

              , SUM(frl.return_item_quantity)                                                                                 AS total_return_unit
              , SUM((frl.return_subtotal_local_amount - frl.return_discount_local_amount) *
                    COALESCE(frl.return_receipt_date_usd_conversion_rate, 1))                                                 AS total_return_dollars
              , SUM((frl.estimated_returned_product_cost_local_amount_resaleable) *
                    COALESCE(frl.return_receipt_date_usd_conversion_rate, 1))                                                 AS total_refund_cost
         FROM edw_prod.data_model_jfb.fact_return_line frl
                  JOIN _order_line_detail_place_date olp
                       ON olp.order_line_id = frl.order_line_id
                  JOIN reporting_prod.gfb.vw_store st
                       ON st.store_id = frl.store_id
                  LEFT JOIN edw_prod.data_model_jfb.dim_return_condition drc
                            ON drc.return_condition_key = frl.return_condition_key
                  JOIN edw_prod.data_model_jfb.dim_product dp
                       ON dp.product_id = frl.product_id
                  JOIN edw_prod.data_model_jfb.fact_order_line fol
                       ON fol.order_line_id = frl.order_line_id
                  JOIN edw_prod.data_model_jfb.dim_product_type dpt
                       ON dpt.product_type_key = fol.product_type_key
                           AND dpt.product_type_name NOT IN
                               ('Gift Certificate', 'Membership Gift', 'Membership Reward Points Item')
                           AND dpt.is_free = 0
                  JOIN edw_prod.data_model_jfb.dim_customer dc
                       ON dc.customer_id = frl.customer_id
                           AND dc.is_test_customer = 0
                  LEFT JOIN edw_prod.data_model_jfb.dim_return_reason drr
                            ON drr.return_reason_id = frl.return_reason_id
                  JOIN edw_prod.data_model_jfb.dim_order_sales_channel dosc
                       ON dosc.order_sales_channel_key = fol.order_sales_channel_key
                           AND dosc.is_test_order = 0
                  JOIN edw_prod.data_model_jfb.dim_return_status drs
                       ON drs.return_status_key = frl.return_status_key
         WHERE (drs.return_status = 'Resolved' OR (dp.membership_brand_id IN (10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,25,26,27,28,29) AND drs.return_status IN ('Resolved','Unknown')))
           AND drc.return_condition != 'Damaged'
           AND dosc.order_classification_l1 IN ('Exchange', 'Product Order', 'Reship')
         GROUP BY fol.order_line_id
                , CAST(frl.return_completion_local_datetime AS DATE)
                , drr.return_reason
     ) a
WHERE a.return_line_rank = 1
;


CREATE OR REPLACE TEMPORARY TABLE _address AS
SELECT DISTINCT fo.order_id
              , sa.country_code     AS shipping_country
              , sa.state            AS shipping_state
              , sa.street_address_1 AS shipping_address
              , sa.city             AS shipping_city
              , sa.zip_code         AS shipping_zip_code
              , ba.country_code     AS billing_country
              , ba.state            AS billing_state
              , ba.street_address_1 AS billing_address
              , ba.city             AS billing_city
              , ba.zip_code         AS billing_zip_code
FROM edw_prod.data_model_jfb.fact_order fo
         JOIN reporting_prod.gfb.vw_store st
              ON st.store_id = fo.store_id
         LEFT JOIN edw_prod.data_model_jfb.dim_address sa
                   ON sa.address_id = fo.shipping_address_id
         LEFT JOIN edw_prod.data_model_jfb.dim_address ba
                   ON ba.address_id = fo.billing_address_id
WHERE CAST(fo.order_local_datetime AS DATE) >= DATEADD(MONTH, -6, $start_date);


DELETE
FROM reporting_prod.gfb.gfb_order_line_data_set_place_date
WHERE order_date >= $start_date;

INSERT INTO reporting_prod.gfb.gfb_order_line_data_set_place_date
SELECT old.business_unit
      ,old.region
      ,old.country
      ,old.order_id
      ,old.order_line_id
      ,old.session_id
      ,old.customer_id
      ,old.product_id
      ,old.product_type
      ,old.product_type_name
      ,old.credit_order_type
      ,old.product_sku
      ,old.sku
      ,old.product_name
      ,old.dp_color
      ,old.dp_size
      ,old.order_date
      ,old.order_classification
      ,old.order_type
      ,old.clearance_flag
      ,old.clearance_price
      ,old.lead_only_flag
      ,old.promo_id_1
      ,old.promo_code_1
      ,old.promo_id_2
      ,old.promo_code_2
      ,old.is_prepaid_creditcard
      ,old.creditcard_type
      ,old.warehouse_name
      ,old.warehouse_city
      ,old.warehouse_state
      ,old.warehouse_country
      ,old.bundle_product_id
      ,old.bundle_component_product_id
      ,old.bundle_order_line_id
      ,old.order_product_source_name
      ,old.bundle_product_id_jfsd
      ,old.warehouse_id
      ,old.total_qty_sold
      ,old.total_product_revenue
      ,old.total_cogs
      ,old.total_cogs_without_tariff
      ,old.total_discount
      ,old.total_tax
      ,old.order_line_subtotal
      ,old.order_subtotal
      ,old.order_line_revenue_percent
      ,old.total_cash_credit_amount
      ,old.total_non_cash_credit_amount
      ,old.order_shipping_revenue
      ,old.order_shipping_cost
      ,old.cash_collected_amount
      ,old.total_product_revenue_with_tariff
      ,old.tokens_applied
      ,old.token_local_amount
      ,old.token_product_revenue
      ,old.token_product_revenue_with_tariff
      ,old.token_cogs
      ,old.token_discount
      ,old.two_for_one_applied
      ,old.two_for_one_amount
      ,old.two_for_one_product_revenue
      ,old.two_for_one_product_revenue_with_tariff
      ,old.two_for_one_cogs
      ,old.two_for_one_discount

     , cb.chargeback_date
     , cb.total_chargeback_amount * old.order_line_revenue_percent    AS total_chargeback_amount

     , rf.refund_date
     , rf.total_refund_amount * old.order_line_revenue_percent        AS total_refund_amount
     , rf.total_refund_cash_amount * old.order_line_revenue_percent   AS total_refund_cash_amount
     , rf.total_refund_credit_amount * old.order_line_revenue_percent AS total_refund_credit_amount

     , rd.return_date
     , rd.return_reason
     , rd.total_return_unit
     , rd.total_return_dollars
     , rd.total_refund_cost

     , old.order_shipping_revenue * old.order_line_revenue_percent    AS total_shipping_revenue
     , old.order_shipping_cost * old.order_line_revenue_percent       AS total_shipping_cost

     , a.shipping_country
     , a.shipping_state
     , a.shipping_address
     , a.shipping_city
     , a.shipping_zip_code
     , a.billing_country
     , a.billing_state
     , a.billing_address
     , a.billing_city
     , a.billing_zip_code
FROM _order_line_detail_place_date old
         LEFT JOIN _chargeback cb
                   ON cb.order_id = old.order_id
         LEFT JOIN _refund rf
                   ON rf.order_id = old.order_id
         LEFT JOIN _return_detail rd
                   ON rd.order_line_id = old.order_line_id
         LEFT JOIN _address a
                   ON a.order_id = old.order_id;


DELETE
FROM reporting_prod.gfb.gfb_order_line_data_set_place_date a
WHERE a.order_classification = 'product order'
  AND a.order_date = '1900-01-01';


DELETE
FROM reporting_prod.gfb.gfb_order_line_data_set_ship_date
WHERE ship_date >= $start_date;


INSERT INTO reporting_prod.gfb.gfb_order_line_data_set_ship_date
SELECT old.business_unit
      ,old.region
      ,old.country
      ,old.order_id
      ,old.order_line_id
      ,old.session_id
      ,old.customer_id
      ,old.product_id
      ,old.product_type
      ,old.credit_order_type
      ,old.product_sku
      ,old.sku
      ,old.product_name
      ,old.dp_color
      ,old.dp_size
      ,old.ship_date
      ,old.order_classification
      ,old.order_type
      ,old.clearance_flag
      ,old.clearance_price
      ,old.lead_only_flag
      ,old.promo_id_1
      ,old.promo_code_1
      ,old.promo_id_2
      ,old.promo_code_2
      ,old.is_prepaid_creditcard
      ,old.creditcard_type
      ,old.warehouse_name
      ,old.warehouse_city
      ,old.warehouse_state
      ,old.warehouse_country
      ,old.bundle_product_id
      ,old.bundle_component_product_id
      ,old.bundle_order_line_id
      ,old.order_product_source_name
      ,old.bundle_product_id_jfsd
      ,old.warehouse_id
      ,old.total_qty_sold
      ,old.total_product_revenue
      ,old.total_cogs
      ,old.total_cogs_without_tariff
      ,old.total_discount
      ,old.total_tax
      ,old.order_line_subtotal
      ,old.order_subtotal
      ,old.order_line_revenue_percent
      ,old.total_cash_credit_amount
      ,old.total_non_cash_credit_amount
      ,old.order_shipping_revenue
      ,old.order_shipping_cost
      ,old.cash_collected_amount
      ,old.total_product_revenue_with_tariff
      ,old.tokens_applied
      ,old.token_local_amount
      ,old.token_product_revenue
      ,old.token_product_revenue_with_tariff
      ,old.token_cogs
      ,old.token_discount
      ,old.two_for_one_applied
      ,old.two_for_one_amount
      ,old.two_for_one_product_revenue
      ,old.two_for_one_product_revenue_with_tariff
      ,old.two_for_one_cogs
      ,old.two_for_one_discount

     , cb.chargeback_date
     , cb.total_chargeback_amount * old.order_line_revenue_percent    AS total_chargeback_amount

     , rf.refund_date
     , rf.total_refund_amount * old.order_line_revenue_percent        AS total_refund_amount
     , rf.total_refund_cash_amount * old.order_line_revenue_percent   AS total_refund_cash_amount
     , rf.total_refund_credit_amount * old.order_line_revenue_percent AS total_refund_credit_amount

     , rd.return_date
     , rd.return_reason
     , rd.total_return_unit
     , rd.total_return_dollars
     , rd.total_refund_cost

     , old.order_shipping_revenue * old.order_line_revenue_percent    AS total_shipping_revenue
     , old.order_shipping_cost * old.order_line_revenue_percent       AS total_shipping_cost

     , a.shipping_country
     , a.shipping_state
     , a.shipping_address
     , a.shipping_city
     , a.shipping_zip_code
     , a.billing_country
     , a.billing_state
     , a.billing_address
     , a.billing_city
     , a.billing_zip_code
FROM _order_line_detail_ship_date old
         LEFT JOIN _chargeback cb
                   ON cb.order_id = old.order_id
         LEFT JOIN _refund rf
                   ON rf.order_id = old.order_id
         LEFT JOIN _return_detail rd
                   ON rd.order_line_id = old.order_line_id
         LEFT JOIN _address a
                   ON a.order_id = old.order_id;
