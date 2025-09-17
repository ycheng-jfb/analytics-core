SET start_date = DATE_TRUNC(MONTH, DATEADD(MONTH, -24, CURRENT_DATE()));

CREATE OR REPLACE TEMPORARY TABLE _order_line_detail_place_date AS
SELECT DISTINCT UPPER(st.store_brand)                           AS business_unit
              , UPPER(st.store_region)                          AS region
              , (CASE
                     WHEN dc.specialty_country_code = 'GB' THEN 'UK'
                     WHEN dc.specialty_country_code != 'Unknown' THEN dc.specialty_country_code
                     ELSE st.store_country END)                 AS country
              , fol.order_id
              , fol.order_line_id
              , fo.session_id
              , dc.customer_id
              , dp.product_id
              , dp.product_type
              , dpt.product_type_name
              , CASE
                    WHEN IFNULL(fol.token_count, 0) = 0 THEN 'Only Cash'
                    WHEN (IFNULL(fol.subtotal_excl_tariff_local_amount, 0) = IFNULL(fol.token_local_amount, 0))
                        THEN 'Credits Only'
                    ELSE 'Cash & Credits' END                   AS credit_order_type
              , dp.product_sku
              , dp.sku
              , dp.product_name
              , dp.color                                        AS dp_color
              , LOWER(dp.size)                                  AS dp_size
              , CAST(fol.order_local_datetime AS DATE)          AS order_date
              , (CASE
                     WHEN dosc.order_classification_l1 = 'Product Order' THEN 'product order'
                     WHEN dosc.order_classification_l1 = 'Billing Order' THEN 'credit billing'
                     WHEN dosc.order_classification_l1 = 'Exchange' THEN 'exchange'
                     WHEN dosc.order_classification_l1 = 'Reship' THEN 'reship'
    END)                                                        AS order_classification
              , (CASE
                     WHEN domc.membership_order_type_l2 = 'Guest' THEN 'ecom'
                     WHEN domc.membership_order_type_l1 = 'Activating VIP' THEN 'vip activating'
                     ELSE 'vip repeat' END)                     AS order_type
              , (CASE
                     WHEN fo.subtotal_excl_tariff_local_amount = 0 THEN 0
                     ELSE fol.subtotal_excl_tariff_local_amount / fo.subtotal_excl_tariff_local_amount
    END)                                                        AS order_line_revenue_percent
              , fo.shipping_cost_local_amount *
                COALESCE(fol.order_date_usd_conversion_rate, 1) AS order_shipping_cost
              , fo.estimated_shipping_supplies_cost_local_amount *
                COALESCE(fol.order_date_usd_conversion_rate, 1) AS shipping_supplies_cost
              , fol.item_quantity                               AS total_qty_sold
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
         LEFT JOIN edw_prod.data_model_jfb.dim_warehouse dw
                   ON dw.warehouse_id = fol.warehouse_id
         LEFT JOIN edw_prod.data_model_jfb.dim_bundle_component_history bch
                   ON bch.bundle_component_history_key = fol.bundle_component_history_key
         LEFT JOIN edw_prod.data_model_jfb.dim_order_product_source ops
                   ON ops.order_product_source_key = fol.order_product_source_key
WHERE (
            dos.order_status = 'Success'
        OR
            (dos.order_status = 'Pending' AND
             dops.order_processing_status IN ('FulFillment (Batching)', 'FulFillment (In Progress)', 'Placed'))
    )
  AND dols.order_line_status != 'Cancelled'
  AND dosc.order_classification_l1 IN ('Billing Order', 'Exchange', 'Product Order', 'Reship')
  AND fol.order_local_datetime IS NOT NULL
  AND CAST(fol.order_local_datetime AS DATE) >= $start_date;

CREATE OR REPLACE TEMPORARY TABLE _return_detail AS
SELECT a.*
FROM (
         SELECT fol.order_line_id
              , CAST(frl.return_completion_local_datetime AS DATE)                                                            AS return_date
              , drr.return_reason
              , SUM(frl.return_item_quantity)                                                                                 AS return_quantity
              , RANK() OVER (PARTITION BY fol.order_line_id ORDER BY CAST(frl.return_completion_local_datetime AS DATE) DESC) AS return_line_rank
              , SUM((frl.estimated_return_shipping_cost_local_amount) *
                    COALESCE(frl.return_receipt_date_usd_conversion_rate, 1))                                                 AS total_return_shipping_cost
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
         WHERE drs.return_status = 'Resolved'
           AND drc.return_condition != 'Damaged'
           AND dosc.order_classification_l1 IN ('Exchange', 'Product Order', 'Reship')
         GROUP BY fol.order_line_id
                , CAST(frl.return_completion_local_datetime AS DATE)
                , drr.return_reason
     ) a
WHERE a.return_line_rank = 1
;

CREATE OR REPLACE TEMPORARY TABLE _main_brand AS
SELECT id.product_sku                                                                          AS product_sku
     , FIRST_VALUE(main_brand) OVER (PARTITION BY id.product_sku ORDER BY inventory_date DESC) AS main_brand
FROM gfb.gfb_inventory_data_set id
         JOIN gfb.merch_dim_product mdp
              ON id.business_unit = mdp.business_unit
                  AND id.region = mdp.region
                  AND id.country = mdp.country
                  AND id.product_sku = mdp.product_sku
WHERE id.business_unit ILIKE ANY ('JUSTFAB', 'SHOEDAZZLE', 'FABKIDS')
  AND mdp.sub_brand IN ('JFB')
UNION
SELECT product_sku, sub_brand AS main_brand
FROM gfb.merch_dim_product
WHERE sub_brand NOT IN ('JFB');

CREATE OR REPLACE TRANSIENT TABLE gfb.gfb078_shipping_return_cost_details AS
SELECT old.business_unit
     , mb.main_brand
     , old.region
     , old.country
     , old.product_sku
     , old.order_date
     , mdp.style_name
     , mdp.first_launch_date
     , mdp.season_code
     , mdp.department_detail
     , mdp.subcategory
     , mdp.color
     , SUM(CASE WHEN old.order_type = 'vip activating' THEN old.total_qty_sold ELSE 0 END)  AS activating_qty_sold
     , SUM(CASE WHEN old.order_type != 'vip activating' THEN old.total_qty_sold ELSE 0 END) AS repeat_qty_sold
     , SUM(total_qty_sold)                                                                  AS total_qty_sold
     , SUM(CASE
               WHEN old.order_type = 'vip activating' THEN rd.total_return_shipping_cost
               ELSE 0 END)                                                                  AS activating_return_shipping_cost
     , SUM(CASE
               WHEN old.order_type != 'vip activating' THEN rd.total_return_shipping_cost
               ELSE 0 END)                                                                  AS repeat_return_shipping_cost

     , SUM(rd.total_return_shipping_cost)                                                   AS total_return_shipping_cost
     , SUM(CASE
               WHEN old.order_type = 'vip activating' THEN rd.return_quantity
               ELSE 0 END)                                                                  AS activating_return_quantity
     , SUM(CASE WHEN old.order_type != 'vip activating' THEN rd.return_quantity ELSE 0 END) AS repeat_return_quantity


     , SUM(rd.return_quantity)                                                              AS return_quantity

     , SUM(CASE
               WHEN old.order_type = 'vip activating' THEN old.order_shipping_cost * old.order_line_revenue_percent
               ELSE 0 END)                                                                  AS activating_shipping_cost
     , SUM(CASE
               WHEN old.order_type != 'vip activating' THEN old.order_shipping_cost * old.order_line_revenue_percent
               ELSE 0 END)                                                                  AS repeat_shipping_cost
     , SUM(old.order_shipping_cost * old.order_line_revenue_percent)                        AS total_shipping_cost
     , SUM(CASE
               WHEN old.order_type = 'vip activating' THEN old.shipping_supplies_cost * old.order_line_revenue_percent
               ELSE 0 END)                                                                  AS activating_shipping_supplies_cost
     , SUM(CASE
               WHEN old.order_type != 'vip activating' THEN old.shipping_supplies_cost * old.order_line_revenue_percent
               ELSE 0 END)                                                                  AS repeat_shipping_supplies_cost
     , SUM(old.shipping_supplies_cost * old.order_line_revenue_percent)                     AS total_shipping_supplies_cost
FROM _order_line_detail_place_date old
         LEFT JOIN _return_detail rd
                   ON rd.order_line_id = old.order_line_id
         JOIN gfb.merch_dim_product mdp
              ON old.business_unit = mdp.business_unit
                  AND old.region = mdp.region
                  AND old.country = mdp.country
                  AND old.product_sku = mdp.product_sku
         LEFT JOIN _main_brand mb ON mb.product_sku = mdp.product_sku
WHERE old.order_classification = 'product order'
GROUP BY old.business_unit
       , mb.main_brand
       , old.region
       , old.country
       , old.product_sku
       , old.order_date
       , mdp.style_name
       , mdp.first_launch_date
       , mdp.season_code
       , mdp.department_detail
       , mdp.subcategory
       , mdp.color;
