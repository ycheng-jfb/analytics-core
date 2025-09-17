SET target_table = 'reporting_prod.shared.sessions_order_stg';
SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
SET is_full_refresh = (SELECT IFF(IFNULL(public.udf_get_watermark($target_table, NULL), '1900-01-01') =
                                  '1900-01-01'::TIMESTAMP_LTZ(3), TRUE,
                                  FALSE));

SET warehouse_to_be_used = IFF($is_full_refresh, 'da_wh_adhoc_large', CURRENT_WAREHOUSE());
USE WAREHOUSE IDENTIFIER ($warehouse_to_be_used);

MERGE INTO public.meta_table_dependency_watermark AS w
    USING
        (SELECT $target_table AS table_name,
                t.dependent_table_name,
                new_high_watermark_datetime
         FROM (SELECT -- For self table
                      NULL                      AS dependent_table_name,
                      MAX(meta_update_datetime) AS new_high_watermark_datetime
               FROM shared.sessions_order_stg
               UNION ALL
               SELECT 'reporting_prod.shared.sessions_by_platform' AS dependent_table_name,
                      MAX(meta_update_datetime)                        AS new_high_watermark_datetime
               FROM shared.sessions_by_platform
               UNION ALL
               SELECT 'edw_prod.data_model.fact_order' AS dependent_table_name,
                      MAX(meta_update_datetime)        AS new_high_watermark_datetime
               FROM edw_prod.data_model.fact_order) AS t
         ORDER BY COALESCE(t.dependent_table_name, '')) AS s
    ON w.table_name = s.table_name AND w.dependent_table_name IS NOT DISTINCT FROM s.dependent_table_name
        AND EQUAL_NULL(w.dependent_table_name, s.dependent_table_name)
    WHEN MATCHED
        AND NOT EQUAL_NULL(w.new_high_watermark_datetime, s.new_high_watermark_datetime)
        THEN UPDATE SET
        w.new_high_watermark_datetime = s.new_high_watermark_datetime,
        w.meta_update_datetime = CURRENT_TIMESTAMP::timestamp_ltz(3)
    WHEN NOT MATCHED
        THEN INSERT (
                     table_name,
                     dependent_table_name,
                     high_watermark_datetime,
                     new_high_watermark_datetime
        )
        VALUES (s.table_name,
                s.dependent_table_name,
                '1900-01-01'::timestamp_ltz,
                s.new_high_watermark_datetime);

SET wm_reporting_prod_shared_sessions_by_platform = public.udf_get_watermark($target_table,
                                                                             'reporting_prod.shared.sessions_by_platform');
SET wm_edw_prod_data_model_fact_order = public.udf_get_watermark($target_table, 'edw_prod.data_model.fact_order');

CREATE OR REPLACE TEMP TABLE _sessions_order_stg__base
(
    session_id NUMBER
);

-- full refresh
INSERT INTO _sessions_order_stg__base
SELECT session_id
FROM shared.sessions_by_platform
WHERE $is_full_refresh = TRUE;

-- incremental refresh
INSERT INTO _sessions_order_stg__base
SELECT DISTINCT session_id
FROM (SELECT session_id
      FROM shared.sessions_by_platform
      WHERE meta_update_datetime > $wm_reporting_prod_shared_sessions_by_platform
      UNION ALL
      SELECT session_id
      FROM edw_prod.data_model.fact_order
      WHERE meta_update_datetime > $wm_edw_prod_data_model_fact_order
        AND session_id IS NOT NULL) AS incr
WHERE $is_full_refresh = FALSE;

CREATE OR REPLACE TEMP TABLE _sessions_order_stg__stg AS
SELECT s.customer_id,
                  s.visitor_id,
                  fo.session_id,
                  s.membership_state,
                  s.user_segment,
                  ds.store_type,
                  s.session_store_id,
                  s.session_platform,
                  ds.store_id                                                                          AS order_store_id,
                  CASE
                      WHEN osc.order_sales_channel_l2 = 'Mobile App Order' THEN 'Mobile App'
                      WHEN osc.order_sales_channel_l2 = 'Web Order' AND s.session_platform IN ('Mobile', 'Unknown')
                          THEN 'Mobile'
                      WHEN osc.order_sales_channel_l2 = 'Web Order' AND s.session_platform IN ('Desktop', 'Tablet')
                          THEN 'Desktop'
                      ELSE s.session_platform END                                                      AS order_platform,
                  s.operating_system,
                  CAST(s.session_local_datetime AS DATE)                                               AS date,
                  s.session_month_date,
                  s.session_local_datetime,
                  fo.order_local_datetime,
                  fo.order_id,
                  os.order_status,
                  IFF(mem.membership_order_type_l1 = 'Activating VIP', 'Activating', 'Non-Activating') AS is_activating,
                  ops.order_processing_status,
                  IFF(mem.membership_order_type_l1 = 'Activating VIP'
                          AND DATEDIFF(HOUR, s.registration_local_datetime, fo.order_local_datetime) <= 1, 1,
                      0)                                                                               AS vip_activations_1hr,
                  IFF(mem.membership_order_type_l1 = 'Activating VIP'
                          AND DATEDIFF(HOUR, s.registration_local_datetime, fo.order_local_datetime) <= 24, 1,
                      0)                                                                               AS vip_activations_24hr,
                  IFF(mem.membership_order_type_l1 = 'Activating VIP'
                          AND DATEDIFF(DAY, s.registration_local_datetime, fo.order_local_datetime) <= 7, 1,
                      0)                                                                               AS vip_activations_7_day,
                  IFF(mem.membership_order_type_l1 = 'Activating VIP'
                          AND DATEDIFF(MONTH, s.registration_local_datetime, fo.order_local_datetime) <= 1, 1,
                      0)                                                                               AS vip_activations_1_month,
                  SUM(IFF(mem.membership_order_type_l1 = 'Activating VIP', 1, 0))                      AS vip_activations,
                  RANK() OVER (PARTITION BY s.customer_id,order_platform ORDER BY fo.order_id)         AS rnk_order_by_store_type,
                  1                                                                                    AS orders,
                  SUM(fo.unit_count)                                                                   AS units,
                  SUM(fo.product_subtotal_local_amount * fo.order_date_usd_conversion_rate)            AS subtotal,
                  SUM((fo.product_discount_local_amount + fo.shipping_discount_local_amount) *
                      fo.order_date_usd_conversion_rate)                                               AS discount,
                  SUM(fo.shipping_revenue_local_amount * fo.order_date_usd_conversion_rate)            AS shipping_revenue,
                  SUM(fo.payment_transaction_local_amount *
                      COALESCE(fo.payment_transaction_date_usd_conversion_rate, 1))                    AS payment_transaction,
                  SUM(fo.tax_local_amount * fo.order_date_usd_conversion_rate)                         AS tax,
                  SUM(fo.product_gross_revenue_local_amount *
                      fo.order_date_usd_conversion_rate)                                               AS product_gross_revenue,
                  SUM(fo.cash_gross_revenue_local_amount * fo.order_date_usd_conversion_rate)          AS cash_collected,
                  SUM(fo.cash_credit_local_amount * fo.order_date_usd_conversion_rate)                 AS cash_credit,
                  SUM(fo.non_cash_credit_local_amount * fo.order_date_usd_conversion_rate)             AS non_cash_credit,
                  SUM(fo.estimated_landed_cost_local_amount *
                      fo.order_date_usd_conversion_rate)                                               AS total_cost_product,
                  SUM(fo.shipping_cost_local_amount * fo.order_date_usd_conversion_rate)               AS estimated_shipping_cost,
                  SUM(fo.product_margin_pre_return_local_amount *
                      fo.order_date_usd_conversion_rate)                                               AS product_margin_pre_return,
                  SUM(fo.cash_membership_credit_count)                                                 AS membership_credits_redeemed_count,
                  SUM(fo.cash_membership_credit_local_amount *
                      fo.order_date_usd_conversion_rate)                                               AS membership_credits_redeemed_amount,
                  SUM(fo.token_count)                                                                  AS membership_token_redeemed_count,
                  SUM(fo.token_local_amount * fo.order_date_usd_conversion_rate)                       AS membership_token_redeemed_amount,
                  $execution_start_time                                                                AS meta_create_datetime,
                  $execution_start_time                                                                AS meta_update_datetime
           FROM _sessions_order_stg__base base
                    JOIN shared.sessions_by_platform s
                         ON base.session_id = s.session_id
                    JOIN edw_prod.data_model.fact_order AS fo
                         ON fo.session_id = s.session_id
                    JOIN edw_prod.data_model.dim_order_status os
                         ON os.order_status_key = fo.order_status_key
                    JOIN edw_prod.data_model.dim_order_sales_channel osc
                         ON osc.order_sales_channel_key = fo.order_sales_channel_key
                    JOIN edw_prod.data_model.dim_order_membership_classification mem
                         ON mem.order_membership_classification_key = fo.order_membership_classification_key
                    JOIN edw_prod.data_model.dim_order_processing_status AS ops
                         ON ops.order_processing_status_key = fo.order_processing_status_key
                    JOIN edw_prod.data_model.dim_store AS ds
                         ON ds.store_id = fo.store_id
           WHERE os.order_status IN ('Success', 'Pending')
             AND osc.order_sales_channel_l1 = 'Online Order'
             AND osc.order_classification_l1 = 'Product Order'
           GROUP BY s.customer_id,
                    s.visitor_id,
                    fo.session_id,
                    s.membership_state,
                    s.user_segment,
                    ds.store_type,
                    s.session_store_id,
                    s.session_platform,
                    ds.store_id,
                    CASE
                        WHEN osc.order_sales_channel_l2 = 'Mobile App Order' THEN 'Mobile App'
                        WHEN osc.order_sales_channel_l2 = 'Web Order' AND s.session_platform IN ('Mobile', 'Unknown')
                            THEN 'Mobile'
                        WHEN osc.order_sales_channel_l2 = 'Web Order' AND s.session_platform IN ('Desktop', 'Tablet')
                            THEN 'Desktop'
                        ELSE s.session_platform END,
                    s.operating_system,
                    CAST(s.session_local_datetime AS DATE),
                    s.session_month_date,
                    s.session_local_datetime,
                    fo.order_local_datetime,
                    fo.order_id,
                    os.order_status,
                    IFF(mem.membership_order_type_l1 = 'Activating VIP', 'Activating', 'Non-Activating'),
                    ops.order_processing_status,
                    IFF(mem.membership_order_type_l1 = 'Activating VIP'
                            AND DATEDIFF(HOUR, s.registration_local_datetime, fo.order_local_datetime) <= 1, 1, 0),
                    IFF(mem.membership_order_type_l1 = 'Activating VIP'
                            AND DATEDIFF(HOUR, s.registration_local_datetime, fo.order_local_datetime) <= 24, 1, 0),
                    IFF(mem.membership_order_type_l1 = 'Activating VIP'
                            AND DATEDIFF(DAY, s.registration_local_datetime, fo.order_local_datetime) <= 7, 1, 0),
                    IFF(mem.membership_order_type_l1 = 'Activating VIP'
                            AND DATEDIFF(MONTH, s.registration_local_datetime, fo.order_local_datetime) <= 1, 1,
                        0);

DELETE
FROM shared.sessions_order_stg
WHERE session_id IN (SELECT session_id FROM _sessions_order_stg__base);

INSERT INTO shared.sessions_order_stg (customer_id,
                                       visitor_id,
                                       session_id,
                                       membership_state,
                                       user_segment,
                                       store_type,
                                       session_store_id,
                                       session_platform,
                                       order_store_id,
                                       order_platform,
                                       operating_system,
                                       date,
                                       session_month_date,
                                       session_local_datetime,
                                       order_local_datetime,
                                       order_id,
                                       order_status,
                                       is_activating,
                                       order_processing_status,
                                       vip_activations_1hr,
                                       vip_activations_24hr,
                                       vip_activations_7_day,
                                       vip_activations_1_month,
                                       vip_activations,
                                       rnk_order_by_store_type,
                                       orders,
                                       units,
                                       subtotal,
                                       discount,
                                       shipping_revenue,
                                       payment_transaction,
                                       tax,
                                       product_gross_revenue,
                                       cash_collected,
                                       cash_credit,
                                       non_cash_credit,
                                       total_cost_product,
                                       estimated_shipping_cost,
                                       product_margin_pre_return,
                                       membership_credits_redeemed_count,
                                       membership_credits_redeemed_amount,
                                       membership_token_redeemed_count,
                                       membership_token_redeemed_amount,
                                       meta_create_datetime,
                                       meta_update_datetime)
SELECT customer_id,
       visitor_id,
       session_id,
       membership_state,
       user_segment,
       store_type,
       session_store_id,
       session_platform,
       order_store_id,
       order_platform,
       operating_system,
       date,
       session_month_date,
       session_local_datetime,
       order_local_datetime,
       order_id,
       order_status,
       is_activating,
       order_processing_status,
       vip_activations_1hr,
       vip_activations_24hr,
       vip_activations_7_day,
       vip_activations_1_month,
       vip_activations,
       rnk_order_by_store_type,
       orders,
       units,
       subtotal,
       discount,
       shipping_revenue,
       payment_transaction,
       tax,
       product_gross_revenue,
       cash_collected,
       cash_credit,
       non_cash_credit,
       total_cost_product,
       estimated_shipping_cost,
       product_margin_pre_return,
       membership_credits_redeemed_count,
       membership_credits_redeemed_amount,
       membership_token_redeemed_count,
       membership_token_redeemed_amount,
       meta_create_datetime,
       meta_update_datetime
FROM _sessions_order_stg__stg;

UPDATE public.meta_table_dependency_watermark
SET high_watermark_datetime = new_high_watermark_datetime,
    meta_update_datetime    = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3)
WHERE table_name = $target_table;
