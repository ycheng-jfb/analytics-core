CREATE OR REPLACE TEMPORARY TABLE _mm_order_info AS
SELECT customer_id,
       SUM(IFF(mdp.sub_brand = 'DREAM PAIRS', total_qty_sold, 0))      AS dream_pairs_sales,
       SUM(IFF(mdp.sub_brand = 'DREAM PAIRS KIDS', total_qty_sold, 0)) AS dream_pairs_kids_sales,
       SUM(IFF(mdp.sub_brand = 'BRUNO MARC', total_qty_sold, 0))       AS bruno_marc_sales,
       SUM(IFF(mdp.sub_brand = 'NORTIV8', total_qty_sold, 0))          AS nortiv8_sales
FROM reporting_prod.gfb.gfb_order_line_data_set_place_date ol
         JOIN reporting_prod.gfb.merch_dim_product mdp
              ON ol.business_unit = mdp.business_unit
                  AND ol.region = mdp.region
                  AND ol.country = mdp.country
                  AND ol.product_sku = mdp.product_sku
WHERE ol.order_classification = 'product order'
GROUP BY customer_id;

CREATE OR REPLACE TEMPORARY TABLE _main_data AS
SELECT cl.segment_name
     , cl.segment_owner
     , dc.customer_id
     , ds.store_brand || ' ' || ds.store_country                       AS store_reporting_name
     , dc.country_code                                                 AS customer_country
     , dc.default_state_province                                       AS customer_state_province
     , dc.default_city                                                 AS customer_city
     , dc.default_postal_code                                          AS customer_postal_code
     , clv.current_membership_status                                   AS is_active
     , fma.membership_state
     , clv.registration_date
     , DATEDIFF(DAY, clv.registration_date, clv.first_activating_date) AS days_as_lead
     , clv.first_activating_date                                       AS first_activation_date
     , clv.first_activating_cohort                                     AS first_activation_cohort
     , clv.recent_activating_cohort                                    AS current_activation_cohort
     , DATEDIFF(DAY, clv.first_activating_date, CURRENT_DATE())        AS days_since_vip_activation
     , cl.dimension_1
     , cl.dimension_2
     , cl.dimension_3
     , cl.dimension_4
     , cl.dimension_5
     , cl.dimension_6

     , clv.first_activating_product_revenue                            AS activating_order_value
     , clv.product_gross_profit                                        AS gaap_gross_margin
     , clv.product_order_count
     , clv.product_order_unit_count                                    AS product_order_units
     , clv.product_gross_revenue_excl_shipping                         AS lifetime_revenue_excl_shipping
     , clv.product_gross_revenue_incl_shipping                         AS lifetime_revenue_incl_shipping
     , clv.cash_gross_profit                                           AS cash_gross_margin
     , clv.activating_gross_margin

     , dc.email                                                        AS customer_email
     , COALESCE(m.membership_id, m1.membership_id)                     AS membership_id
     , DATEDIFF(DAY, clv.first_activating_date, CURRENT_DATE()) + 1    AS days_since_first_activation
     , (CASE
            WHEN clv.product_order_unit_count != 0
                THEN clv.product_order_return_unit_count * 1.0 / clv.product_order_unit_count
            ELSE 0 END)                                                AS return_rate
     , (CASE
            WHEN clv.product_order_count != 0
                THEN clv.product_gross_revenue_excl_shipping * 1.0 / clv.product_order_count
            ELSE 0 END)                                                AS aov
     , (CASE
            WHEN clv.repeat_product_order_count = 0 AND clv.first_activating_cohort IS NOT NULL THEN 'Activating'
            ELSE 'Repeat' END)                                         AS activating_vs_repeat
     , clv.gaap_gross_margin_decile_ltd                                AS gaap_gross_margin_decile
     , clv.cash_gross_margin_decile_ltd                                AS cash_gross_margin_decile
     , dc.birth_year
FROM lake_view.sharepoint.gfb_customer_lookup cl
         LEFT JOIN lake_jfb_view.ultra_merchant.membership m
                   ON m.membership_id = cl.member_id
         JOIN edw_prod.data_model_jfb.dim_customer dc
              ON dc.customer_id = COALESCE(m.customer_id, cl.customer_id)
         LEFT JOIN lake_jfb_view.ultra_merchant.membership m1
                   ON m1.customer_id = dc.customer_id
         JOIN reporting_prod.gfb.gfb_dim_vip clv
              ON clv.customer_id = dc.customer_id
         JOIN edw_prod.data_model_jfb.dim_store ds
              ON ds.store_id = clv.store_id
         JOIN edw_prod.data_model_jfb.fact_membership_event fma
              ON fma.customer_id = dc.customer_id
                  AND fma.is_current = 1;


CREATE OR REPLACE TEMPORARY TABLE _last_purchase_date AS
SELECT olp.customer_id
     , MAX(olp.order_date) AS last_product_purchase_date
FROM reporting_prod.gfb.gfb_order_line_data_set_place_date olp
         JOIN _main_data md
              ON md.customer_id = olp.customer_id
WHERE olp.order_classification = 'product order'
GROUP BY olp.customer_id;


--customer shopping seg
SET start_date = DATE_TRUNC(YEAR, DATEADD(YEAR, -2, CURRENT_DATE()));
SET end_date = CURRENT_DATE();


CREATE OR REPLACE TEMPORARY TABLE _repeat_orders AS
SELECT md.customer_id
     , SUM(olp.total_cash_credit_amount + olp.total_non_cash_credit_amount)            AS credit_redeemed_amount
     , SUM(olp.total_product_revenue)                                                  AS product_revenue
     , COUNT(olp.order_id)                                                             AS total_repeat_orders
     , COUNT(DISTINCT CASE
                          WHEN olp.clearance_flag = 'clearance' THEN olp.order_id END) AS clearance_repeat_orders
FROM reporting_prod.gfb.gfb_order_line_data_set_place_date olp
         JOIN _main_data md
              ON md.customer_id = olp.customer_id
WHERE olp.order_classification = 'product order'
  AND olp.order_date >= $start_date
  AND olp.order_date < $end_date
  AND olp.order_type = 'vip repeat'
GROUP BY md.customer_id;


CREATE OR REPLACE TEMPORARY TABLE _customer_shopping_seg AS
SELECT ro.customer_id
     , (CASE
            WHEN ro.customer_id IS NOT NULL AND ro.product_revenue > 0 AND
                 ro.credit_redeemed_amount * 1.0 / ro.product_revenue >= 0.75
                THEN 1
            ELSE 0 END) AS shops_with_credit_only_flag
     , (CASE
            WHEN ro.customer_id IS NOT NULL AND ro.product_revenue > 0 AND
                 ro.credit_redeemed_amount * 1.0 / ro.product_revenue <= 0.25
                THEN 1
            ELSE 0 END) AS shops_with_cash_only_flag
     , (CASE
            WHEN ro.customer_id IS NOT NULL AND ro.product_revenue > 0 AND
                 ro.credit_redeemed_amount * 1.0 / ro.product_revenue > 0.25 AND
                 ro.credit_redeemed_amount * 1.0 / ro.product_revenue < 0.75
                THEN 1
            ELSE 0 END) AS shops_with_cash_credit_flag
     , (CASE
            WHEN ro.clearance_repeat_orders * 1.0 / ro.total_repeat_orders >= 0.75
                THEN 1
            ELSE 0 END) AS clearance_only_flag
     , (CASE
            WHEN ro.clearance_repeat_orders * 1.0 / ro.total_repeat_orders <= 0.25
                THEN 1
            ELSE 0 END) AS regular_only_flag
     , (CASE
            WHEN ro.clearance_repeat_orders * 1.0 / ro.total_repeat_orders < 0.75 AND
                 ro.clearance_repeat_orders * 1.0 / ro.total_repeat_orders > 0.25
                THEN 1
            ELSE 0 END) AS regular_clearance_flag
FROM _repeat_orders ro
WHERE ro.product_revenue > 0;


CREATE OR REPLACE TRANSIENT TABLE reporting_prod.gfb.dos_110_customer_lookup AS
SELECT md.*
     , lpd.last_product_purchase_date
     , COALESCE(css.shops_with_credit_only_flag, 0) AS shops_with_credit_only_flag
     , COALESCE(css.shops_with_cash_only_flag, 0)   AS shops_with_cash_only_flag
     , COALESCE(css.shops_with_cash_credit_flag, 0) AS shops_with_cash_credit_flag
     , COALESCE(css.clearance_only_flag, 0)         AS clearance_only_flag
     , COALESCE(css.regular_only_flag, 0)           AS regular_only_flag
     , COALESCE(css.regular_clearance_flag, 0)      AS regular_clearance_flag
     , COALESCE(moi.dream_pairs_sales, 0)           AS dream_pairs_sales
     , COALESCE(moi.dream_pairs_kids_sales, 0)      AS dream_pairs_kids_sales
     , COALESCE(moi.bruno_marc_sales, 0)            AS bruno_marc_sales
     , COALESCE(moi.nortiv8_sales, 0)               AS nortiv8_sales
FROM _main_data md
         LEFT JOIN _last_purchase_date lpd
                   ON lpd.customer_id = md.customer_id
         LEFT JOIN _customer_shopping_seg css
                   ON css.customer_id = md.customer_id
         LEFT JOIN _mm_order_info moi
                   ON css.customer_id = moi.customer_id;
