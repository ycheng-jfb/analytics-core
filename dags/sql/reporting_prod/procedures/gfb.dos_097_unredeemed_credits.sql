CREATE OR REPLACE TEMPORARY TABLE _active_store_credit AS
SELECT UPPER(st.store_brand || ' ' || st.store_country)                                   AS store_reporting_name,
       UPPER(st.store_brand_abbr || st.store_region)                                      AS store_region,
       sc.customer_id,
       (CASE
            WHEN clv.current_membership_status = 'VIP' THEN 'Active VIP'
            ELSE 'Other' END)                                                             AS vip_status,
       'Membership Credit'                                                                AS credit_type,
       m.price                                                                            AS membership_price,
       DATEDIFF(MONTH, clv.recent_activating_cohort, DATE_TRUNC(MONTH, CURRENT_DATE)) + 1 AS tenure,
       COUNT(sc.store_credit_id)                                                          AS active_credit_count,
       SUM(sc.balance * dc.credit_issued_usd_conversion_rate)                             AS active_amount
FROM lake_jfb_view.ultra_merchant.store_credit sc
     JOIN edw_prod.data_model_jfb.dim_credit dc
          ON dc.credit_id = sc.store_credit_id
              AND dc.source_credit_id_type = 'store_credit_id'
     JOIN gfb.gfb_dim_vip clv
          ON clv.customer_id = sc.customer_id
     JOIN gfb.vw_store st
          ON st.store_id = clv.store_id
     LEFT JOIN lake_jfb_view.ultra_merchant.membership m
               ON m.customer_id = sc.customer_id
WHERE sc.statuscode = 3240
  AND dc.credit_reason = 'Membership Credit'
  AND dc.credit_type = 'Fixed Credit'
GROUP BY UPPER(st.store_brand || ' ' || st.store_country),
         UPPER(st.store_brand_abbr || st.store_region),
         sc.customer_id,
         (CASE
              WHEN clv.current_membership_status = 'VIP' THEN 'Active VIP'
              ELSE 'Other' END),
         m.price,
         DATEDIFF(MONTH, clv.recent_activating_cohort, DATE_TRUNC(MONTH, CURRENT_DATE)) + 1;

CREATE OR REPLACE TEMPORARY TABLE _customer_order_info AS
SELECT olp.business_unit || ' ' || olp.country                              AS store_reporting_name,
       (CASE
            WHEN olp.business_unit = 'JUSTFAB' THEN 'JF'
            WHEN olp.business_unit = 'SHOEDAZZLE' THEN 'SD'
            WHEN olp.business_unit = 'FABKIDS' THEN 'FK' END) || olp.region AS store_region,
       olp.customer_id,
       (CASE
            WHEN clv.current_membership_status = 'VIP' THEN 'Active VIP'
            ELSE 'Other' END)                                               AS vip_status,
       SUM(olp.total_discount) * 1.0 / SUM(olp.order_line_subtotal)         AS customer_discount_rate,
       DATEDIFF(DAY, MIN(olp.order_date), CURRENT_DATE) * 1.0 /
       COUNT(DISTINCT olp.order_id)                                         AS customer_avg_days_between_purchase,
       COUNT(DISTINCT CASE
                          WHEN olp.order_type = 'vip repeat'
                              THEN olp.order_id END)                        AS repeat_order_count,
       COUNT(DISTINCT CASE
                          WHEN olp.order_type = 'vip repeat' AND DAY(olp.order_date) <= 5
                              THEN olp.order_id END)                        AS first_5_day_repeat_order_count
FROM gfb.gfb_order_line_data_set_place_date olp
     JOIN gfb.gfb_dim_vip clv
          ON clv.customer_id = olp.customer_id
WHERE olp.order_classification = 'product order'
  AND olp.order_date >= DATEADD(YEAR, -2, CURRENT_DATE)
  AND olp.order_line_subtotal > 0
GROUP BY olp.business_unit || ' ' || olp.country,
         (CASE
              WHEN olp.business_unit = 'JUSTFAB' THEN 'JF'
              WHEN olp.business_unit = 'SHOEDAZZLE' THEN 'SD'
              WHEN olp.business_unit = 'FABKIDS' THEN 'FK' END) || olp.region,
         olp.customer_id,
         (CASE
              WHEN clv.current_membership_status = 'VIP' THEN 'Active VIP'
              ELSE 'Other' END);

CREATE OR REPLACE TEMPORARY TABLE _vip_activities AS
SELECT clvm.meta_original_customer_id AS customer_id,
       SUM(CASE
               WHEN clvm.is_successful_billing = 1
                   THEN 1
               ELSE 0 END)            AS successful_billing_count,
       SUM(CASE
               WHEN clvm.is_skip = 1
                   THEN 1
               ELSE 0 END)            AS skips,
       SUM(clvm.cash_gross_profit)    AS cash_gross_margin,
       SUM(clvm.product_gross_profit) AS gaap_gross_margin
FROM edw_prod.analytics_base.customer_lifetime_value_monthly clvm
     JOIN gfb.vw_store st
          ON st.store_id = clvm.store_id
WHERE clvm.month_date >= DATEADD(YEAR, -2, DATE_TRUNC(MONTH, CURRENT_DATE))
GROUP BY clvm.meta_original_customer_id;

CREATE OR REPLACE TRANSIENT TABLE gfb.dos_097_unredeemed_credits AS
SELECT ac.store_reporting_name,
       ac.store_region,
       ac.vip_status,
       ac.active_credit_count,
       ac.membership_price,
       COUNT(DISTINCT ac.customer_id)              AS customer_count,
       SUM(CASE
               WHEN ac.credit_type = 'Membership Credit' THEN ac.active_amount
               ELSE 0 END)                         AS unredeemed_membership_credit_amount,
       AVG(coi.customer_discount_rate)             AS avg_discount_rate,
       AVG(coi.customer_avg_days_between_purchase) AS avg_days_between_orders,
       AVG(coi.repeat_order_count)                 AS avg_repeat_orders,
       AVG(va.successful_billing_count)            AS avg_credit_billings,
       AVG(va.skips)                               AS avg_skips,
       AVG(coi.first_5_day_repeat_order_count)     AS avg_first_5_day_repeat_orders,
       AVG(ac.tenure)                              AS avg_tenure,
       AVG(va.cash_gross_margin)                   AS avg_cash_gross_margin,
       AVG(va.gaap_gross_margin)                   AS avg_gaap_gross_margin
FROM _active_store_credit ac
     LEFT JOIN _customer_order_info coi
               ON coi.customer_id = ac.customer_id
     LEFT JOIN _vip_activities va
               ON va.customer_id = ac.customer_id
GROUP BY ac.store_reporting_name,
         ac.store_region,
         ac.vip_status,
         ac.active_credit_count,
         ac.membership_price;
