CREATE OR REPLACE TEMPORARY TABLE _case_base AS
SELECT DISTINCT cu.customer_id,
                c.case_id,
                c.datetime_added,
                cs.label,
                cfd.case_disposition_type_id,
                cc.case_flag_type_id,
                cc.case_flag_id,
                cf.label AS cancel_reason
FROM lake_jfb_view.ultra_merchant.case c
     JOIN lake_jfb_view.ultra_merchant.case_customer cu
          ON cu.case_id = c.case_id
     JOIN edw_prod.data_model_jfb.dim_customer dc
          ON cu.customer_id = dc.customer_id
     JOIN gfb.vw_store st
          ON st.store_id = dc.store_id
     JOIN lake_jfb_view.ultra_merchant.case_source cs
          ON cs.case_source_id = c.case_source_id
     LEFT JOIN lake_jfb_view.ultra_merchant.case_classification cc
               ON cc.case_id = c.case_id
     LEFT JOIN lake_jfb_view.ultra_merchant.case_flag cf
               ON cf.case_flag_id = cc.case_flag_id
     LEFT JOIN lake_jfb_view.ultra_merchant.case_flag_disposition_case_flag_type cfdc
               ON cfdc.case_flag_type_id = cc.case_flag_type_id
     LEFT JOIN lake_jfb_view.ultra_merchant.case_flag_disposition cfd
               ON cfd.case_flag_disposition_id = cfdc.case_flag_disposition_id;

CREATE OR REPLACE TEMPORARY TABLE _gms_cases AS
SELECT DISTINCT customer_id,
                case_id,
                datetime_added,
                label
FROM _case_base;

CREATE OR REPLACE TEMPORARY TABLE _customer_service_contacts AS
SELECT DISTINCT customer_id,
                COUNT(DISTINCT case_id)                                           AS customer_service_contacts
FROM _case_base
GROUP BY customer_id;

CREATE OR REPLACE TEMPORARY TABLE _membership_baseline AS
SELECT DISTINCT dc.customer_id,
                ds.store_brand                                            AS store_brand_name,
                ds.store_name,
                ds.store_region                                           AS store_region_abbr,
                fme.event_start_local_datetime,
                fme.event_end_local_datetime,
                DATE_TRUNC('month', fme.event_start_local_datetime::DATE) AS vip_cohort,
                (CASE
                     WHEN dc.specialty_country_code = 'GB' THEN 'UK'
                     WHEN dc.specialty_country_code != 'Unknown' THEN dc.specialty_country_code
                     ELSE ds.store_country END)                           AS store_country_abbr,
                CASE
                    WHEN fme.is_current = 1 THEN 'Currently Active VIP'
                    WHEN fme.is_current = 0 THEN 'Currently Cancelled'
                    ELSE 'Unsure' END                                     AS current_vip_status
FROM edw_prod.data_model_jfb.fact_membership_event fme
     JOIN edw_prod.data_model_jfb.dim_customer dc
          ON fme.customer_id = dc.customer_id
     JOIN gfb.vw_store ds
          ON ds.store_id = dc.store_id
WHERE fme.membership_state = 'VIP';

CREATE OR REPLACE TEMPORARY TABLE _vip_orders AS
SELECT olp.customer_id,
       b.vip_cohort,
       COUNT(DISTINCT IFF(olp.order_type='vip repeat', olp.order_id,null)) repeat_orders,
       COUNT(DISTINCT olp.order_id) num_orders
FROM gfb.gfb_order_line_data_set_place_date olp
     JOIN _membership_baseline b
          ON b.customer_id = olp.customer_id
              AND TO_DATE(olp.order_date) >= TO_DATE(b.event_start_local_datetime)
              AND olp.order_date <= b.event_end_local_datetime
WHERE olp.order_classification = 'product order'
  AND olp.order_type IN ('vip repeat', 'vip activating')
GROUP BY olp.customer_id,
         b.vip_cohort;

CREATE OR REPLACE TEMPORARY TABLE _cancels_base AS
SELECT cl.customer_log_id,
       cl.customer_id,
       cl.datetime_added,
       cl.comment,
       (CASE
            WHEN cl.comment ILIKE ANY ('%Membership downgraded to Guest Checkout. Reason%',
                                       '%Membership cancellation number%',
                                       '%Fee membership scheduled for cancel immediately',
                                       '%Fee membership scheduled for cancel at the end of period',
                                       '%Membership has been set to Guest Checkout by%',
                                       '%Membership downgraded to PAYGO. Reason: IVR cancel') THEN 'gms'
            WHEN cl.comment ILIKE ANY ('membership has been set to guest checkout through passive cancellation.',
                                       'membership has been set to pay as you go by customer using online cancel.')
                THEN 'other'
           END) AS cancel_type,
       (CASE
            WHEN cl.comment ILIKE '%Membership downgraded to PAYGO. Reason: IVR cancel%'
                THEN CONCAT('IVR ', MIN(x.label))
            WHEN cl.comment IS NOT NULL AND cancel_type = 'gms'
                THEN COALESCE(MIN(x.label), 'GMS Other')
            WHEN cancel_type = 'gms' THEN 'GMS Other'
           END) AS gms_cancel_channel,
       (CASE
            WHEN cl.comment ILIKE
                 'membership has been set to guest checkout through passive cancellation.' THEN 'Passive'
            WHEN cl.comment ILIKE
                 'membership has been set to pay as you go by customer using online cancel.' THEN 'Online'
           END) AS other_cancel_channel
FROM lake_jfb_view.ultra_merchant.customer_log cl
     LEFT JOIN _gms_cases x
               ON x.customer_id = cl.customer_id
                   AND DATE_TRUNC('day', x.datetime_added) = DATE_TRUNC('day', cl.datetime_added)
WHERE cl.comment ILIKE ANY ('%Membership downgraded to Guest Checkout. Reason%',
                            '%Membership cancellation number%',
                            '%Fee membership scheduled for cancel immediately',
                            '%Fee membership scheduled for cancel at the end of period',
                            '%Membership has been set to Guest Checkout by%',
                            '%Membership downgraded to PAYGO. Reason: IVR cancel',
                            'membership has been set to guest checkout through passive cancellation.',
                            'membership has been set to pay as you go by customer using online cancel.')
GROUP BY cl.customer_log_id,
         cl.customer_id,
         cl.datetime_added,
         cl.comment;

CREATE OR REPLACE TEMPORARY TABLE _blacklist AS
SELECT DISTINCT c.customer_id,
                c.datetime_added,
                c.datetime_modified
FROM lake_jfb_view.ultra_merchant.fraud_customer c
WHERE c.statuscode = 5233;

CREATE OR REPLACE TEMPORARY TABLE _gms_cancel_reasons AS
SELECT DISTINCT customer_id,
                datetime_added::DATE AS cancel_date,
                case_flag_id,
                cancel_reason
FROM _case_base
WHERE case_flag_type_id IN
      (7, 54, 78, 102, 126, 150, 174, 198, 222, 246, 270, 294, 318, 342, 365, 388, 411, 434, 457, 480, 503, 526, 553,
       580, 607, 634, 661, 688);

CREATE OR REPLACE TEMPORARY TABLE _online_cancel_reasons AS
SELECT DISTINCT m.customer_id,
                TO_DATE(md.datetime_added) AS cancel_date,
                mdr.label                  AS cancel_reason
FROM lake_jfb_view.ultra_merchant.membership_downgrade md
     JOIN lake_jfb_view.ultra_merchant.membership_downgrade_reason mdr
          ON md.membership_downgrade_reason_id = mdr.membership_downgrade_reason_id
     JOIN lake_jfb_view.ultra_merchant.membership m
          ON md.membership_id = m.membership_id
WHERE mdr.access = 'online_cancel';

CREATE OR REPLACE TEMPORARY TABLE _inactive_customer_list AS
SELECT clvm.meta_original_customer_id AS customer_id,
       clvm.vip_cohort_month_date     AS recent_activation_cohort
FROM edw_prod.analytics_base.customer_lifetime_value_monthly_cust clvm
     JOIN gfb.vw_store ds
          ON ds.store_id = clvm.store_id
WHERE clvm.customer_action_category LIKE '%Failed Billing%'
  AND clvm.is_bop_vip = 1
  AND (CASE
           WHEN DATE_PART('day', TO_DATE(CURRENT_DATE())) >= 1 AND DATE_PART('day', TO_DATE(CURRENT_DATE())) <= 10 THEN
               (clvm.month_date = DATEADD('month', -1, DATE_TRUNC('month', CURRENT_DATE())) OR
                clvm.month_date = DATEADD('month', -2, DATE_TRUNC('month', CURRENT_DATE())))
           WHEN DATE_PART('day', TO_DATE(CURRENT_DATE())) > 10 THEN
               (clvm.month_date = DATEADD('month', -1, DATE_TRUNC('month', CURRENT_DATE())) OR
                clvm.month_date = DATE_TRUNC('month', CURRENT_DATE()))
    END)
GROUP BY clvm.meta_original_customer_id,
         clvm.vip_cohort_month_date
HAVING SUM(CASE WHEN clvm.is_failed_billing = TRUE THEN 1 ELSE 0 END) = 2
   AND MAX(CASE WHEN is_login = TRUE THEN month_date END) <= DATEADD('month', -2, TO_DATE(CURRENT_DATE()));

CREATE OR REPLACE TEMPORARY TABLE _last_12_month_purchases AS
SELECT olp.customer_id,
       b.vip_cohort,
       COUNT(DISTINCT olp.order_id)                          AS Last_12_month_purchases
FROM gfb.gfb_order_line_data_set_place_date olp
     JOIN _membership_baseline b
          ON b.customer_id = olp.customer_id
              AND TO_DATE(olp.order_date) >= DATEADD(year, -1, current_date())
              AND olp.order_date <= b.event_end_local_datetime
WHERE olp.order_classification = 'product order'
  AND olp.order_type IN ('vip repeat', 'vip activating')
GROUP BY olp.customer_id,
         b.vip_cohort;

CREATE OR REPLACE TEMPORARY TABLE _customer_billing AS
SELECT clvm.meta_original_customer_id AS customer_id,
       clvm.vip_cohort_month_date     AS recent_activation_cohort,
       SUM(CASE WHEN clvm.is_successful_billing = 'TRUE' THEN 1 ELSE 0 END) AS total_credits_billed
FROM edw_prod.analytics_base.customer_lifetime_value_monthly_cust clvm
     JOIN gfb.vw_store ds
          ON ds.store_id = clvm.store_id
WHERE clvm.customer_action_category LIKE '%Successful Billing%'
  AND clvm.is_bop_vip = 1
  AND clvm.month_date >= DATEADD(year, -1, current_date())
GROUP BY clvm.meta_original_customer_id,
         clvm.vip_cohort_month_date;

CREATE OR REPLACE TEMPORARY TABLE _all_time_customer_billing AS
SELECT clvm.meta_original_customer_id AS customer_id,
       clvm.vip_cohort_month_date     AS recent_activation_cohort,
       SUM(CASE WHEN clvm.is_successful_billing = 'TRUE' THEN 1 ELSE 0 END) AS all_time_credits_billed,
       SUM(CASE WHEN is_skip = 'TRUE' THEN 1 ELSE 0 END)                    AS skip_count
FROM edw_prod.analytics_base.customer_lifetime_value_monthly_cust clvm
     JOIN gfb.vw_store ds
          ON ds.store_id = clvm.store_id
WHERE clvm.customer_action_category LIKE '%Successful Billing%'
  AND clvm.is_bop_vip = 1
GROUP BY clvm.meta_original_customer_id,
         clvm.vip_cohort_month_date;
         
CREATE OR REPLACE TEMPORARY TABLE _redeemed_tokens AS
SELECT mca.customer_id,
    COUNT(mca.credit_id) AS redeemed_token_count
FROM gfb.gfb_membership_credit_activity mca
    JOIN _membership_baseline b
          ON b.customer_id = mca.customer_id
              AND TO_DATE(mca.redeemed_order_date) >= DATEADD(year, -1, current_date())
              AND TO_DATE(mca.redeemed_order_date) <= b.event_end_local_datetime
WHERE  mca.credit_type = 'Token'
    AND mca.credit_reason = 'Token Billing'
GROUP BY mca.customer_id;

CREATE OR REPLACE TEMPORARY TABLE _recent_order AS
SELECT customer_id
     , MAX(order_date) AS recent_order_date
FROM gfb.gfb_order_line_data_set_place_date
WHERE order_type IN ('vip repeat', 'vip activating')
  AND order_classification = 'product order'
GROUP BY customer_id;

CREATE OR REPLACE TEMPORARY TABLE _most_recent_activating_order AS
SELECT customer_id
     , MAX(order_id) AS max_activating_order
FROM gfb.gfb_order_line_data_set_place_date
WHERE order_type = 'vip activating'
  AND order_classification = 'product order'
GROUP BY customer_id;

CREATE OR REPLACE TEMPORARY TABLE _activating_sub_brand AS
SELECT DISTINCT ol.customer_id
              , CASE WHEN sub_brand = 'JFB' THEN mdp.business_unit ELSE sub_brand END AS sub_brand
FROM gfb.gfb_order_line_data_set_place_date ol
         JOIN gfb.merch_dim_product mdp
              ON ol.business_unit = mdp.business_unit
                  AND ol.region = mdp.region
                  AND ol.country = mdp.country
                  AND ol.product_sku = mdp.product_sku
            JOIN _most_recent_activating_order mrao ON ol.customer_id = mrao.customer_id
    AND ol.order_id = mrao.max_activating_order
WHERE promo_code_1 IN ('LEADDAY1ALLBOOTS10NEW',
                       'FIRSTPAIR10DAY8',
                       'SHOES10MEN',
                       '75OFF',
                       'D37_SHOES10',
                       'FIRSTPAIR10DAY8MEN',
                       'D37_SHOES10_MEN',
                       'SHOES10GIRLS',
                       'FIRSTPAIR10DAY8GIRLS',
                       'SHOES10BOYS',
                       'FIRSTPAIR10DAY8BOYS',
                       'D37_SHOES10_BOYS')
  AND department_detail IN ('FOOTWEAR', 'GIRLS SHOES', 'BOYS SHOES', 'MENS - SHOES', 'KIDS - BOYS', 'KIDS - GIRLS');

CREATE OR REPLACE TEMPORARY TABLE _outstanding_credit AS
SELECT mca.customer_id,
    COUNT(DISTINCT mca.credit_id) AS outstanding_credit_count
FROM _membership_baseline b
JOIN gfb.gfb_membership_credit_activity mca
     ON mca.customer_id = b.customer_id
LEFT JOIN _cancels_base c
               ON mca.customer_id = c.customer_id
                   AND TO_DATE(mca.issued_date) = TO_DATE(c.datetime_added)
                   AND c.cancel_type = 'gms'
     LEFT JOIN _cancels_base o
               ON o.customer_id = b.customer_id
                   AND TO_DATE(mca.issued_date) <= TO_DATE(o.datetime_added)
                   AND c.cancel_type = 'other'
     LEFT JOIN _blacklist bl
               ON bl.customer_id = b.customer_id
                   AND TO_DATE(mca.issued_date) = TO_DATE(bl.datetime_added)
     LEFT JOIN _blacklist bl2
               ON bl2.customer_id = b.customer_id
                   AND TO_DATE(mca.issued_date) = TO_DATE(bl2.datetime_modified)
WHERE credit_type IN ('Fixed Credit','Token')
    AND credit_reason IN  ('Membership Credit','Token Billing','Converted Membership Credit')
    AND mca.redeemed_order_date IS NULL
    AND mca.cancelled_date IS NULL
    AND mca.other_activity_date IS NULL
    AND b.current_vip_status = 'Currently Cancelled'
GROUP BY mca.customer_id;
    
CREATE OR REPLACE TEMPORARY TABLE _channels_combined AS
SELECT b.customer_id,
       b.store_brand_name,
       b.store_name,
       b.store_region_abbr,
       b.store_country_abbr,
	   ao.sub_brand,
       b.event_start_local_datetime,
       b.event_end_local_datetime,
       ro.recent_order_date,
       dv.first_activating_cohort                                AS first_cohort,
       b.vip_cohort,
       (CASE
            WHEN ic.customer_id IS NOT NULL THEN 'Currently Inactive VIP'
            ELSE b.current_vip_status END)                       AS current_vip_status,
       CASE
           WHEN dv.first_activating_cohort = b.vip_cohort THEN 'First Time Activation'
           ELSE 'Reactivation'
           END                                                   AS activation_type,
       vo.num_orders,
       vo.repeat_orders,
       cb.total_credits_billed,
       acb.all_time_credits_billed,
       acb.skip_count,
       cs.customer_service_contacts,
       oc.outstanding_credit_count,
       c.customer_log_id,
       TO_DATE(c.datetime_added)                                 AS gms_cancel_date,
       c.comment,
       c.gms_cancel_channel,
       (CASE
            WHEN c.gms_cancel_channel IS NOT NULL
                THEN COALESCE(gcr.cancel_reason, 'Unknown') END) AS gms_cancel_reason,
       TO_DATE(o.datetime_added)                                 AS other_cancel_date,
       o.other_cancel_channel,
       (CASE
            WHEN o.other_cancel_channel = 'Online' THEN COALESCE(ocr.cancel_reason, 'Unknown')
            WHEN o.other_cancel_channel = 'Passive' THEN 'Passive - unspecific reason'
           END)                                                  AS other_cancel_reason,
       TO_DATE(bl.datetime_added)                                AS blacklist_cancel_date,
       (CASE
            WHEN bl.customer_id IS NOT NULL
                THEN 'Blacklisted' END)                          AS blacklisted,
       TO_DATE(bl2.datetime_modified)                            AS blacklist_cancel_date2,
       (CASE
            WHEN bl2.customer_id IS NOT NULL
                THEN 'Blacklisted' END)                          AS blacklisted2,
       (CASE
            WHEN bl.customer_id IS NOT NULL OR bl2.customer_id IS NOT NULL
                THEN 'Blacklisted - unspecific reason' END)      AS blacklist_cancel_reason,
       COALESCE((CASE
                     WHEN os.opt_in = 0 THEN 'opt-out'
                     ELSE 'optin' END), 'no data')               AS optin_status,
       (CASE
            WHEN COALESCE(po.Last_12_month_purchases,0) = 0 OR COALESCE(cb.total_credits_billed,0) < DATEDIFF('month',DATEADD(year, -1, current_date()),TO_DATE(b.event_end_local_datetime)) 
                    OR DATEDIFF('month',DATEADD(year, -1, current_date()),TO_DATE(b.event_end_local_datetime))=1 THEN 'Good Attrition'
            WHEN (COALESCE(po.Last_12_month_purchases,0) > 0 AND COALESCE(rt.redeemed_token_count,0) = 0) 
                    OR (COALESCE(po.Last_12_month_purchases,0) = 0 AND COALESCE(cb.total_credits_billed,0) >= DATEDIFF('month',DATEADD(year, -1, current_date()),TO_DATE(b.event_end_local_datetime))
                    AND DATEDIFF('month',DATEADD(year, -1, current_date()),TO_DATE(b.event_end_local_datetime))<>1) THEN 'Bad Attrition'
            WHEN (COALESCE(po.Last_12_month_purchases,0) > 0 AND COALESCE(rt.redeemed_token_count,0) > 0) 
                    OR (COALESCE(rt.redeemed_token_count,0) > 0 AND COALESCE(cb.total_credits_billed,0) >= DATEDIFF('month',DATEADD(year, -1, current_date()),TO_DATE(b.event_end_local_datetime))
                    AND DATEDIFF('month',DATEADD(year, -1, current_date()),TO_DATE(b.event_end_local_datetime))<>1) THEN 'Grey Attrition'
           END)                                                  AS Attrition_type,
       dv.membership_price
FROM _membership_baseline b
     JOIN gfb.gfb_dim_vip dv
          ON dv.customer_id = b.customer_id
     LEFT JOIN _cancels_base c
               ON c.customer_id = b.customer_id
                   AND TO_DATE(b.event_end_local_datetime) = TO_DATE(c.datetime_added)
                   AND b.current_vip_status = 'Currently Cancelled'
                   AND c.cancel_type = 'gms'
     LEFT JOIN _cancels_base o
               ON o.customer_id = b.customer_id
                   AND TO_DATE(b.event_end_local_datetime) <= TO_DATE(o.datetime_added)
                   AND b.current_vip_status = 'Currently Cancelled'
                   AND c.cancel_type = 'other'
     LEFT JOIN _blacklist bl
               ON bl.customer_id = b.customer_id
                   AND TO_DATE(b.event_end_local_datetime) = TO_DATE(bl.datetime_added)
                   AND b.current_vip_status = 'Currently Cancelled'
     LEFT JOIN _blacklist bl2
               ON bl2.customer_id = b.customer_id
                   AND TO_DATE(b.event_end_local_datetime) = TO_DATE(bl2.datetime_modified)
                   AND b.current_vip_status = 'Currently Cancelled'
     LEFT JOIN _online_cancel_reasons ocr
               ON ocr.customer_id = o.customer_id
                   AND ocr.cancel_date = TO_DATE(o.datetime_added)
                   AND o.other_cancel_channel = 'Online'
                   AND b.current_vip_status = 'Currently Cancelled'
     LEFT JOIN _gms_cancel_reasons gcr
               ON gcr.customer_id = c.customer_id
                   AND gcr.cancel_date = TO_DATE(c.datetime_added)
                   AND b.current_vip_status = 'Currently Cancelled'
     LEFT JOIN _vip_orders vo
               ON vo.customer_id = b.customer_id
                   AND vo.vip_cohort = b.vip_cohort
     LEFT JOIN _inactive_customer_list ic
               ON ic.customer_id = b.customer_id
                   AND ic.recent_activation_cohort = b.vip_cohort
     LEFT JOIN gfb.view_customer_opt_info os
               ON os.customer_id = b.customer_id
     LEFT JOIN _activating_sub_brand ao
               ON ao.customer_id = b.customer_id
     LEFT JOIN _recent_order ro
               ON ro.customer_id = b.customer_id
     LEFT JOIN _customer_service_contacts cs
               ON cs.customer_id = b.customer_id
     LEFT JOIN _outstanding_credit oc 
                ON oc.customer_id=b.customer_id
     LEFT JOIN _Last_12_month_purchases po 
                ON po.customer_id=b.customer_id
                    AND po.vip_cohort = b.vip_cohort
     LEFT JOIN _customer_billing cb 
                ON cb.customer_id=b.customer_id
                    AND cb.recent_activation_cohort = b.vip_cohort
     LEFT JOIN _all_time_customer_billing acb
                ON acb.customer_id=b.customer_id
                    AND acb.recent_activation_cohort = b.vip_cohort
     LEFT JOIN _redeemed_tokens rt
                ON rt.customer_id=b.customer_id;

CREATE OR REPLACE TRANSIENT TABLE gfb.gfb014_attrition_data_set AS
SELECT store_brand_name,
       store_name,
       store_region_abbr,
       store_country_abbr,
	   COALESCE(sub_brand,UPPER(store_brand_name))                                                          AS sub_brand,
       first_cohort,
       vip_cohort,
       activation_type,
       current_vip_status,
       num_orders,
       repeat_orders,
       total_credits_billed,
       all_time_credits_billed,
       skip_count,
       customer_service_contacts,
       outstanding_credit_count,
       Attrition_type,
       COALESCE(gms_cancel_date, other_cancel_date, blacklist_cancel_date, blacklist_cancel_date2,
                event_end_local_datetime)                                                                   AS cancel_date,
       (CASE
            WHEN current_vip_status = 'Currently Cancelled' THEN COALESCE(gms_cancel_channel, other_cancel_channel,
                                                                          blacklisted, blacklisted2, 'Unknown')
            WHEN current_vip_status = 'Currently Active VIP' OR current_vip_status = 'Currently Inactive VIP'
                THEN 'Active VIP'
           END)                                                                                             AS cancel_channel,
       (CASE
            WHEN current_vip_status = 'Currently Cancelled' THEN COALESCE(gms_cancel_reason, other_cancel_reason,
                                                                          blacklist_cancel_reason, 'Unknown')
            WHEN current_vip_status = 'Currently Active VIP' THEN 'Currently Active VIP'
            WHEN current_vip_status = 'Currently Inactive VIP' THEN 'Currently Inactive VIP'
           END)                                                                                             AS cancel_reason,
       DATEDIFF('day', DATE_TRUNC('day', event_start_local_datetime),
                DATE_TRUNC('day', cancel_date))                                                             AS day_difference,
       DATEDIFF('day', DATE_TRUNC('day', recent_order_date),
               DATE_TRUNC('day', cancel_date))                                                             AS days_since_final_purchase,         
       DATEDIFF('month', DATE_TRUNC('day', event_start_local_datetime), DATE_TRUNC('day', cancel_date)) + 1 AS tenure,
       optin_status,
       membership_price,
       COUNT(DISTINCT customer_id)                                                                          AS num_customers
FROM _channels_combined
GROUP BY store_brand_name,
         store_name,
         store_region_abbr,
         store_country_abbr,
		 COALESCE(sub_brand,UPPER(store_brand_name)),
         first_cohort,
         vip_cohort,
         activation_type,
         current_vip_status,
         num_orders,
         repeat_orders,
         total_credits_billed,
         all_time_credits_billed,
         skip_count,
         customer_service_contacts,
         outstanding_credit_count,
         Attrition_type,
         COALESCE(gms_cancel_date, other_cancel_date, blacklist_cancel_date, blacklist_cancel_date2,
                  event_end_local_datetime),
         (CASE
              WHEN current_vip_status = 'Currently Cancelled' THEN COALESCE(gms_cancel_channel, other_cancel_channel,
                                                                            blacklisted, blacklisted2, 'Unknown')
              WHEN current_vip_status = 'Currently Active VIP' OR current_vip_status = 'Currently Inactive VIP'
                  THEN 'Active VIP'
             END),
         (CASE
              WHEN current_vip_status = 'Currently Cancelled' THEN COALESCE(gms_cancel_reason, other_cancel_reason,
                                                                            blacklist_cancel_reason, 'Unknown')
              WHEN current_vip_status = 'Currently Active VIP' THEN 'Currently Active VIP'
              WHEN current_vip_status = 'Currently Inactive VIP' THEN 'Currently Inactive VIP'
             END),
         DATEDIFF('day', DATE_TRUNC('day', event_start_local_datetime), DATE_TRUNC('day', cancel_date)),
         DATEDIFF('day', DATE_TRUNC('day', recent_order_date),DATE_TRUNC('day', cancel_date)),
         DATEDIFF('month', DATE_TRUNC('day', event_start_local_datetime), DATE_TRUNC('day', cancel_date)) + 1,
         optin_status,
         membership_price;
