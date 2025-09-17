SET start_date = DATEADD('year', -1, DATE_TRUNC('year', CURRENT_DATE()));

CREATE OR REPLACE TEMP TABLE _period_month AS
SELECT DISTINCT dd.month_date
FROM edw_prod.data_model_jfb.dim_date dd
WHERE month_date >= $start_date
ORDER BY month_date;

CREATE OR REPLACE TEMPORARY TABLE _customer_gender AS
SELECT dc.customer_id,
       CASE WHEN value IN ('M', 'm') THEN 'Male'
            WHEN value IN ('F', 'f') THEN 'Female'
            WHEN value IN ('K') THEN 'Kids'
       END AS gender
FROM lake_jfb_view.ultra_merchant.customer_detail cd
     JOIN edw_prod.data_model_jfb.dim_customer dc
          ON cd.customer_id=dc.customer_id
WHERE name='gender';

CREATE OR REPLACE TEMP TABLE _vip_customer_base AS
SELECT DISTINCT a.customer_id,
                cg.gender,
                b.month_date
FROM edw_prod.data_model_jfb.fact_activation a
         LEFT JOIN _customer_gender cg
              ON cg.customer_id=a.customer_id
         JOIN _period_month b
              ON b.month_date BETWEEN DATE_TRUNC(MONTH, a.activation_local_datetime) AND a.cancellation_local_datetime
         JOIN gfb.vw_store ds
              ON ds.store_id = a.store_id;

CREATE OR REPLACE TEMPORARY TABLE _activations_and_cancels AS
SELECT cb.month_date,
       cb.customer_id,
       CASE WHEN cb.gender='Female' AND ds.store_brand='FabKids' THEN 'Girls'
            WHEN cb.gender='Male' AND ds.store_brand='FabKids' THEN 'Boys'
            WHEN cb.gender IS NOT NULL THEN cb.gender
            WHEN cb.gender IS NULL AND ds.store_brand='FabKids' THEN 'Kids'
            WHEN cb.gender IS NULL AND ds.store_brand in ('JustFab','ShoeDazzle') THEN 'Female'
       END AS gender,
       fa.session_id,
       dsc.store_id,
       ds.store_brand                                            AS activating_store_id,
       dsc.store_region                                          AS region,
       CASE
           WHEN dc.specialty_country_code = 'GB' THEN 'UK'
           WHEN dc.specialty_country_code != 'Unknown' THEN dc.specialty_country_code
           ELSE dsc.store_country END                            AS country,
       dsc.store_type,
       ds.store_type                                             AS activating_store_type,
       dc.birth_year                                             AS customer_birth_year,
       CASE
           WHEN dc.birth_year BETWEEN 1930 AND 2005 THEN 2021 - birth_year
           WHEN dc.birth_year = -1 THEN -1
           ELSE 999 END                                          AS age,
       UPPER(dc.default_state_province)                          AS customer_state_province,
       CASE
           WHEN dc.mobile_app_cohort_month_date IS NOT NULL
               THEN 1
           ELSE 0 END                                            AS mobile_app_downloaded_flag,
       dc.how_did_you_hear,
       fa.activation_local_datetime                              AS activation_local_datetime,
       CAST(fa.activation_local_datetime AS DATE)                AS activation_local_date,
       DATEDIFF('days', fr.registration_local_datetime,
                fa.activation_local_datetime)                    AS lead_age_at_activation_in_days,
       NVL(fa.activation_sequence_number, 1) - 1                 AS reactivation_count,
       IFF(CAST(fa.cancellation_local_datetime AS DATE) = '9999-12-31', NULL,
           fa.cancellation_local_datetime)                       AS cancel_local_datetime,
       CAST(cancel_local_datetime AS DATE)                       AS cancel_local_date,
       fa.cancel_type,
       DATEDIFF(MONTH, activation_local_date, cancel_local_date) AS days_to_cancel,
       IFF(cancel_local_datetime IS NOT NULL, 1, 0)              AS churned_flag,
       fa.order_id
FROM _vip_customer_base cb
         JOIN edw_prod.data_model_jfb.fact_activation fa
              ON fa.customer_id = cb.customer_id
                  AND cb.month_date BETWEEN fa.vip_cohort_month_date AND fa.cancellation_local_datetime
         JOIN edw_prod.data_model_jfb.dim_customer dc
              ON dc.customer_id = fa.customer_id
         JOIN edw_prod.data_model_jfb.dim_store ds
              ON ds.store_id = fa.sub_store_id
         JOIN edw_prod.data_model_jfb.dim_store dsc
              ON dsc.store_id = dc.store_id
         JOIN edw_prod.data_model_jfb.fact_registration fr
              ON fr.customer_id = dc.customer_id
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cb.customer_id, cb.month_date ORDER BY fa.activation_local_datetime DESC)
           = 1;

CREATE OR REPLACE TEMPORARY TABLE _all_customers AS
SELECT UPPER(activating_store_id)                               AS store_brand,
       gender,
       DATEDIFF('month', activation_local_date, month_date) + 1 AS tenure,
       month_date,
       DATE_TRUNC('month', activation_local_date)               AS vip_cohort,
       CASE
           WHEN customer_state_province = 'CA' THEN 'CA'
           WHEN country != 'US' THEN 'Other Country'
           ELSE 'Other State' END                               AS state,
       region,
       COUNT(*)                                                 AS total_count
FROM _activations_and_cancels
GROUP BY UPPER(activating_store_id),
         gender,
         (DATEDIFF('month', activation_local_date, month_date) + 1),
         month_date,
         DATE_TRUNC('month', activation_local_date),
         region,
         CASE
             WHEN customer_state_province = 'CA' THEN 'CA'
             WHEN country != 'US' THEN 'Other Country'
             ELSE 'Other State'
             END;

CREATE OR REPLACE TEMPORARY TABLE _vip_cohorts AS
SELECT DISTINCT clvm.customer_id,
                clvm.first_activating_cohort AS vip_cohort
FROM gfb.gfb_dim_vip clvm
WHERE clvm.first_activating_cohort <> '1900-01-01';

CREATE OR REPLACE TEMPORARY TABLE _first_cohort AS
SELECT customer_id,
       MIN(vip_cohort) AS first_cohort
FROM _vip_cohorts
WHERE vip_cohort <> '1900-01-01'
GROUP BY customer_id;

CREATE OR REPLACE TEMPORARY TABLE _jfb_customers AS
SELECT dc.customer_id,
       CASE WHEN cg.gender='Female' AND ds.store_brand='FabKids' THEN 'Girls'
            WHEN cg.gender='Male' AND ds.store_brand='FabKids' THEN 'Boys'
            WHEN cg.gender IS NOT NULL THEN cg.gender
            WHEN cg.gender IS NULL AND ds.store_brand='FabKids' THEN 'Kids'
            WHEN cg.gender IS NULL AND ds.store_brand in ('JustFab','ShoeDazzle') THEN 'Female'
       END AS gender,
       dc.email,
       dc.default_state_province,
       dc.specialty_country_code,
       ds.store_brand,
       ds.store_name,
       ds.store_region,
       ds.store_country,
       ds.store_brand_abbr
FROM edw_prod.data_model_jfb.dim_customer dc
        LEFT JOIN _customer_gender cg
              ON dc.customer_id=cg.customer_id
         JOIN gfb.vw_store ds
              ON ds.store_id = dc.store_id
ORDER BY dc.customer_id;

CREATE OR REPLACE TEMPORARY TABLE _vip_dates AS
SELECT DISTINCT dc.customer_id,
                dc.gender,
                mee.event_start_local_datetime,
                mee.event_end_local_datetime,
                dc.store_brand                AS store_brand_name,
                dc.store_name,
                dc.store_region               AS store_region_abbr,
                CASE
                    WHEN dc.specialty_country_code = 'GB' THEN 'UK'
                    WHEN dc.specialty_country_code != 'Unknown' THEN dc.specialty_country_code
                    ELSE dc.store_country END AS store_country_abbr,
                mee.is_current,
                dc.email
FROM edw_prod.data_model_jfb.fact_membership_event mee
         JOIN _jfb_customers dc
              ON mee.customer_id = dc.customer_id
WHERE mee.membership_state = 'VIP';

CREATE OR REPLACE TEMPORARY TABLE _membership_baseline AS
SELECT v.customer_id,
       v.gender,
       v.email,
       v.store_brand_name,
       v.store_name,
       v.store_region_abbr,
       v.store_country_abbr,
       v.event_start_local_datetime,
       v.event_end_local_datetime,
       COALESCE(f.first_cohort, DATE_TRUNC('month', v.event_start_local_datetime)) AS first_cohort,
       COALESCE(c.vip_cohort, DATE_TRUNC('month', v.event_start_local_datetime))   AS vip_cohort,
       CASE
           WHEN v.is_current = 1 THEN 'Currently Active VIP'
           WHEN v.is_current = 0 THEN 'Currently Cancelled'
           ELSE 'Unsure'
           END                                                                     AS current_vip_status,
       CASE
           WHEN f.first_cohort = c.vip_cohort THEN 'First Time Activation'
           ELSE 'Reactivation'
           END                                                                     AS activation_type,
       dv.membership_price,
       dv.default_state_province,
       dv.country,
       dv.region
FROM _vip_dates v
         LEFT JOIN _first_cohort f
                   ON f.customer_id = v.customer_id
         LEFT JOIN _vip_cohorts c
                   ON v.customer_id = c.customer_id
                       AND DATE_TRUNC('month', v.event_start_local_datetime) = c.vip_cohort
         JOIN lake_jfb_view.ultra_merchant.membership m
              ON m.customer_id = v.customer_id
         JOIN gfb.gfb_dim_vip dv
              ON dv.customer_id = v.customer_id;

CREATE OR REPLACE TEMPORARY TABLE _vip_orders AS
SELECT olp.customer_id,
       b.gender,
       b.vip_cohort,
       COUNT(DISTINCT olp.order_id) num_orders
FROM gfb.gfb_order_line_data_set_place_date olp
         JOIN _membership_baseline b
              ON b.customer_id = olp.customer_id
                  AND TO_DATE(olp.order_date) >= TO_DATE(b.event_start_local_datetime)
                  AND olp.order_date <= b.event_end_local_datetime
WHERE olp.order_classification = 'product order'
  AND olp.order_type IN ('vip repeat', 'vip activating')
GROUP BY olp.customer_id,
         b.gender,
         b.vip_cohort;

CREATE OR REPLACE TEMPORARY TABLE _gms_cases AS
SELECT DISTINCT cc.customer_id,
                c.case_id,
                c.datetime_added,
                cs.label
FROM lake_jfb_view.ultra_merchant.case c
         JOIN lake_jfb_view.ultra_merchant.case_customer cc
              ON cc.case_id = c.case_id
         JOIN lake_jfb_view.ultra_merchant.case_source cs
              ON cs.case_source_id = c.case_source_id;

CREATE OR REPLACE TEMPORARY TABLE _gms_cancels AS
SELECT cl.customer_log_id,
       cl.customer_id,
       cl.datetime_added,
       cl.comment,
       CASE
           WHEN cl.comment ILIKE '%Membership downgraded to PAYGO. Reason: IVR cancel%'
               THEN CONCAT('IVR ', MIN(x.label))
           WHEN cl.comment IS NOT NULL THEN COALESCE(MIN(x.label), 'GMS Other')
           ELSE 'GMS Other'
           END AS gms_cancel_channel
FROM lake_jfb_view.ultra_merchant.customer_log cl
         LEFT JOIN _gms_cases x
                   ON x.customer_id = cl.customer_id
                       AND DATE_TRUNC('day', x.datetime_added) = DATE_TRUNC('day', cl.datetime_added)
WHERE (cl.comment ILIKE '%Membership downgraded to Guest Checkout. Reason%'
    OR cl.comment ILIKE '%Membership cancellation number%'
    OR cl.comment ILIKE '%Fee membership scheduled for cancel immediately'
    OR cl.comment ILIKE '%Fee membership scheduled for cancel at the end of period'
    OR cl.comment ILIKE '%Membership has been set to Guest Checkout by%'
    OR cl.comment ILIKE '%Membership downgraded to PAYGO. Reason: IVR cancel') --no comments for SXF
      -- or cl.comment like 'Membership trial period scheduled for cancel immediately'
      -- or cl.comment like 'Membership trial period scheduled for cancel at the end of trial period'
GROUP BY cl.customer_log_id,
         cl.customer_id,
         cl.datetime_added,
         cl.comment;

CREATE OR REPLACE TEMPORARY TABLE _online_passive_cancels AS
SELECT DISTINCT cl.customer_id,
                cl.datetime_added,
                CASE
                    WHEN LOWER(cl.comment) LIKE
                         'membership has been set to guest checkout through passive cancellation.' THEN 'Passive'
                    WHEN LOWER(cl.comment) LIKE
                         'membership has been set to pay as you go by customer using online cancel.' THEN 'Online'
                    END other_cancel_channel
FROM lake_jfb_view.ultra_merchant.customer_log cl
WHERE (LOWER(cl.comment) LIKE 'membership has been set to guest checkout through passive cancellation.'
    OR LOWER(cl.comment) LIKE 'membership has been set to pay as you go by customer using online cancel.');

CREATE OR REPLACE TEMPORARY TABLE _blacklist AS
SELECT DISTINCT c.customer_id,
                c.datetime_added,
                c.datetime_modified
FROM lake_jfb_view.ultra_merchant.fraud_customer c;

CREATE OR REPLACE TEMPORARY TABLE _retention_cases AS
SELECT DISTINCT c.case_id,
                cu.customer_id,
                c.datetime_added
FROM lake_jfb_view.ultra_merchant.case c
         JOIN lake_jfb_view.ultra_merchant.case_customer cu
              ON cu.case_id = c.case_id
         JOIN lake_jfb_view.ultra_merchant.case_classification cc
              ON cc.case_id = c.case_id
         JOIN lake_jfb_view.ultra_merchant.case_flag_disposition_case_flag_type cfdc
              ON cfdc.case_flag_type_id = cc.case_flag_type_id
         JOIN lake_jfb_view.ultra_merchant.case_flag_disposition cfd
              ON cfd.case_flag_disposition_id = cfdc.case_flag_disposition_id
WHERE cfd.case_disposition_type_id = 9; --RETENTION SURVEY

CREATE OR REPLACE TEMPORARY TABLE _gms_cancel_reasons AS
SELECT DISTINCT b.customer_id,
                TO_DATE(b.datetime_added) cancel_date,
                cc.case_flag_id,
                cf.label                  cancel_reason
FROM _retention_cases b
         JOIN lake_jfb_view.ultra_merchant.case_classification cc
              ON cc.case_id = b.case_id
         JOIN lake_jfb_view.ultra_merchant.case_flag cf
              ON cf.case_flag_id = cc.case_flag_id
WHERE cc.case_flag_type_id IN (7, 54, 78, 102, 126, 150, 174, 198, 222, 246, 270, 294, 318, 342, 365, 388, 411,
                               434, 457, 480, 503, 526, 553, 580, 607, 634, 661, 688);

CREATE OR REPLACE TEMPORARY TABLE _online_cancel_reasons AS
SELECT DISTINCT m.customer_id,
                TO_DATE(md.datetime_added) cancel_date,
                mdr.label AS               cancel_reason
FROM lake_jfb_view.ultra_merchant.membership_downgrade md
         JOIN lake_jfb_view.ultra_merchant.membership_downgrade_reason mdr
              ON md.membership_downgrade_reason_id = mdr.membership_downgrade_reason_id
         JOIN lake_jfb_view.ultra_merchant.membership m
              ON md.membership_id = m.membership_id
WHERE mdr.access = 'online_cancel';

CREATE OR REPLACE TEMP TABLE _customers_last_login AS
SELECT edw_prod.stg.udf_unconcat_brand(b.customer_id) AS customer_id
FROM reporting_base_prod.shared.session b
         JOIN gfb.vw_store s
              ON s.store_id = b.store_id
GROUP BY b.customer_id
HAVING MAX(CONVERT_TIMEZONE('America/Los_Angeles', b.session_local_datetime)::DATETIME) <=
       DATEADD('month', -2, TO_DATE(CURRENT_DATE()))
ORDER BY customer_id;

CREATE OR REPLACE TEMPORARY TABLE _last_login AS
SELECT DISTINCT b.customer_id,
                b.gender
FROM _membership_baseline b
         JOIN _customers_last_login AS m
              ON b.customer_id = m.customer_id;

CREATE OR REPLACE TEMP TABLE _customer_lifetime_value_monthly AS
SELECT clvm.meta_original_customer_id AS customer_id,
       clvm.month_date,
       clvm.vip_cohort_month_date,
       clvm.is_bop_vip,
       clvm.customer_action_category,
       clvm.is_failed_billing
FROM edw_prod.analytics_base.customer_lifetime_value_monthly_cust clvm
WHERE clvm.month_date >= $start_date;

CREATE OR REPLACE TEMPORARY TABLE _failed_credit_billings AS
SELECT DISTINCT clvm.customer_id,
                dc.gender,
                clvm.vip_cohort_month_date AS recent_activation_cohort
FROM _customer_lifetime_value_monthly clvm
         JOIN _jfb_customers dc
              ON dc.customer_id = clvm.customer_id
WHERE clvm.customer_action_category IN ('Failed Billing', 'Merch Purchase and Failed Billing')
  AND clvm.is_bop_vip = 1
  AND CASE
          WHEN DATE_PART('day', TO_DATE(CURRENT_DATE())) BETWEEN 1 AND 10 THEN
              (clvm.month_date = DATEADD('month', -1, DATE_TRUNC('month', CURRENT_DATE())) OR
               clvm.month_date = DATEADD('month', -2, DATE_TRUNC('month', CURRENT_DATE())))
          WHEN DATE_PART('day', TO_DATE(CURRENT_DATE())) > 10 THEN
              (clvm.month_date = DATEADD('month', -1, DATE_TRUNC('month', CURRENT_DATE())) OR
               clvm.month_date = DATE_TRUNC('month', CURRENT_DATE()))
    END
GROUP BY clvm.customer_id,
         dc.gender,
         clvm.vip_cohort_month_date
HAVING SUM(CASE WHEN clvm.is_failed_billing = TRUE THEN 1 ELSE 0 END) = 2;

CREATE OR REPLACE TEMPORARY TABLE _inactive_customer_list AS
SELECT f.customer_id,f.gender,
       f.recent_activation_cohort
FROM _failed_credit_billings f
         JOIN _last_login l
              ON l.customer_id = f.customer_id;

CREATE OR REPLACE TEMPORARY TABLE _optin_status AS
SELECT opt.store_id,
       opt.customer_id,
       CASE
           WHEN opt.opt_in = 0 THEN 'opt-out'
           ELSE 'optin'
           END AS optin_status
FROM gfb.view_customer_opt_info opt;

CREATE OR REPLACE TEMPORARY TABLE _last_six_months AS
SELECT DISTINCT customer_id
FROM gfb.gfb_order_line_data_set_place_date
WHERE order_date >= DATEADD('month', -6, CURRENT_DATE())
  AND order_classification = 'product order';

CREATE OR REPLACE TEMPORARY TABLE _channels_combined AS
SELECT b.customer_id,
       b.gender,
       b.email,
       b.store_brand_name,
       b.store_name,
       b.store_region_abbr,
       b.store_country_abbr,
       b.default_state_province,
       lsm.customer_id                      AS bought,
       b.event_start_local_datetime,
       b.event_end_local_datetime,
       b.first_cohort,
       b.vip_cohort,
       CASE
           WHEN ic.customer_id IS NOT NULL THEN 'Currently Inactive VIP'
           ELSE b.current_vip_status
           END                              AS current_vip_status,
       b.activation_type,
       vo.num_orders,
       c.customer_log_id,
       TO_DATE(c.datetime_added)            AS gms_cancel_date,
       c.comment,
       c.gms_cancel_channel,
       CASE
           WHEN c.gms_cancel_channel IS NOT NULL
               THEN COALESCE(gcr.cancel_reason, 'Unknown')
           END                              AS gms_cancel_reason,
       TO_DATE(o.datetime_added)            AS other_cancel_date,
       o.other_cancel_channel,
       CASE
           WHEN o.other_cancel_channel = 'Online' THEN COALESCE(ocr.cancel_reason, 'Unknown')
           WHEN o.other_cancel_channel = 'Passive' THEN 'Passive - unspecific reason'
           END                              AS other_cancel_reason,
       TO_DATE(bl.datetime_added)           AS blacklist_cancel_date,
       CASE
           WHEN bl.customer_id IS NOT NULL
               THEN 'Blacklisted'
           END                              AS blacklisted,
       TO_DATE(bl2.datetime_modified)       AS blacklist_cancel_date2,
       CASE
           WHEN bl2.customer_id IS NOT NULL
               THEN 'Blacklisted'
           END                              AS blacklisted2,
       CASE
           WHEN bl.customer_id IS NOT NULL OR bl2.customer_id IS NOT NULL
               THEN 'Blacklisted - unspecific reason'
           END                              AS blacklist_cancel_reason,
       COALESCE(os.optin_status, 'no data') AS optin_status,
       b.membership_price
FROM _membership_baseline b
         LEFT JOIN _gms_cancels c
                   ON c.customer_id = b.customer_id
                       AND TO_DATE(b.event_end_local_datetime) = TO_DATE(c.datetime_added)
                       AND b.current_vip_status = 'Currently Cancelled'
         LEFT JOIN _online_passive_cancels o
                   ON o.customer_id = b.customer_id
                       AND TO_DATE(b.event_end_local_datetime) <= TO_DATE(o.datetime_added)
                       AND b.current_vip_status = 'Currently Cancelled'
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
         LEFT JOIN _optin_status os
                   ON os.customer_id = b.customer_id
         LEFT JOIN _last_six_months lsm
                   ON b.customer_id = lsm.customer_id;

CREATE OR REPLACE TEMPORARY TABLE _month_scaffold AS
SELECT DISTINCT DATE_TRUNC('month', full_date) AS activity_month
FROM edw_prod.data_model_jfb.dim_date
WHERE full_date >= $start_date
  AND full_date <= CURRENT_DATE();

CREATE OR REPLACE TEMPORARY TABLE _fk_customers AS
SELECT DISTINCT dc.customer_id,
                dc.gender,
                m.activity_month,
                mee.event_start_local_datetime,
                mee.event_end_local_datetime,
                DATEDIFF('month', mee.event_start_local_datetime,
                         IFF(mee.event_end_local_datetime < CURRENT_DATE(), m.activity_month,
                             IFF(mee.event_end_local_datetime > CURRENT_DATE(), CURRENT_DATE(),
                                 mee.event_end_local_datetime))) AS tenure,
                dc.store_brand                                   AS store_brand_name,
                dc.store_country                                 AS store_country_abbr,
                dc.email,
                mee.is_current
FROM edw_prod.data_model_jfb.fact_membership_event mee
         JOIN _month_scaffold m
              ON m.activity_month >= DATE_TRUNC('month', mee.event_start_local_datetime)
                  AND m.activity_month <= DATE_TRUNC('month', mee.event_end_local_datetime)
         JOIN _jfb_customers dc
              ON dc.customer_id = mee.customer_id
WHERE mee.membership_state = 'VIP'
  AND dc.store_brand_abbr = 'FK';

CREATE OR REPLACE TEMPORARY TABLE _sd_customers AS
SELECT DISTINCT dc.customer_id,
                dc.gender,
                m.activity_month,
                mee.event_start_local_datetime,
                mee.event_end_local_datetime,
                DATEDIFF('month', mee.event_start_local_datetime,
                         IFF(mee.event_end_local_datetime < CURRENT_DATE(), m.activity_month,
                             IFF(mee.event_end_local_datetime > CURRENT_DATE(), CURRENT_DATE(),
                                 mee.event_end_local_datetime))) AS tenure,
                dc.store_brand                                   AS store_brand_name,
                dc.store_country                                 AS store_country_abbr,
                dc.email,
                mee.is_current
FROM edw_prod.data_model_jfb.fact_membership_event mee
         JOIN _month_scaffold m
              ON m.activity_month >= DATE_TRUNC('month', mee.event_start_local_datetime)
                  AND m.activity_month <= DATE_TRUNC('month', mee.event_end_local_datetime)
         JOIN _jfb_customers dc
              ON mee.customer_id = dc.customer_id
WHERE mee.membership_state = 'VIP'
  AND dc.store_brand_abbr = 'SD';

CREATE OR REPLACE TEMPORARY TABLE _jf_customers AS
SELECT DISTINCT dc.customer_id,
                dc.gender,
                m.activity_month,
                mee.event_start_local_datetime,
                mee.event_end_local_datetime,
                DATEDIFF('month', mee.event_start_local_datetime,
                         IFF(mee.event_end_local_datetime < CURRENT_DATE(), m.activity_month,
                             IFF(mee.event_end_local_datetime > CURRENT_DATE(), CURRENT_DATE(),
                                 mee.event_end_local_datetime))) AS tenure,
                dc.store_brand                                   AS store_brand_name,
                dc.store_country                                 AS store_country_abbr,
                dc.email,
                mee.is_current
FROM edw_prod.data_model_jfb.fact_membership_event mee
         JOIN _month_scaffold m
              ON m.activity_month >= DATE_TRUNC('month', mee.event_start_local_datetime)
                  AND m.activity_month <= DATE_TRUNC('month', mee.event_end_local_datetime)
         JOIN _jfb_customers dc
              ON mee.customer_id = dc.customer_id
WHERE mee.membership_state = 'VIP'
  AND dc.store_brand_abbr = 'JF';

CREATE OR REPLACE TEMPORARY TABLE _jf_key AS
SELECT
  email,
  activity_month,
  customer_id,
  tenure,
  is_current
FROM (
  SELECT
    email,
    activity_month,
    customer_id,
    tenure,
    is_current,
    ROW_NUMBER() OVER (
      PARTITION BY email, activity_month
      ORDER BY is_current DESC, tenure DESC, customer_id
    ) AS rn
  FROM _jf_customers
  WHERE email IS NOT NULL AND email <> ''
)
WHERE rn = 1;

CREATE OR REPLACE TEMPORARY TABLE _sd_key AS
SELECT
  email,
  activity_month,
  customer_id,
  tenure,
  is_current
FROM (
  SELECT
    email,
    activity_month,
    customer_id,
    tenure,
    is_current,
    ROW_NUMBER() OVER (
      PARTITION BY email, activity_month
      ORDER BY is_current DESC, tenure DESC, customer_id
    ) AS rn
  FROM _sd_customers
  WHERE email IS NOT NULL AND email <> ''
)
WHERE rn = 1;

CREATE OR REPLACE TEMPORARY TABLE _jfsd_crossover AS
SELECT COALESCE(jf.activity_month, sd.activity_month) AS activity_month,
       COALESCE(jf.email, sd.email)                   AS email,
       jf.customer_id                                 AS jf_customer_id,
       sd.customer_id                                 AS sd_customer_id,
       jf.tenure                                      AS jf_tenure,
       sd.tenure                                      AS sd_tenure,
       jf.is_current                                  AS jf_active,
       sd.is_current                                  AS sd_active
FROM _jf_key jf
         LEFT JOIN _sd_key sd
                   ON jf.email = sd.email
                       AND jf.activity_month = sd.activity_month;

CREATE OR REPLACE TEMPORARY TABLE _brand_crossover AS
SELECT COALESCE(fk.activity_month, jc.activity_month) AS activity_month,
       COALESCE(fk.email, jc.email)                   AS email,
       jc.jf_customer_id,
       jc.sd_customer_id,
       fk.customer_id                                 AS fk_customer_id,
       jc.jf_tenure,
       jc.sd_tenure,
       fk.tenure                                      AS fk_tenure,
       jc.jf_active,
       jc.sd_active,
       fk.is_current                                  AS fk_active
FROM _fk_customers fk
         JOIN _jfsd_crossover jc
              ON fk.email = jc.email
                  AND fk.activity_month = jc.activity_month;

CREATE OR REPLACE TEMPORARY TABLE _cancelled_info AS
SELECT store_brand_name,
       store_name,
       store_region_abbr,
       store_country_abbr,
       default_state_province,
       first_cohort,
       date(DATE_TRUNC('month', event_start_local_datetime))                                                AS vip_cohort,
       activation_type,
       current_vip_status,
       gender,
       num_orders,
       COALESCE(gms_cancel_date, other_cancel_date, blacklist_cancel_date, blacklist_cancel_date2,
                event_end_local_datetime)                                                                   AS cancel_date,
       CASE
           WHEN current_vip_status = 'Currently Cancelled'
               THEN COALESCE(gms_cancel_channel, other_cancel_channel, blacklisted, blacklisted2, 'Unknown')
           WHEN current_vip_status = 'Currently Active VIP' OR current_vip_status = 'Currently Inactive VIP'
               THEN 'Active VIP'
           END                                                                                              AS cancel_channel,
       CASE
           WHEN current_vip_status = 'Currently Cancelled' THEN COALESCE(gms_cancel_reason, other_cancel_reason,
                                                                         blacklist_cancel_reason, 'Unknown')
           WHEN current_vip_status = 'Currently Active VIP' THEN 'Currently Active VIP'
           WHEN current_vip_status = 'Currently Inactive VIP' THEN 'Currently Inactive VIP'
           END                                                                                              AS cancel_reason,
       DATEDIFF('day', DATE_TRUNC('day', event_start_local_datetime),
                DATE_TRUNC('day', cancel_date))                                                             AS day_difference,
       DATEDIFF('month', DATE_TRUNC('day', event_start_local_datetime), DATE_TRUNC('day', cancel_date)) + 1 AS tenure,
       optin_status,
       membership_price,
       COUNT(DISTINCT bought)                                                                               AS purchased_six_months,
       COUNT(DISTINCT customer_id)                                                                          AS num_customers,
       SUM(IFF(jf_active = TRUE, 1, 0))                                                                     AS jf_active,
       SUM(IFF(sd_active = TRUE, 1, 0))                                                                     AS sd_active,
       SUM(IFF(fk_active = TRUE, 1, 0))                                                                     AS fk_active
FROM _channels_combined cc
         LEFT JOIN _brand_crossover bc
                   ON cc.email = bc.email
                       AND DATE_TRUNC('month', COALESCE(gms_cancel_date, other_cancel_date, blacklist_cancel_date,
                                                        blacklist_cancel_date2, event_end_local_datetime)) =
                           bc.activity_month
GROUP BY store_brand_name,
         store_name,
         store_region_abbr,
         store_country_abbr,
         first_cohort,
         date(DATE_TRUNC('month', event_start_local_datetime)),
         default_state_province,
         activation_type,
         current_vip_status,
         gender,
         num_orders,
         COALESCE(gms_cancel_date, other_cancel_date, blacklist_cancel_date, blacklist_cancel_date2,
                  event_end_local_datetime),
         CASE
             WHEN current_vip_status = 'Currently Cancelled' THEN COALESCE(gms_cancel_channel, other_cancel_channel,
                                                                           blacklisted, blacklisted2, 'Unknown')
             WHEN current_vip_status = 'Currently Active VIP' OR current_vip_status = 'Currently Inactive VIP'
                 THEN 'Active VIP'
             END,
         CASE
             WHEN current_vip_status = 'Currently Cancelled' THEN COALESCE(gms_cancel_reason, other_cancel_reason,
                                                                           blacklist_cancel_reason, 'Unknown')
             WHEN current_vip_status = 'Currently Active VIP' THEN 'Currently Active VIP'
             WHEN current_vip_status = 'Currently Inactive VIP' THEN 'Currently Inactive VIP'
             END,
         DATEDIFF('day', DATE_TRUNC('day', event_start_local_datetime), DATE_TRUNC('day', cancel_date)),
         (DATEDIFF('month', DATE_TRUNC('day', event_start_local_datetime), DATE_TRUNC('day', cancel_date)) + 1),
         optin_status,
         membership_price;

CREATE OR REPLACE TEMPORARY TABLE _cancel_final_table AS
SELECT UPPER(store_brand_name)                                                               AS business_unit,
       CASE
           WHEN default_state_province = 'CA' THEN 'CA'
           WHEN store_country_abbr != 'US' THEN 'Other Country'
           ELSE 'Other State' END                                                            AS state,
       store_region_abbr                                                                     AS region,
       DATE_TRUNC('month', cancel_date::DATE)                                                AS cancel_month,
       tenure,
       vip_cohort,
       gender,
       SUM(jf_active)                                                                        AS jf_active,
       SUM(fk_active)                                                                        AS fk_active,
       SUM(sd_active)                                                                        AS sd_active,
       SUM(purchased_six_months)                                                             AS purchased_six_months,
       SUM(CASE WHEN cancel_channel IN ('Online') THEN num_customers ELSE 0 END)             AS online_cancel,
       SUM(CASE WHEN cancel_channel IN ('Passive') THEN num_customers ELSE 0 END)            AS passive_cancel,
       SUM(CASE
               WHEN cancel_channel IN ('IVR Web Messaging', 'Web Messaging')
                        OR cancel_channel IN ('IVR Chat', 'Chat')
                   THEN num_customers
               ELSE 0 END)                                                                   AS chat_cancels,
       SUM(CASE WHEN cancel_channel IN ('IVR Email', 'Email') THEN num_customers ELSE 0 END) AS email_cancel,
       SUM(CASE WHEN cancel_channel IN ('IVR Phone', 'Phone') THEN num_customers ELSE 0 END) AS phone_cancel,
       SUM(CASE
               WHEN cancel_channel IN
                    ('GMS Other', 'Twitter DM', 'Instagram DM', 'Facebook Messenger', 'Unknown')
                   THEN num_customers
               ELSE 0 END)                                                                   AS gms_other_cancel,
       SUM(CASE WHEN cancel_channel IN ('Blacklisted') THEN num_customers ELSE 0 END)        AS blacklisted_cancel
FROM _cancelled_info
WHERE current_vip_status = 'Currently Cancelled'
GROUP BY UPPER(store_brand_name),
         CASE
             WHEN default_state_province = 'CA' THEN 'CA'
             WHEN store_country_abbr != 'US' THEN 'Other Country'
             ELSE 'Other State'
             END,
         DATE_TRUNC('month', cancel_date::DATE),
         tenure,
         store_region_abbr,
         vip_cohort,
         gender;

CREATE OR REPLACE TEMPORARY TABLE _pause_customers AS
SELECT DISTINCT UPPER(mb.store_brand_name)                                              AS business_unit,
                CASE
                    WHEN default_state_province = 'CA' THEN 'CA'
                    WHEN country != 'US' THEN 'Other Country'
                    ELSE 'Other State' END                                              AS state,
                region,
                DATEDIFF('month', DATE_TRUNC('day', event_start_local_datetime),
                         DATE_TRUNC('day', DATE_TRUNC('day', ms.date_start::DATE))) + 1 AS tenure,
                date(DATE_TRUNC('month', event_start_local_datetime))                   AS vip_cohort,
                ms.membership_id,
                m.customer_id,
                mb.gender,
                ms.membership_snooze_id,
                DATE_TRUNC('day', ms.date_start::DATE)                                  AS date_start,
                DATE_TRUNC('day', ms.date_end::DATE)                                    AS date_end,
                DATE_TRUNC('day', ms.datetime_added::DATE)                              AS datetime_added
FROM lake_jfb_view.ultra_merchant.membership_snooze ms
         JOIN lake_jfb_view.ultra_merchant.membership m
              ON ms.membership_id = m.membership_id
         JOIN _membership_baseline mb
              ON m.customer_id = mb.customer_id
                  AND DATE_TRUNC('day', ms.date_start::DATE) >= mb.event_start_local_datetime
                  AND DATE_TRUNC('day', ms.date_end::DATE) <= mb.event_end_local_datetime;

-- customers who started their pause in that month
CREATE OR REPLACE TEMPORARY TABLE _actual_pause_month AS
SELECT business_unit,
       state,
       region,
       gender,
       tenure,
       vip_cohort,
       DATE_TRUNC('month', datetime_added) AS added_month,
       COUNT(*)                            AS actual_pause_month_count
FROM _pause_customers
GROUP BY business_unit,
         state,
         region,
         gender,
         tenure,
         vip_cohort,
         DATE_TRUNC('month', datetime_added);

-- total number of customers who are currently in pause (i.e. pause in that month and have been paused from previous months)
CREATE OR REPLACE TEMPORARY TABLE _monthly_agg_pause_customers AS
SELECT pc.business_unit,
       pc.state,
       pc.region,
       pc.gender,
       DATEDIFF('month', DATE_TRUNC('day', event_start_local_datetime),
                DATE_TRUNC('day', month_date)) + 1     AS tenure,
       DATE_TRUNC('month', event_start_local_datetime) AS vip_cohort,
       month_date,
       COUNT(*)                                        AS monthly_pause_count
FROM _pause_customers pc
         JOIN _period_month pm
              ON pm.month_date >= DATE_TRUNC('month', pc.date_start)
                  AND pm.month_date <= DATE_TRUNC('month', pc.date_end)
         JOIN _membership_baseline mb
              ON pc.customer_id = mb.customer_id
                  AND pm.month_date >= mb.event_start_local_datetime
                  AND pm.month_date <= mb.event_end_local_datetime
GROUP BY pc.business_unit,
         pc.state,
         pc.region,
         pc.gender,
         (DATEDIFF('month', DATE_TRUNC('day', event_start_local_datetime),
                   DATE_TRUNC('day', month_date)) + 1),
         DATE_TRUNC('month', event_start_local_datetime),
         month_date;

CREATE OR REPLACE TEMPORARY TABLE _cancel_click_count AS
SELECT 'JUSTFAB'                         AS brand,
       TRY_TO_NUMBER(userid)             AS customer_id,
       date(LEFT(originaltimestamp, 10)) AS cancel_click_date
FROM lake_view.segment_gfb.javascript_justfab_my_vip_online_cancel_click
UNION
SELECT 'SHOEDAZZLE'                      AS brand,
       TRY_TO_NUMBER(userid)             AS customer_id,
       date(LEFT(originaltimestamp, 10)) AS cancel_click_date
FROM lake_view.segment_gfb.javascript_shoedazzle_my_vip_online_cancel_click
UNION
SELECT 'FABKIDS'                         AS brand,
       TRY_TO_NUMBER(userid)             AS customer_id,
       date(LEFT(originaltimestamp, 10)) AS cancel_click_date
FROM lake_view.segment_gfb.javascript_fabkids_my_vip_online_cancel_click;

CREATE OR REPLACE TEMPORARY TABLE _cancel_click_final AS
SELECT ccc.brand,
       ccc.customer_id,
       dc.gender,
       ccc.cancel_click_date,
       dc.default_state_province,
       dc.store_region  AS region,
       dc.store_country AS country
FROM _cancel_click_count ccc
         JOIN _jfb_customers dc
              ON ccc.customer_id = dc.customer_id;

CREATE OR REPLACE TEMPORARY TABLE _cancel_click_data AS
SELECT UPPER(mb.store_brand_name)                                              AS business_unit,
       DATE_TRUNC('month', ccd.cancel_click_date)                              AS cancel_click_month,
       CASE
           WHEN ccd.default_state_province = 'CA' THEN 'CA'
           WHEN ccd.country != 'US' THEN 'Other Country'
           ELSE 'Other State' END                                              AS state,
       ccd.region,
       DATEDIFF('month', DATE_TRUNC('day', event_start_local_datetime),
                DATE_TRUNC('day', cancel_click_date)) + 1                      AS tenure,
       DATE_TRUNC('month', DATEADD('month', -(tenure) + 1, cancel_click_date)) AS vip_cohort,
       mb.gender,
       COUNT(*)                                                                AS cancel_click_count,
       COUNT(DISTINCT ccd.membership_snooze_id)                                AS pause_count
FROM (SELECT cc.customer_id,
             cc.cancel_click_date,
             cc.default_state_province,
             cc.region,
             cc.country,
             pc.membership_snooze_id,
             pc.date_start,
             pc.date_end,
             pc.datetime_added
      FROM _cancel_click_final cc
               LEFT JOIN _pause_customers pc
                         ON cc.customer_id = pc.customer_id
                             AND cc.cancel_click_date = pc.datetime_added
      WHERE cc.customer_id IS NOT NULL) ccd
         JOIN _membership_baseline mb
              ON ccd.customer_id = mb.customer_id
                  AND ccd.cancel_click_date >= mb.event_start_local_datetime
                  AND ccd.cancel_click_date <= DATEADD('day', 2, mb.event_end_local_datetime)
GROUP BY UPPER(mb.store_brand_name),
         DATE_TRUNC('month', ccd.cancel_click_date),
         ccd.region,
         CASE
             WHEN ccd.default_state_province = 'CA' THEN 'CA'
             WHEN ccd.country != 'US' THEN 'Other Country'
             ELSE 'Other State'
             END,
         mb.gender,
         (DATEDIFF('month', DATE_TRUNC('day', event_start_local_datetime),
                   DATE_TRUNC('day', cancel_click_date)) + 1),
         DATE_TRUNC('month', DATEADD('month', -(tenure) + 1, cancel_click_date));

CREATE OR REPLACE TEMPORARY TABLE _bop_vips AS
SELECT UPPER(mb.store_brand_name)                                       AS business_unit,
       CASE
           WHEN default_state_province = 'CA' THEN 'CA'
           WHEN country != 'US' THEN 'Other Country'
           ELSE 'Other State' END                                       AS state,
       mb.store_region_abbr                                             AS region,
       clvm.month_date,
       clvm.vip_cohort_month_date,
       DATEDIFF(MONTH, clvm.vip_cohort_month_date, clvm.month_date) + 1 AS tenure,
       mb.gender,
       COUNT(DISTINCT
             CASE
                 WHEN clvm.is_bop_vip = TRUE THEN clvm.customer_id
                 END)                                                   AS bop_vip
FROM _customer_lifetime_value_monthly clvm
         JOIN _membership_baseline mb
              ON clvm.customer_id = mb.customer_id
WHERE DATEDIFF(MONTH, clvm.vip_cohort_month_date, clvm.month_date) + 1 > 0
GROUP BY mb.store_brand_name,
         mb.store_region_abbr,
         CASE
             WHEN default_state_province = 'CA' THEN 'CA'
             WHEN country != 'US' THEN 'Other Country'
             ELSE 'Other State'
             END,
         clvm.month_date,
         clvm.vip_cohort_month_date,
         mb.gender,
         (DATEDIFF(MONTH, clvm.vip_cohort_month_date, clvm.month_date) + 1);

CREATE OR REPLACE TRANSIENT TABLE gfb.gfb075_jfb_online_cancellations AS
SELECT COALESCE(cft.business_unit, ac.store_brand, ccd.business_unit, pc.business_unit,
                mapc.business_unit, bv.business_unit)                                   AS business_unit,
       COALESCE(cft.state, ac.state, ccd.state, pc.state, mapc.state, bv.state)         AS state,
       COALESCE(cft.region, ac.region, ccd.region, pc.region, mapc.region, bv.region)   AS region,
       COALESCE(cft.cancel_month, ac.month_date::DATE,
                ccd.cancel_click_month, pc.added_month, mapc.month_date, bv.month_date) AS month_date,
       COALESCE(cft.tenure, ac.tenure, ccd.tenure, pc.tenure, mapc.tenure, bv.tenure)   AS tenure,
       COALESCE(cft.vip_cohort, ac.vip_cohort, ccd.vip_cohort, pc.vip_cohort, mapc.vip_cohort,
                bv.vip_cohort_month_date)                                               AS vip_cohort,
       COALESCE(cft.gender, ac.gender, ccd.gender, pc.gender, mapc.gender,
                bv.gender)                                                              AS gender,
       IFNULL(cft.fk_active, 0)                                                         AS fk_active,
       IFNULL(cft.jf_active, 0)                                                         AS jf_active,
       IFNULL(cft.sd_active, 0)                                                         AS sd_active,
       IFNULL(cft.blacklisted_cancel, 0)                                                AS blacklisted_cancel,
       IFNULL(cft.chat_cancels, 0)                                                      AS chat_cancels,
       IFNULL(cft.email_cancel, 0)                                                      AS email_cancel,
       IFNULL(cft.gms_other_cancel, 0)                                                  AS gms_other_cancel,
       IFNULL(cft.online_cancel, 0)                                                     AS online_cancel,
       IFNULL(cft.passive_cancel, 0)                                                    AS passive_cancel,
       IFNULL(cft.phone_cancel, 0)                                                      AS phone_cancel,
       IFNULL(cft.purchased_six_months, 0)                                              AS purchased_six_months,
       IFNULL(ac.total_count, 0)                                                        AS total_count,
       IFNULL(ccd.cancel_click_count, 0)                                                AS cancel_click_count,
       IFNULL(ccd.pause_count, 0)                                                       AS save_pause_count,
       IFNULL(pc.actual_pause_month_count, 0)                                           AS actual_pause_month_count,
       IFNULL(mapc.monthly_pause_count, 0)                                              AS monthly_pause_count,
       IFNULL(bv.bop_vip, 0)                                                            AS bop_vip
FROM _cancel_final_table cft
         FULL JOIN _all_customers ac
                   ON cft.business_unit = UPPER(ac.store_brand)
                       AND cft.state = ac.state
                       AND cft.region=ac.region
                       AND cft.cancel_month = ac.month_date::DATE
                       AND cft.tenure = ac.tenure
                       AND cft.vip_cohort = ac.vip_cohort
                       AND cft.gender = ac.gender
         FULL JOIN _cancel_click_data ccd
                   ON ccd.business_unit = COALESCE(UPPER(ac.store_brand), cft.business_unit)
                       AND ccd.state = COALESCE(ac.state, cft.state)
                       AND ccd.region=COALESCE(ac.region, cft.region)
                       AND ccd.cancel_click_month = COALESCE(ac.month_date::DATE, cft.cancel_month)
                       AND ccd.tenure = COALESCE(ac.tenure, cft.tenure)
                       AND ccd.vip_cohort = COALESCE(ac.vip_cohort, cft.vip_cohort)
                       AND ccd.gender = COALESCE(ac.gender, cft.gender)
         FULL JOIN _actual_pause_month pc
                   ON pc.business_unit = COALESCE(cft.business_unit, UPPER(ac.store_brand), ccd.business_unit)
                       AND pc.state = COALESCE(cft.state, ac.state, ccd.state)
                       AND pc.region = COALESCE(cft.region, ac.region, ccd.region)
                       AND pc.added_month = COALESCE(cft.cancel_month, ac.month_date, ccd.cancel_click_month)
                       AND pc.tenure = COALESCE(cft.tenure, ac.tenure, ccd.tenure)
                       AND pc.vip_cohort = COALESCE(cft.vip_cohort, ac.vip_cohort, ccd.vip_cohort)
                       AND pc.gender = COALESCE(cft.gender, ac.gender, ccd.gender)
         FULL JOIN _monthly_agg_pause_customers mapc
                   ON mapc.business_unit =
                      COALESCE(cft.business_unit, UPPER(ac.store_brand), ccd.business_unit, pc.business_unit)
                       AND mapc.state = COALESCE(cft.state, ac.state, ccd.state, pc.state)
                       AND mapc.region = COALESCE(cft.region, ac.region, ccd.region, pc.region)
                       AND mapc.month_date =
                           COALESCE(cft.cancel_month, ac.month_date, ccd.cancel_click_month, pc.added_month)
                       AND mapc.tenure = COALESCE(cft.tenure, ac.tenure, ccd.tenure, pc.tenure)
                       AND mapc.vip_cohort = COALESCE(cft.vip_cohort, ac.vip_cohort, ccd.vip_cohort, pc.vip_cohort)
                       AND mapc.gender = COALESCE(cft.gender, ac.gender, ccd.gender, pc.gender)
         FULL JOIN _bop_vips bv
                   ON bv.business_unit =
                      COALESCE(cft.business_unit, UPPER(ac.store_brand), ccd.business_unit, pc.business_unit,
                               mapc.business_unit)
                       AND bv.state = COALESCE(cft.state, ac.state, ccd.state, pc.state, mapc.state)
                       AND bv.region = COALESCE(cft.region, ac.region, ccd.region, pc.region, mapc.region)
                       AND bv.month_date =
                           COALESCE(cft.cancel_month, ac.month_date, ccd.cancel_click_month, pc.added_month,
                                    mapc.month_date)
                       AND bv.tenure = COALESCE(cft.tenure, ac.tenure, ccd.tenure, pc.tenure, mapc.tenure)
                       AND bv.vip_cohort_month_date =
                           COALESCE(cft.vip_cohort, ac.vip_cohort, ccd.vip_cohort, pc.vip_cohort, mapc.vip_cohort)
                       AND bv.gender =
                           COALESCE(cft.gender, ac.gender, ccd.gender, pc.gender, mapc.gender)
WHERE COALESCE(cft.cancel_month, ac.month_date::DATE, ccd.cancel_click_month, pc.added_month, mapc.month_date) >= $start_date
  AND COALESCE(cft.cancel_month, ac.month_date::DATE, ccd.cancel_click_month, pc.added_month, mapc.month_date) <=CURRENT_DATE();
