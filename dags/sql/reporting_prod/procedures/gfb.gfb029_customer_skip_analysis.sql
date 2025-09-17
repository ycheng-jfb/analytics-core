SET start_date = '2019-01-01';

CREATE OR REPLACE TEMPORARY TABLE _all_skips AS
SELECT st.store_brand || ' ' || st.store_region                             AS store,
       dc.customer_id,
       ms.membership_skip_id,
       ms.session_id,
       CAST(ms.datetime_added AS DATE)                                      AS skip_date,
       p.date_period_start                                                  AS skip_month,
       msr.label                                                            AS skip_reason,
       DAYOFMONTH(skip_date)                                                AS skip_day_of_month,
       dv.membership_price,
       dv.first_activating_cohort,
       DATEDIFF(MONTH, dv.first_activating_cohort, p.date_period_start) + 1 AS tenure
FROM lake_jfb_view.ultra_merchant.membership_skip ms
     JOIN lake_jfb_view.ultra_merchant.membership m
          ON m.membership_id = ms.membership_id
     JOIN lake_jfb_view.ultra_merchant.period p
          ON p.period_id = ms.period_id
     JOIN edw_prod.data_model_jfb.dim_customer dc
          ON dc.customer_id = m.customer_id
              AND dc.is_test_customer = 0
     JOIN gfb.vw_store st
          ON st.store_id = dc.store_id
     LEFT JOIN lake_jfb_view.ultra_merchant.membership_skip_reason msr
               ON msr.membership_skip_reason_id = ms.membership_skip_reason_id
     JOIN gfb.gfb_dim_vip dv
          ON dv.customer_id = dc.customer_id
WHERE p.date_period_start >= $start_date
  AND tenure > 1;

CREATE OR REPLACE TEMPORARY TABLE _fk_skip_reason AS
SELECT DISTINCT dv.business_unit,
                DATE_TRUNC(MONTH, md.datetime_added)                                                 AS month,
                FIRST_VALUE(REPLACE(md.value, '-', ' '))
                            OVER (PARTITION BY dv.customer_id, month ORDER BY md.datetime_added ASC) AS skip_reason,
                dv.customer_id
FROM lake_jfb_view.ultra_merchant.membership_detail md
     JOIN lake_jfb_view.ultra_merchant.membership m
          ON m.membership_id = md.membership_id
     JOIN gfb.gfb_dim_vip dv
          ON dv.customer_id = m.customer_id
              AND dv.business_unit = 'FABKIDS'
WHERE md.name LIKE '%skip-reason%'
  AND month >= $start_date;

CREATE OR REPLACE TEMPORARY TABLE _customers_with_session AS
SELECT DISTINCT skip.customer_id,
                skip.skip_month,
                skip.membership_skip_id
FROM _all_skips skip
     JOIN reporting_base_prod.shared.session s
          ON edw_prod.stg.udf_unconcat_brand(s.customer_id) = skip.customer_id
              AND skip.session_id = s.session_id
              AND DAYOFMONTH(CAST(s.session_local_datetime AS DATE)) < 6
              AND s.is_in_segment = TRUE;

CREATE OR REPLACE TRANSIENT TABLE gfb.gfb029_customer_skip_analysis AS
SELECT skip.store,
       skip.skip_month,
       COALESCE(fsr.skip_reason, skip.skip_reason, 'No Reason') AS skip_reason,
       (CASE
            WHEN cws.customer_id IS NULL AND skip.skip_day_of_month < 6 THEN 'Skips Without Session In First 5 Days'
            WHEN skip.session_id IS NOT NULL AND skip.skip_day_of_month >= 25 THEN 'Skips With Session After 25th'
            WHEN skip.session_id IS NOT NULL AND skip.skip_day_of_month >= 6 AND skip.skip_day_of_month <= 10
                THEN 'Skips With Session Between 6th and 10th'
            WHEN skip.session_id IS NOT NULL AND skip.skip_day_of_month >= 11 AND skip.skip_day_of_month < 25
                THEN 'Skips With Session Between 11th and 24th'
            WHEN skip.session_id IS NULL THEN 'Skips Without Session'
            WHEN cws.customer_id IS NOT NULL AND skip.skip_day_of_month < 6 THEN 'Skips With Session In First 5 Days'
           END)                                                 AS skip_group,
       skip.membership_price,
       skip.first_activating_cohort,
       skip.tenure,
       COUNT(DISTINCT skip.customer_id)                         AS total_skips
FROM _all_skips skip
     LEFT JOIN _customers_with_session cws
               ON cws.membership_skip_id = skip.membership_skip_id
     LEFT JOIN _fk_skip_reason fsr
               ON fsr.customer_id = skip.customer_id
                   AND fsr.month = skip.skip_month
GROUP BY skip.store,
         skip.skip_month,
         COALESCE(fsr.skip_reason, skip.skip_reason, 'No Reason'),
         skip_group,
         skip.membership_price,
         skip.first_activating_cohort,
         skip.tenure;
