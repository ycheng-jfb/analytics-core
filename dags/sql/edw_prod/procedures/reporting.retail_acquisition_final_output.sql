SET target_table = 'reporting.retail_acquisition_final_output';
SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
ALTER SESSION SET QUERY_TAG = $target_table;

CREATE OR REPLACE TEMP TABLE _vips_cancels_stg AS
SELECT DISTINCT am.date,
                vip.store_full_name                                          AS retail_store,
                am.event_store_id,
                am.vip_store_id,
                IFF(am.customer_gender = 'Unknown', 'F', am.customer_gender) AS customer_gender,
                am.finance_specialty_store,
                am.is_retail_registration,
                am.is_scrubs_customer,
                SUM(am.leads)                                                AS leads,
                SUM(am.reactivated_leads)                                    AS reactivated_leads,
                SUM(am.new_vips)                                             AS new_vips,
                SUM(am.reactivated_vips)                                     AS reactivated_vips,
                SUM(am.vips_from_reactivated_leads_m1)                       AS vips_from_reactivated_leads_m1,
                SUM(am.paid_vips)                                            AS paid_vips,
                SUM(am.unpaid_vips)                                          AS unpaid_vips,
                SUM(am.new_vips_m1)                                          AS new_vips_m1,
                SUM(am.paid_vips_m1)                                         AS paid_vips_m1,
                SUM(am.cancels)                                              AS cancels,
                SUM(am.m1_cancels)                                           AS m1_cancels,
                SUM(am.bop_vips)                                             AS bop_vips,
                SUM(am.media_spend)                                          AS media_spend,
                SUM(am.new_vips - am.cancels)                                AS net_vips
FROM analytics_base.acquisition_media_spend_daily_agg am
         JOIN data_model.dim_store st ON st.store_id = am.event_store_id
         JOIN data_model.dim_store vip ON vip.store_id = am.vip_store_id
    AND vip.store_type = 'Retail' -- retail vips
WHERE am.date_object = 'placed'
  AND LOWER(am.currency_type) = 'usd'
  AND date >= '2010-01-01'
GROUP BY am.date
       , vip.store_full_name
       , am.event_store_id
       , am.vip_store_id
       , IFF(am.customer_gender = 'Unknown', 'F', am.customer_gender)
       , am.finance_specialty_store
       , am.is_retail_registration
       , am.is_scrubs_customer
;

CREATE OR REPLACE TEMPORARY TABLE _scaffold AS
SELECT *
FROM (SELECT full_date AS date
      FROM data_model.dim_date
      WHERE full_date >= '2010-01-01'
        AND full_date <= (SELECT MAX(date) FROM _vips_cancels_stg)) d
         CROSS JOIN (SELECT DISTINCT vip.store_full_name AS retail_store
                                     , am.event_store_id
                                     , am.vip_store_id
                                     , IFF(am.customer_gender = 'Unknown', 'F', am.customer_gender) as customer_gender
                                     , am.finance_specialty_store
                                     , am.is_retail_registration
                                     , am.is_scrubs_customer
                     FROM analytics_base.acquisition_media_spend_daily_agg am
                                 JOIN data_model.dim_store st ON st.store_id = am.event_store_id
                                 JOIN data_model.dim_store vip ON vip.store_id = am.vip_store_id
                            AND vip.store_type = 'Retail' -- retail vips
                            AND vip.store_sub_type <> 'Media Spend'
                        WHERE am.date_object = 'placed'
                          AND LOWER(am.currency_type) = 'usd'
                          AND date >= '2010-01-01') a;



create or replace TEMPORARY table _vips_cancels as
SELECT s.date,
       s.retail_store,
       s.event_store_id,
       s.vip_store_id,
       s.customer_gender,
       s.finance_specialty_store,
       s.is_retail_registration,
       s.is_scrubs_customer,
       SUM(COALESCE(a.leads, 0))                             AS leads,
       SUM(COALESCE(a.reactivated_leads, 0))                 AS reactivated_leads,
       SUM(COALESCE(a.new_vips, 0))                          AS new_vips,
       SUM(COALESCE(a.reactivated_vips, 0))                  AS reactivated_vips,
       SUM(COALESCE(a.vips_from_reactivated_leads_m1, 0))    AS vips_from_reactivated_leads_m1,
       SUM(COALESCE(a.paid_vips, 0))                         AS paid_vips,
       SUM(COALESCE(a.unpaid_vips, 0))                       AS unpaid_vips,
       SUM(COALESCE(a.new_vips_m1, 0))                       AS new_vips_m1,
       SUM(COALESCE(a.paid_vips_m1, 0))                      AS paid_vips_m1,
       SUM(COALESCE(a.cancels, 0))                           AS cancels,
       SUM(COALESCE(a.m1_cancels, 0))                        AS m1_cancels,
       SUM(COALESCE(a.bop_vips, 0))                          AS bop_vips,
       SUM(COALESCE(a.media_spend, 0))                       AS media_spend,
       SUM(COALESCE(a.new_vips, 0) - COALESCE(a.cancels, 0)) AS net_vips
FROM _scaffold s
LEFT JOIN _vips_cancels_stg a ON a.date = s.date
    AND a.retail_store = s.retail_store
    AND a.event_store_id = s.event_store_id
    AND a.vip_store_id = s.vip_store_id
    AND a.customer_gender = s.customer_gender
    AND a.finance_specialty_store = s.finance_specialty_store
    AND a.is_retail_registration = s.is_retail_registration
    AND a.is_scrubs_customer = s.is_scrubs_customer
GROUP BY s.date,
         s.retail_store,
         s.event_store_id,
         s.vip_store_id,
         s.customer_gender,
         s.finance_specialty_store,
         s.is_retail_registration,
         s.is_scrubs_customer;


CREATE OR REPLACE TEMP TABLE _cumulative AS
select date,
       retail_store,
       event_store_id,
       vip_store_id,
       customer_gender,
       finance_specialty_store,
       is_retail_registration,
       is_scrubs_customer,
       leads,
       reactivated_leads,
       new_vips,
       SUM(new_vips) OVER (PARTITION BY
                               retail_store,
                               event_store_id,
                               vip_store_id,
                               customer_gender,
                               finance_specialty_store,
                               is_retail_registration,
                               is_scrubs_customer ORDER BY date) AS cumulative_new_vips,
       reactivated_vips,
       vips_from_reactivated_leads_m1,
       paid_vips,
       unpaid_vips,
       new_vips_m1,
       paid_vips_m1,
       cancels,
       SUM(cancels) OVER (PARTITION BY
                               retail_store,
                               event_store_id,
                               vip_store_id,
                               customer_gender,
                               finance_specialty_store,
                               is_retail_registration,
                               is_scrubs_customer ORDER BY date) AS cumulative_cancels,
       m1_cancels,
       bop_vips,
       media_spend,
       net_vips
FROM _vips_cancels
;


CREATE OR REPLACE TEMP TABLE _cumulative_collapsed AS
SELECT date,
       retail_store,
       event_store_id,
       vip_store_id,
       customer_gender,
       SUM(leads)                          AS leads,
       SUM(reactivated_leads)              AS reactivated_leads,
       SUM(new_vips)                       AS new_vips,
       SUM(cumulative_new_vips)            AS cumulative_new_vips,
       CASE
           WHEN DATEADD(DD, -1, CURRENT_DATE) = date THEN SUM(cumulative_new_vips)
           WHEN DAY(DATEADD(DD, 1, date)) = 1 THEN SUM(cumulative_new_vips)
           ELSE 0 END                      AS cumulative_new_vips_monthly_summary,
       SUM(reactivated_vips)               AS reactivated_vips,
       SUM(vips_from_reactivated_leads_m1) AS vips_from_reactivated_leads_m1,
       SUM(paid_vips)                      AS paid_vips,
       SUM(unpaid_vips)                    AS unpaid_vips,
       SUM(new_vips_m1)                    AS new_vips_m1,
       SUM(paid_vips_m1)                   AS paid_vips_m1,
       SUM(cancels)                        AS cancels,
       SUM(cumulative_cancels)             AS cumulative_cancels,
       CASE
           WHEN DATEADD(DD, -1, CURRENT_DATE) = date THEN SUM(cumulative_cancels)
           WHEN DAY(DATEADD(DD, 1, date)) = 1 THEN SUM(cumulative_cancels)
           ELSE 0 END                      AS cumulative_cancels_monthly_summary,
       SUM(m1_cancels)                     AS m1_cancels,
       SUM(bop_vips)                       AS bop_vips,
       SUM(media_spend)                    AS media_spend,
       SUM(net_vips)                       AS net_vips
FROM _cumulative
GROUP BY date,
         retail_store,
         event_store_id,
         vip_store_id,
         customer_gender
;



CREATE OR REPLACE TEMP TABLE _base AS
SELECT cc.date,
       DATE_TRUNC(MONTH, cc.date)                                                     AS month_date,
       dd.month_name || ' ' || dd.calendar_year                                       AS month,
       vip.store_brand,
       vip.store_region,
       vip.store_full_name                                                            AS retail_store,
       vip.store_retail_location_code || ' ' || vip.store_full_name                   AS location_code_retail_store,
       vip.store_retail_location_code,
       vip.store_retail_region,
       vip.store_retail_district,
       CASE
           WHEN vip.store_region = 'EU' THEN 'Store - EU'
           WHEN vip.store_region = 'NA' AND vip.store_sub_type = 'Browser' THEN 'Store'
           ELSE vip.store_sub_type END                                                AS store_sub_type,
       cc.event_store_id,
       cc.vip_store_id,
       cc.customer_gender,
       cc.leads,
       cc.reactivated_leads,
       cc.new_vips,
       cc.cumulative_new_vips,
       cc.cumulative_new_vips_monthly_summary,
       cc.reactivated_vips,
       cc.vips_from_reactivated_leads_m1,
       cc.paid_vips,
       cc.unpaid_vips,
       cc.new_vips_m1,
       cc.paid_vips_m1,
       cc.cancels,
       cc.cumulative_cancels,
       cc.cumulative_cancels_monthly_summary,
       cc.m1_cancels,
       cc.bop_vips,
       cc.media_spend,
       cc.net_vips,
       cc.cumulative_new_vips - cc.cumulative_cancels                                    AS cumulative_net_vips,
       cc.cumulative_new_vips_monthly_summary - cc.cumulative_cancels_monthly_summary    AS cumulative_net_vips_monthly_summary
FROM _cumulative_collapsed cc
         JOIN data_model.dim_date dd ON cc.date = dd.full_date
         JOIN data_model.dim_store st ON st.store_id = cc.event_store_id
         JOIN data_model.dim_store vip ON vip.store_id = cc.vip_store_id
    AND vip.store_type = 'Retail' -- retail vips
;

TRUNCATE TABLE reporting.retail_acquisition_final_output;

INSERT INTO reporting.retail_acquisition_final_output (
        date,
        month_date,
        month,
        report_segment,
        rank,
        store_brand,
        retail_store,
        location_code_retail_store,
        store_retail_location_code,
        store_retail_region,
        store_retail_district,
        store_sub_type,
        event_store_id,
        vip_store_id,
        customer_gender,
        leads,
        reactivated_leads,
        new_vips,
        reactivated_vips,
        vips_from_reactivated_leads_m1,
        paid_vips,
        unpaid_vips,
        new_vips_m1,
        paid_vips_m1,
        cancels,
        m1_cancels,
        bop_vips,
        media_spend,
        cumulative_new_vips,
        cumulative_cancels,
        net_vips,
        cumulative_net_vips,
        cumulative_new_vips_monthly_summary,
        cumulative_cancels_monthly_summary,
        cumulative_net_vips_monthly_summary,
        meta_create_datetime,
        meta_update_datetime
)
SELECT date,
       month_date,
       month,
       'Summary-All Stores'                     AS report_segment,
       1                                        AS rank,
       store_brand,
       'Total'                                  AS retail_store,
       'Total'                                  AS location_code_retail_store,
       'Total'                                  AS store_retail_location_code,
       'Total'                                  AS store_retail_region,
       'Total'                                  AS store_retail_district,
       'Total'                                  AS store_sub_type,
       -1                                       AS event_store_id,
       -1                                       AS vip_store_id,
       customer_gender,
       SUM(leads)                               AS leads,
       SUM(reactivated_leads)                   AS reactivated_leads,
       SUM(new_vips)                            AS new_vips,
       SUM(reactivated_vips)                    AS reactivated_vips,
       SUM(vips_from_reactivated_leads_m1)      AS vips_from_reactivated_leads_m1,
       SUM(paid_vips)                           AS paid_vips,
       SUM(unpaid_vips)                         AS unpaid_vips,
       SUM(new_vips_m1)                         AS new_vips_m1,
       SUM(paid_vips_m1)                        AS paid_vips_m1,
       SUM(cancels)                             AS cancels,
       SUM(m1_cancels)                          AS m1_cancels,
       SUM(bop_vips)                            AS bop_vips,
       SUM(media_spend)                         AS media_spend,
       SUM(cumulative_new_vips)                 AS cumulative_new_vips,
       SUM(cumulative_cancels)                  AS cumulative_cancels,
       SUM(net_vips)                            AS net_vips,
       SUM(cumulative_net_vips)                 AS cumulative_net_vips,
       SUM(cumulative_new_vips_monthly_summary) AS cumulative_new_vips_monthly_summary,
       SUM(cumulative_cancels_monthly_summary)  AS cumulative_cancels_monthly_summary,
       SUM(cumulative_net_vips_monthly_summary) AS cumulative_net_vips_monthly_summary,
       $execution_start_time                    AS meta_create_datetime,
       $execution_start_time                    AS meta_update_datetime
FROM _base
GROUP BY date,
         month_date,
         month,
         store_brand,
         customer_gender,
         $execution_start_time

UNION ALL

SELECT date,
       month_date,
       month,
       'Summary-' || store_sub_type             AS report_segment,
       CASE
           WHEN store_sub_type = 'Store' THEN 2
           WHEN store_sub_type = 'Varsity' THEN 3
           WHEN store_sub_type = 'Legging Bar' THEN 4
           WHEN store_sub_type = 'Tough Mudder' THEN 5
           WHEN store_sub_type = 'Store - EU' THEN 6
           ELSE 7
           END                                  AS rank,
       store_brand,
       store_sub_type                           AS retail_store,
       store_sub_type                           AS location_code_retail_store,
       store_sub_type                           AS store_retail_location_code,
       store_sub_type                           AS store_retail_region,
       store_sub_type                           AS store_retail_district,
       store_sub_type,
       -1                                       AS event_store_id,
       -1                                       AS vip_store_id,
       customer_gender,
       SUM(leads)                               AS leads,
       SUM(reactivated_leads)                   AS reactivated_leads,
       SUM(new_vips)                            AS new_vips,
       SUM(reactivated_vips)                    AS reactivated_vips,
       SUM(vips_from_reactivated_leads_m1)      AS vips_from_reactivated_leads_m1,
       SUM(paid_vips)                           AS paid_vips,
       SUM(unpaid_vips)                         AS unpaid_vips,
       SUM(new_vips_m1)                         AS new_vips_m1,
       SUM(paid_vips_m1)                        AS paid_vips_m1,
       SUM(cancels)                             AS cancels,
       SUM(m1_cancels)                          AS m1_cancels,
       SUM(bop_vips)                            AS bop_vips,
       SUM(media_spend)                         AS media_spend,
       SUM(cumulative_new_vips)                 AS cumulative_new_vips,
       SUM(cumulative_cancels)                  AS cumulative_cancels,
       SUM(net_vips)                            AS net_vips,
       SUM(cumulative_net_vips)                 AS cumulative_net_vips,
       SUM(cumulative_new_vips_monthly_summary) AS cumulative_new_vips_monthly_summary,
       SUM(cumulative_cancels_monthly_summary)  AS cumulative_cancels_monthly_summary,
       SUM(cumulative_net_vips_monthly_summary) AS cumulative_net_vips_monthly_summary,
       $execution_start_time                    AS meta_create_datetime,
       $execution_start_time                    AS meta_update_datetime
FROM _base b
GROUP BY date,
         month_date,
         month,
         store_brand,
         store_sub_type,
         customer_gender,
         $execution_start_time

UNION ALL

SELECT date,
       month_date,
       month,
       'Datadump'                               AS report_segment,
       8                                        AS rank,
       store_brand,
       retail_store,
       location_code_retail_store,
       store_retail_location_code,
       store_retail_region,
       store_retail_district,
       store_sub_type,
       event_store_id,
       vip_store_id,
       customer_gender,
       SUM(leads)                               AS leads,
       SUM(reactivated_leads)                   AS reactivated_leads,
       SUM(new_vips)                            AS new_vips,
       SUM(reactivated_vips)                    AS reactivated_vips,
       SUM(vips_from_reactivated_leads_m1)      AS vips_from_reactivated_leads_m1,
       SUM(paid_vips)                           AS paid_vips,
       SUM(unpaid_vips)                         AS unpaid_vips,
       SUM(new_vips_m1)                         AS new_vips_m1,
       SUM(paid_vips_m1)                        AS paid_vips_m1,
       SUM(cancels)                             AS cancels,
       SUM(m1_cancels)                          AS m1_cancels,
       SUM(bop_vips)                            AS bop_vips,
       SUM(media_spend)                         AS media_spend,
       SUM(cumulative_new_vips)                 AS cumulative_new_vips,
       SUM(cumulative_cancels)                  AS cumulative_cancels,
       SUM(net_vips)                            AS net_vips,
       SUM(cumulative_net_vips)                 AS cumulative_net_vips,
       SUM(cumulative_new_vips_monthly_summary) AS cumulative_new_vips_monthly_summary,
       SUM(cumulative_cancels_monthly_summary)  AS cumulative_cancels_monthly_summary,
       SUM(cumulative_net_vips_monthly_summary) AS cumulative_net_vips_monthly_summary,
       $execution_start_time                    AS meta_create_datetime,
       $execution_start_time                    AS meta_update_datetime
FROM _base
GROUP BY date,
         month_date,
         month,
         report_segment,
         rank,
         store_brand,
         retail_store,
         location_code_retail_store,
         store_retail_location_code,
         store_retail_region,
         store_retail_district,
         store_sub_type,
         event_store_id,
         vip_store_id,
         customer_gender,
         $execution_start_time
;
