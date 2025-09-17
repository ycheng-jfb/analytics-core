CREATE OR REPLACE TEMPORARY TABLE _lead_dates AS
SELECT dc.customer_id,
       ds.store_brand                                                        AS store_brand_name,
       ds.store_region                                                       AS store_region_abbr,
       (CASE
            WHEN dc.specialty_country_code = 'GB' THEN 'UK'
            WHEN dc.specialty_country_code != 'Unknown' THEN dc.specialty_country_code
            ELSE ds.store_country END)                                       AS store_country_abbr,
       DATE_TRUNC('month', fme.event_start_local_datetime)                   AS lead_cohort,
       DATE_FROM_PARTS(dc.birth_year, dc.birth_month, 1)                     AS birthday_month,
       DATEDIFF('month', fme.event_start_local_datetime, CURRENT_DATE()) + 1 AS tenure_by_month,
       DATEDIFF('day', fme.event_start_local_datetime, CURRENT_DATE()) + 1   AS tenure_by_day,
       DATEDIFF('year', birthday_month, CURRENT_DATE())                      AS age,
       dc.default_state_province                                             AS state,
       dc.how_did_you_hear                                                   AS customer_referrer
FROM edw_prod.data_model_jfb.fact_membership_event fme
         JOIN edw_prod.data_model_jfb.dim_customer dc
              ON fme.customer_id = dc.customer_id
         JOIN gfb.vw_store ds
              ON ds.store_id = dc.store_id
WHERE fme.is_current = 1
  AND fme.membership_state = 'Lead';

CREATE OR REPLACE TEMPORARY TABLE _sailthru_optin AS
SELECT DISTINCT
       customer_id,
       CASE
           WHEN opt_in = 1 THEN 'opt_in'
           WHEN opt_in = 0 THEN 'opt_out'
           ELSE 'unknown'
           END AS opt_out_status
FROM gfb.view_customer_opt_info;

CREATE OR REPLACE TEMPORARY TABLE _ind_customers AS
SELECT ld.customer_id,
       ld.store_brand_name,
       ld.store_region_abbr,
       ld.store_country_abbr,
       ld.lead_cohort,
       CASE
           WHEN ld.tenure_by_day <= 7 THEN 'D' || CAST(ld.tenure_by_day AS VARCHAR(20))
           WHEN ld.tenure_by_day <= 12 THEN 'D8 - D12'
           WHEN ld.tenure_by_day <= 30 THEN 'D13 - D30'
           WHEN ld.tenure_by_month > 1 AND ld.tenure_by_month <= 3 THEN 'M1 - M3'
           WHEN ld.tenure_by_month >= 4 AND ld.tenure_by_month <= 6 THEN 'M4 - M6'
           WHEN ld.tenure_by_month >= 7 AND ld.tenure_by_month <= 12 THEN 'M7 - M12'
           WHEN ld.tenure_by_month > 12 THEN 'Over 1 year'
           ELSE 'Unknown Tenure'
           END AS tenure,
       CASE
           WHEN ld.age >= 18 AND ld.age <= 20 THEN '18-20'
           WHEN ld.age >= 21 AND ld.age <= 25 THEN '21-25'
           WHEN ld.age >= 26 AND ld.age <= 30 THEN '26-30'
           WHEN ld.age >= 31 AND ld.age <= 35 THEN '31-35'
           WHEN ld.age >= 35 AND ld.age <= 45 THEN '36-45'
           WHEN ld.age > 45 AND ld.age <= 100 THEN 'Over 45'
           ELSE 'Unknown Age'
           END AS age,
       ld.state,
       CASE
           WHEN ld.customer_referrer IN
                (
                 'Instagram',
                 'Rihanna',
                 'Facebook',
                 'Unknown',
                 'Friend',
                 'YouTube',
                 'Influencer/Blogger',
                 'Search Engine',
                 'TV',
                 'Other',
                 'Banner Ad',
                 'Amazon',
                 'Influencer',
                 'Online Magazine',
                 'Google',
                 'Pinterest',
                 'TikTok',
                 'Event') THEN ld.customer_referrer
           WHEN ld.customer_referrer IS NOT NULL THEN 'Individual Influencer Names + Other'
           ELSE 'Unsure HDYH'
           END AS hdyh,
       so.opt_out_status
FROM _lead_dates ld
         LEFT JOIN _sailthru_optin so
                   ON so.customer_id = ld.customer_id;

CREATE OR REPLACE TRANSIENT TABLE gfb.gfb017_lead_universe AS
SELECT store_brand_name,
       store_region_abbr,
       store_country_abbr,
       lead_cohort,
       age,
       tenure,
       state,
       hdyh,
       opt_out_status,
       COUNT(customer_id) AS num_customers
FROM _ind_customers
GROUP BY store_brand_name,
         store_region_abbr,
         store_country_abbr,
         lead_cohort,
         age,
         tenure,
         state,
         hdyh,
         opt_out_status;
