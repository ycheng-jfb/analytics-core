CREATE OR REPLACE TEMPORARY TABLE _all_customers AS
SELECT st.store_brand             AS store_brand_name,
       st.store_brand_abbr,
       dv.country                 AS store_country_abbr,
       st.store_region            AS store_region_abbr,
       dv.customer_id,
       dv.first_activating_cohort AS vip_cohort,
       DATEDIFF(DAY, dv.registration_date, COALESCE(dv.first_activating_date, CURRENT_DATE())) +
       1                          AS lead_tenure,
       CASE
           WHEN dv.current_membership_status = 'VIP'
               THEN 1
           ELSE 0 END             AS is_active
FROM gfb.gfb_dim_vip dv
         JOIN gfb.vw_store st
              ON st.store_id = dv.store_id
                  AND dv.first_activating_cohort IS NOT NULL
                  AND dv.first_activating_cohort != '1900-01-01';

-- EMP New vs Converted VIPs based on nmp_migration tag & activation date
CREATE OR REPLACE TEMPORARY TABLE _emp_new_vs_converted_vips AS
SELECT ac.customer_id,
       ac.vip_cohort,
       'EMP New vs Converted VIPs' AS custom_segment_category,
       'Converted VIPs'            AS custom_segment
FROM lake_jfb_view.ultra_merchant.membership_detail md
         JOIN lake_jfb_view.ultra_merchant.membership m
              ON m.membership_id = md.membership_id
         JOIN _all_customers ac
              ON ac.customer_id = m.customer_id
WHERE md.name = 'nmp_migration'
UNION
SELECT fa.customer_id,
       DATE_TRUNC('month', activation_local_datetime) AS vip_cohort,
       'EMP New vs Converted VIPs'                    AS custom_segment_category,
       'New VIPs'                                     AS custom_segment
FROM edw_prod.data_model_jfb.fact_activation fa
         JOIN edw_prod.data_model_jfb.dim_store ds ON fa.store_id = ds.store_id
WHERE activation_local_datetime >= '2023-07-18'
  AND ds.store_brand = 'JustFab'
  AND ds.store_region = 'NA'
  AND fa.is_current = 'true'
UNION
SELECT fa.customer_id,
       DATE_TRUNC('month', activation_local_datetime) AS vip_cohort,
       'EMP New vs Converted VIPs'                    AS custom_segment_category,
       'New VIPs'                                     AS custom_segment
FROM edw_prod.data_model_jfb.fact_activation fa
         JOIN edw_prod.data_model_jfb.dim_store ds ON fa.store_id = ds.store_id
WHERE activation_local_datetime >= '2023-09-26'
  AND ds.store_brand = 'FabKids'
  AND ds.store_region = 'NA'
  AND fa.is_current = 'true'
UNION
SELECT fa.customer_id,
       DATE_TRUNC('month', activation_local_datetime) AS vip_cohort,
       'EMP New vs Converted VIPs'                    AS custom_segment_category,
       'New VIPs'                                     AS custom_segment
FROM edw_prod.data_model_jfb.fact_activation fa
         JOIN edw_prod.data_model_jfb.dim_store ds ON fa.store_id = ds.store_id
WHERE activation_local_datetime >= '2023-09-21'
  AND ds.store_brand = 'ShoeDazzle'
  AND ds.store_region = 'NA'
  AND fa.is_current = 'true';

--Due to promo setup mistake, dim_customer does not have these customer flagged as free trial
CREATE OR REPLACE TEMPORARY TABLE _free_trial_da15752 AS
SELECT DISTINCT ac.customer_id,
                ac.vip_cohort,
                'Free Trial' AS custom_segment_category,
                'Free Trial' AS custom_segment
FROM gfb.gfb_order_line_data_set_place_date olp
         JOIN _all_customers ac
              ON ac.customer_id = olp.customer_id
WHERE olp.order_classification = 'product order'
  AND olp.order_type = 'vip activating'
  AND (
            olp.promo_code_1 IN ('GENFPL95OFF', 'GENFPL90OFF')
        OR
            olp.promo_code_2 IN ('GENFPL95OFF', 'GENFPL90OFF')
    )
  AND olp.order_date >= '2020-12-14'
  AND olp.order_date < '2020-12-21';

CREATE OR REPLACE TEMPORARY TABLE _free_trial AS
SELECT customer_id,
       vip_cohort,
       custom_segment_category,
       custom_segment
FROM _free_trial_da15752 ftd15752
UNION
SELECT ac.customer_id,
       ac.vip_cohort,
       'Free Trial'                  AS custom_segment_category,
       CASE
           WHEN dc.is_free_trial = 1 THEN 'Free Trial'
           ELSE 'Not Free Trial' END AS custom_segment
FROM _all_customers ac
         JOIN edw_prod.data_model_jfb.dim_customer dc
              ON dc.customer_id = ac.customer_id
WHERE ac.customer_id NOT IN (SELECT DISTINCT a.customer_id
                             FROM _free_trial_da15752 a);

CREATE OR REPLACE TEMPORARY TABLE _membership_customers AS
SELECT ac.customer_id, md.name
FROM _all_customers ac
         JOIN lake_jfb_view.ultra_merchant.membership m
              ON m.customer_id = ac.customer_id
         JOIN lake_jfb_view.ultra_merchant.membership_detail md
              ON md.membership_id = m.membership_id
WHERE ac.store_brand_abbr = 'FK'
  AND md.name IN ('vip-3995-upgrade-complete', '2995upgrade3995', '3995upgrade4995', '3995legacyupgrade499')
  AND md.value = 1;

CREATE OR REPLACE TEMPORARY TABLE _membership_price AS
SELECT ac.customer_id,
       ac.vip_cohort,
       'Membership Price'                                     AS custom_segment_category,
       CAST((CASE
                 WHEN ac.store_region_abbr = 'EU' AND m.price = 399 THEN 39.95
                 WHEN ac.store_region_abbr = 'EU' AND m.price = 349 THEN 39.95
                 ELSE ROUND(m.price, 2) END) AS VARCHAR(100)) AS custom_segment
FROM _all_customers ac
         JOIN lake_jfb_view.ultra_merchant.membership m
              ON m.customer_id = ac.customer_id
WHERE ac.store_brand_abbr != 'FK'
UNION
SELECT ac.customer_id,
       ac.vip_cohort,
       'Membership Price'                                   AS custom_segment_category,
       CASE
           WHEN upglegacy_49.customer_id IS NOT NULL THEN '49.95 legacy upgrade'
           WHEN upg_49.customer_id IS NOT NULL THEN '49.95 upgrade'
           WHEN upg_39.customer_id IS NOT NULL THEN '39.95 upgrade'
           WHEN optin.customer_id IS NOT NULL THEN 'opt-in originally $29.95'
           WHEN ac.store_region_abbr = 'EU' AND m.price = 399 THEN '39.95'
           WHEN ac.store_region_abbr = 'EU' AND m.price = 349 THEN '39.95'
           ELSE CAST(ROUND(m.price, 2) AS VARCHAR(100)) END AS custom_segment
FROM _all_customers ac
         JOIN lake_jfb_view.ultra_merchant.membership m
              ON m.customer_id = ac.customer_id
         LEFT JOIN
         (SELECT DISTINCT mc.customer_id
          FROM _membership_customers mc
          WHERE mc.name = 'vip-3995-upgrade-complete') optin
              ON optin.customer_id = ac.customer_id
         LEFT JOIN
         (SELECT DISTINCT mc.customer_id
          FROM _membership_customers mc
          WHERE mc.name = '2995upgrade3995') upg_39
              ON upg_39.customer_id = ac.customer_id
         LEFT JOIN
         (SELECT DISTINCT mc.customer_id
          FROM _membership_customers mc
          WHERE mc.name = '3995upgrade4995') upg_49
              ON upg_49.customer_id = ac.customer_id
         LEFT JOIN
         (SELECT DISTINCT mc.customer_id
          FROM _membership_customers mc
          WHERE mc.name = '3995legacyupgrade499') upglegacy_49
              ON upglegacy_49.customer_id = ac.customer_id
WHERE ac.store_brand_abbr = 'FK';

-- FK Membership Upgrades by Phase 1 and Phase
CREATE OR REPLACE TEMPORARY TABLE _fk_3995_upgrade_4995_phases AS
SELECT ac.customer_id,
       ac.vip_cohort,
       'FK 49.95 upgrade by Phase' AS custom_segment_category,
       CASE
           WHEN name = '3995upgrade4995' AND md.datetime_modified <= '2022-09-30' THEN 'Phase 1'
           ELSE 'Phase 2'
           END                     AS custom_segment
FROM _all_customers ac
         JOIN lake_jfb_view.ultra_merchant.membership m
              ON m.customer_id = ac.customer_id
         JOIN lake_jfb_view.ultra_merchant.membership_detail md
              ON md.membership_id = m.membership_id
WHERE name = '3995upgrade4995';

CREATE OR REPLACE TEMPORARY TABLE _mobile_app_vip AS
SELECT ac.customer_id,
       ac.vip_cohort,
       'Mobile App User' AS custom_segment_category,
       CASE
           WHEN a.customer_id IS NOT NULL THEN 'Y'
           ELSE 'N' END  AS custom_segment
FROM _all_customers ac
         LEFT JOIN
         (SELECT DISTINCT ac.customer_id
          FROM _all_customers ac
                   JOIN reporting_base_prod.shared.session_single_view_media smd
                        ON edw_prod.stg.udf_unconcat_brand(smd.customer_id) = ac.customer_id
                   JOIN gfb.vw_store st
                        ON st.store_id = smd.store_id
                            AND st.store_type = 'Mobile App'
          WHERE smd.sessionlocaldatetime >= DATEADD(YEAR, -1, CURRENT_DATE())) a ON a.customer_id = ac.customer_id;

CREATE OR REPLACE TRANSIENT TABLE gfb.gfb001_customer_seg_all_time AS
SELECT customer_id,
       vip_cohort,
       custom_segment_category,
       custom_segment
FROM _free_trial
UNION
SELECT customer_id,
       vip_cohort,
       custom_segment_category,
       custom_segment
FROM _membership_price
UNION
SELECT customer_id,
       vip_cohort,
       custom_segment_category,
       custom_segment
FROM _fk_3995_upgrade_4995_phases
UNION
SELECT ac.customer_id,
       ac.vip_cohort,
       'Lead Tenure'                                           AS custom_segment_category,
       CASE
           WHEN ac.lead_tenure > 7 AND ac.lead_tenure < 16 THEN 'D8-D15'
           WHEN ac.lead_tenure >= 16 AND ac.lead_tenure < 31 THEN 'D16-D30'
           WHEN ac.lead_tenure >= 31 AND ac.lead_tenure < 61 THEN 'M2'
           WHEN ac.lead_tenure >= 61 AND ac.lead_tenure < 91 THEN 'M3'
           WHEN ac.lead_tenure >= 91 AND ac.lead_tenure < 121 THEN 'M4'
           WHEN ac.lead_tenure >= 121 AND ac.lead_tenure < 151 THEN 'M5'
           WHEN ac.lead_tenure >= 151 THEN 'M6+'
           ELSE 'D' || CAST(ac.lead_tenure AS VARCHAR(10)) END AS custom_segment
FROM _all_customers ac
WHERE ac.lead_tenure > 0
UNION
SELECT customer_id,
       vip_cohort,
       custom_segment_category,
       custom_segment
FROM _mobile_app_vip
UNION
SELECT customer_id,
       vip_cohort,
       custom_segment_category,
       custom_segment
FROM _emp_new_vs_converted_vips;

CREATE OR REPLACE TRANSIENT TABLE gfb.gfb001_customer_seg_count_all_time AS
SELECT UPPER(ac.store_brand_name)   AS store_brand_name,
       UPPER(ac.store_country_abbr) AS store_country_abbr,
       UPPER(ac.store_region_abbr)  AS store_region_abbr,
       ac.vip_cohort,
       cs.custom_segment_category,
       cs.custom_segment,
       ac.is_active,
       COUNT(cs.customer_id)        AS customer_count
FROM gfb.gfb001_customer_seg_all_time cs
         JOIN _all_customers ac
              ON ac.customer_id = cs.customer_id
GROUP BY UPPER(ac.store_brand_name),
         UPPER(ac.store_country_abbr),
         UPPER(ac.store_region_abbr),
         ac.vip_cohort,
         cs.custom_segment_category,
         cs.custom_segment,
         ac.is_active;
