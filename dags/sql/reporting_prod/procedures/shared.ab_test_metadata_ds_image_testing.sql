CREATE OR REPLACE TEMP TABLE _testing_calendar__meta_data AS
SELECT DISTINCT m.test_framework_id,
                test_type,
                master_product_id,
                m.campaign_code,
                m.test_key,
                m.test_label,
                master_test_number,
                MIN(product_effective_start_date_pst)                                                AS min_test_start_datetime_pst,
                MAX(max_record_pst)                                                                  AS last_refreshed_hq_datetime,
                MAX(CONVERT_TIMEZONE('America/Los_Angeles', ab_test_start_local_datetime))::datetime AS max_ab_test_start_datetime
FROM reporting_base_prod.shared.session_ab_test_ds_image_test AS m
GROUP BY 1, 2, 3, 4, 5, 6, 7;

CREATE OR REPLACE TEMP TABLE _testing_calendar__last_3_months_campaign_code AS
SELECT DISTINCT campaign_code,
                master_test_number,
                master_product_id,
                MIN(ab_test_start_local_datetime) AS min_campaign_code_datetime,
                MAX(ab_test_start_local_datetime) AS max_campaign_code_datetime
FROM reporting_base_prod.shared.session_ab_test_ds_image_test
GROUP BY campaign_code, master_test_number, master_product_id
HAVING MAX(ab_test_start_local_datetime) >= CURRENT_DATE - INTERVAL '3 month'
   AND MAX(ab_test_start_local_datetime) <= CURRENT_DATE + INTERVAL '1 day';

CREATE OR REPLACE TEMP TABLE _testing_calendar__max_datetime_campaign_code AS
SELECT DISTINCT test_framework_id,
                test_key,
                test_label,
                store_brand,
                min_campaign_code_datetime,
                max_campaign_code_datetime,
                c.master_test_number,
                c.master_product_id
FROM _testing_calendar__last_3_months_campaign_code AS c
         JOIN (SELECT DISTINCT campaign_code,
                               test_framework_id,
                               test_key,
                               test_label,
                               store_brand,
                               master_test_number,
                               master_product_id
               FROM reporting_base_prod.shared.session_ab_test_ds_image_test) AS m
              ON m.campaign_code = c.campaign_code
                  AND m.master_test_number = c.master_test_number
                  AND m.master_product_id = c.master_product_id;

CREATE OR REPLACE TEMP TABLE _testing_calendar__avail_in_abt_dash AS
SELECT DISTINCT test_key,
                test_label,
                master_test_number,
                product_id::INT AS product_id
FROM shared.ab_test_ds_image_testing;

CREATE OR REPLACE TEMP TABLE _testing_calendar__product_test_times AS
SELECT effective_start_date AS product_effective_start_date_pst,
       effective_end_date   AS product_effective_end_date_pst,
       evaluation_batch_id,
       is_current_record,
       master_product_id    AS meta_original_master_product_id,
       master_test_number
FROM lake_fl.ultra_rollup.product_image_test_config_history
WHERE image_test_flag = TRUE
  AND hvr_is_deleted = FALSE
  AND evaluation_message = 'START';

CREATE OR REPLACE TEMP TABLE _testing_calendar__meta AS
SELECT DISTINCT m.store_brand,
                m.test_label,
                m.campaign_code,
                m.test_key,
                SPLIT_PART(m.test_key, '_', 2)                                            AS test_key_version,
                CAST(m.test_framework_id AS SMALLINT)                                     AS test_framework_id,
                test_framework_description,
                m.test_type,
                m.master_test_number,
                max.master_product_id,
                m.product_name,
                m.color,
                LISTAGG(DISTINCT m.platform, ', ')                                        AS platform,
                m.sub_brand,
                m.gender,
                m.segment,
                min_test_start_datetime_pst                                               AS activated_ab_test_start_datetime,
                max_ab_test_start_datetime                                                AS max_ab_test_start_datetime,
                max.min_campaign_code_datetime,
                max.max_campaign_code_datetime,
                last_refreshed_hq_datetime                                                AS last_refreshed_hq_datetime,
                CASE
                    WHEN na_traffic = TRUE AND eu_traffic = TRUE THEN 'NA+EU'
                    WHEN na_traffic = TRUE AND eu_traffic IS NULL THEN 'NA'
                    WHEN na_traffic IS NULL AND eu_traffic = TRUE THEN 'EU'
                    ELSE '-' END                                                          AS test_region,
                CASE
                    WHEN prospect_traffic = TRUE AND lead_traffic = TRUE AND vip_traffic = TRUE AND
                         guest_traffic = TRUE AND cancelled_traffic = TRUE AND
                         (LOWER(m.test_label) NOT ILIKE '%quiz%' OR LOWER(m.test_label) NOT ILIKE '%prospect%') AND
                         m.test_type = 'session' THEN 'ALL'
                    WHEN (LOWER(m.test_label) ILIKE '%quiz%' OR LOWER(m.test_label) ILIKE '%prospect%') AND
                         m.test_type = 'session' THEN 'Prospects'
                    WHEN (prospect_traffic = TRUE AND lead_traffic IS NULL AND vip_traffic IS NULL AND
                          guest_traffic IS NULL AND cancelled_traffic IS NULL) AND m.test_type = 'session'
                        THEN 'Prospects'
                    WHEN prospect_traffic IS NULL AND lead_traffic = TRUE AND vip_traffic IS NULL AND
                         guest_traffic IS NULL AND cancelled_traffic IS NULL THEN 'Leads'
                    WHEN prospect_traffic IS NULL AND lead_traffic IS NULL AND vip_traffic IS NULL AND
                         guest_traffic IS NULL AND cancelled_traffic = TRUE THEN 'Cancelled'
                    WHEN prospect_traffic IS NULL AND lead_traffic IS NULL AND vip_traffic IS NULL AND
                         guest_traffic = TRUE AND cancelled_traffic IS NULL THEN 'Guest'
                    WHEN prospect_traffic IS NULL AND lead_traffic IS NULL AND vip_traffic = TRUE AND
                         guest_traffic IS NULL AND cancelled_traffic IS NULL THEN 'VIPS'
                    WHEN prospect_traffic = TRUE AND lead_traffic = TRUE AND vip_traffic IS NULL AND
                         guest_traffic IS NULL AND cancelled_traffic IS NULL AND m.test_type = 'session'
                        THEN 'Prospects & Leads'
                    WHEN prospect_traffic = TRUE AND lead_traffic IS NULL AND vip_traffic IS NULL AND
                         guest_traffic = TRUE AND cancelled_traffic IS NULL AND m.test_type = 'session'
                        THEN 'Prospects & Guest'
                    WHEN prospect_traffic = TRUE AND lead_traffic IS NULL AND vip_traffic IS NULL AND
                         guest_traffic IS NULL AND cancelled_traffic = TRUE AND m.test_type = 'session'
                        THEN 'Prospects & Cancelled'
                    WHEN (prospect_traffic = TRUE AND lead_traffic = TRUE AND vip_traffic IS NULL AND
                          guest_traffic = TRUE AND cancelled_traffic = TRUE) AND
                         (LOWER(m.test_label) NOT ILIKE '%quiz%' OR LOWER(m.test_label) NOT ILIKE '%prospect%') AND
                         m.test_type = 'session' THEN 'Prospects, Leads, Cancelled, Guest'
                    WHEN (prospect_traffic = TRUE AND lead_traffic = TRUE AND vip_traffic = TRUE AND
                          guest_traffic IS NULL AND cancelled_traffic = TRUE) AND
                         (LOWER(m.test_label) NOT ILIKE '%quiz%' OR LOWER(m.test_label) NOT ILIKE '%prospect%') AND
                         m.test_type = 'session' THEN 'Prospects, Leads, VIP, Cancelled'
                    WHEN prospect_traffic IS NULL AND lead_traffic = TRUE AND vip_traffic = TRUE AND
                         guest_traffic IS NULL AND cancelled_traffic IS NULL THEN 'Leads & VIPS'
                    WHEN prospect_traffic IS NULL AND lead_traffic = TRUE AND vip_traffic IS NULL AND
                         guest_traffic IS NULL AND cancelled_traffic = TRUE THEN 'Leads & Cancelled'
                    WHEN prospect_traffic IS NULL AND lead_traffic = TRUE AND vip_traffic IS NULL AND
                         guest_traffic IS NULL AND cancelled_traffic = TRUE THEN 'Leads & Guest'
                    WHEN prospect_traffic IS NULL AND lead_traffic = TRUE AND vip_traffic IS NULL AND
                         guest_traffic = TRUE AND cancelled_traffic = TRUE THEN 'Leads, Cancelled, Guest'
                    WHEN prospect_traffic IS NULL AND lead_traffic = TRUE AND vip_traffic = TRUE AND
                         guest_traffic = TRUE AND cancelled_traffic = TRUE THEN 'Leads, VIPS, Cancelled, Guest'
                    WHEN prospect_traffic IS NULL AND lead_traffic = TRUE AND vip_traffic = TRUE AND
                         guest_traffic = TRUE AND cancelled_traffic IS NULL THEN 'Leads, VIPS, Guest'
                    WHEN prospect_traffic IS NULL AND lead_traffic = TRUE AND vip_traffic = TRUE AND
                         guest_traffic IS NULL AND cancelled_traffic = TRUE THEN 'Leads, VIPS, Cancelled'
                    WHEN prospect_traffic IS NULL AND lead_traffic IS NULL AND vip_traffic = TRUE AND
                         guest_traffic IS NULL AND cancelled_traffic = TRUE THEN 'VIPS & Cancelled'
                    WHEN prospect_traffic IS NULL AND lead_traffic IS NULL AND vip_traffic = TRUE AND
                         guest_traffic = TRUE AND cancelled_traffic IS NULL THEN 'VIPS & Guest'
                    WHEN prospect_traffic IS NULL AND lead_traffic IS NULL AND vip_traffic = TRUE AND
                         guest_traffic = TRUE AND cancelled_traffic = TRUE THEN 'VIPS, Cancelled, Guest'
                    ELSE '-' END                                                          AS member_segments,
                IFF(LOWER(test_framework_ticket) ILIKE 'http%', SPLIT_PART(test_framework_ticket, '/', 5),
                    test_framework_ticket)                                                AS test_framework_ticket,
                IFF(abt.test_key IS NOT NULL AND abt.test_label IS NOT NULL, TRUE,
                    FALSE)                                                                AS is_currently_avail_in_dashboard,
                (SELECT MAX(CONVERT_TIMEZONE('America/Los_Angeles', ab_test_start_local_datetime)::datetime)
                 FROM reporting_base_prod.shared.session_ab_test_ds_image_test)           AS last_refreshed_datetime_hq,
                pt.product_effective_start_date_pst,
                pt.product_effective_end_date_pst,
                pt.evaluation_batch_id
FROM shared.ab_test_ds_image_testing AS m
         LEFT JOIN _testing_calendar__max_datetime_campaign_code AS max
             ON max.test_framework_id = m.test_framework_id
                AND max.test_key = m.test_key
                AND max.test_label = m.test_label
                AND max.master_product_id = m.product_id
                AND max.master_test_number = m.master_test_number
         LEFT JOIN _testing_calendar__meta_data md
             ON m.test_key = md.test_key
                AND m.test_framework_id = md.test_framework_id
                AND m.test_type = md.test_type
                AND m.campaign_code = md.campaign_code
                AND m.master_test_number = md.master_test_number
         LEFT JOIN _testing_calendar__avail_in_abt_dash AS abt
             ON abt.test_key = m.test_key
                AND abt.test_label = m.test_label
                AND abt.master_test_number = m.master_test_number
                AND abt.product_id = m.product_id
         LEFT JOIN _testing_calendar__product_test_times pt
             ON m.master_test_number = pt.master_test_number
                AND m.product_id = pt.meta_original_master_product_id
         LEFT JOIN (SELECT DISTINCT test_key, master_test_number, master_product_id,
                                    TRUE AS prospect_traffic
                    FROM reporting_base_prod.shared.session_ab_test_ds_image_test
                    WHERE test_start_membership_state = 'Prospect') AS p
                   ON p.test_key = m.test_key
                          AND p.master_test_number = m.master_test_number
                          AND p.master_product_id = m.product_id
         LEFT JOIN (SELECT DISTINCT test_key, master_test_number, master_product_id,
                                    TRUE AS lead_traffic
                    FROM reporting_base_prod.shared.session_ab_test_ds_image_test
                    WHERE test_start_membership_state = 'Lead') AS l
                   ON l.test_key = m.test_key
                          AND l.master_test_number = m.master_test_number
                          AND l.master_product_id = m.product_id
         LEFT JOIN (SELECT DISTINCT test_key,
                                    master_test_number,
                                    master_product_id,
                                    TRUE AS vip_traffic
                    FROM reporting_base_prod.shared.session_ab_test_ds_image_test
                    WHERE test_start_membership_state = 'VIP') AS v
                   ON v.test_key = m.test_key
                          AND v.master_test_number = m.master_test_number
                          AND v.master_product_id = m.product_id
         LEFT JOIN (SELECT DISTINCT test_key,
                                    master_test_number,
                                    master_product_id,
                                    TRUE AS cancelled_traffic
                    FROM reporting_base_prod.shared.session_ab_test_ds_image_test
                    WHERE test_start_membership_state = 'Cancelled') AS c
                   ON c.test_key = m.test_key
                          AND c.master_test_number = m.master_test_number
                          AND c.master_product_id = m.product_id
         LEFT JOIN (SELECT DISTINCT test_key,
                                    master_test_number,
                                    master_product_id,
                                    TRUE AS guest_traffic
                    FROM reporting_base_prod.shared.session_ab_test_ds_image_test
                    WHERE test_start_membership_state = 'Guest') AS g
                   ON g.test_key = m.test_key
                          AND g.master_test_number = m.master_test_number
                          AND g.master_product_id = m.product_id
         LEFT JOIN (SELECT DISTINCT test_key,
                                    master_test_number,
                                    master_product_id,
                                    TRUE AS na_traffic
                    FROM reporting_base_prod.shared.session_ab_test_ds_image_test
                    WHERE store_region = 'NA') AS na
                   ON na.test_key = m.test_key
                          AND na.master_test_number = m.master_test_number
                          AND na.master_product_id = m.product_id
         LEFT JOIN (SELECT DISTINCT test_key,
                                    master_test_number,
                                    master_product_id,
                                    TRUE AS eu_traffic
                    FROM reporting_base_prod.shared.session_ab_test_ds_image_test
                    WHERE store_region = 'EU') AS eu
                   ON eu.test_key = m.test_key
                          AND eu.master_test_number = m.master_test_number
                          AND eu.master_product_id = m.product_id
GROUP BY ALL;

CREATE OR REPLACE TEMP TABLE _testing_calendar__max_evaluation_batches AS
SELECT master_test_number,
       MAX(evaluation_batch_id) AS max_evaluation_batch_id
FROM reporting_base_prod.shared.product_image_test_config_export
GROUP BY master_test_number
ORDER BY master_test_number;

CREATE OR REPLACE TEMP TABLE _testing_calendar__eval AS
SELECT ce.*
FROM _testing_calendar__max_evaluation_batches me
         LEFT JOIN reporting_base_prod.shared.product_image_test_config_export ce
     ON ce.evaluation_batch_id = me.max_evaluation_batch_id
         AND ce.master_test_number = me.master_test_number
ORDER BY ce.master_test_number;

INSERT OVERWRITE INTO shared.ab_test_metadata_ds_image_testing
SELECT DISTINCT dd.full_date,
                IFF(max_test_date_rnk = 1, test_key_version, '') AS test_key_version_rnk_label,
                t.*
FROM edw_prod.data_model.dim_date AS dd
         FULL JOIN
     (SELECT m.store_brand,
             m.test_label,
             m.campaign_code,
             m.test_key,
             m.test_key_version,
             m.test_framework_id,
             m.test_framework_description,
             m.test_type,
             m.master_test_number,
             m.master_product_id,
             m.activated_ab_test_start_datetime,
             m.max_ab_test_start_datetime,
             m.min_campaign_code_datetime,
             m.max_campaign_code_datetime,
             m.last_refreshed_hq_datetime,
             m.test_region,
             m.member_segments,
             m.test_framework_ticket,
             m.is_currently_avail_in_dashboard,
             m.last_refreshed_datetime_hq,
             ab_test_start_local_date                                                                                                                                      AS test_date,
             s.test_key_rnk,
             s.product_sku,
             s.image_sort_url_variant,
             s.image_sort_url_control,
             m.segment,
             m.gender,
             m.sub_brand,
             m.platform,
             m.color,
             m.product_name,
             c.cms_min_test_start_datetime_hq,
             c.cms_end_date,
             e.evaluation_message,
             e.datetime_added                                                                                                                                              AS product_end_datetime,
             CASE
                 WHEN is_test_key_rnk = TRUE
                     THEN RANK() OVER (PARTITION BY m.test_key,m.test_label,m.store_brand,m.test_framework_id,s.is_test_key_rnk ORDER BY ab_test_start_local_date ASC) END AS max_test_date_rnk,
             m.product_effective_start_date_pst,
             m.product_effective_end_date_pst,
             m.evaluation_batch_id

      FROM _testing_calendar__meta AS m
               JOIN (SELECT DISTINCT m.test_key,
                                     m.ab_test_start_local_date,
                                     RANK() OVER (PARTITION BY m.test_key,m.test_label,store_brand,test_framework_id,m.master_test_number, m.product_id, test_activated_datetime ORDER BY test_activated_datetime DESC) AS test_key_rnk,
                                     IFF(
                                             RANK() OVER (PARTITION BY m.test_key,m.test_label,store_brand,test_framework_id,test_activated_datetime ORDER BY test_activated_datetime DESC) BETWEEN 1 AND 2,
                                             TRUE,
                                             FALSE)                                                                                                                                                                     AS is_test_key_rnk,
                                     m.master_test_number,
                                     master_product_id,
                                     product_sku,
                                     image_sort_url_control,
                                     image_sort_url_variant
                     FROM shared.ab_test_ds_image_testing AS m
                              JOIN (SELECT DISTINCT test_key,
                                                    master_product_id,
                                                    master_test_number,
                                                    CONVERT_TIMEZONE('America/Los_Angeles', ab_test_start_local_datetime)::date AS ab_test_start_local_date
                                    FROM reporting_base_prod.shared.session_ab_test_ds_image_test) AS s
                                   ON m.test_key = s.test_key
                                          AND m.master_test_number = s.master_test_number
                                          AND m.product_id = s.master_product_id
                     ORDER BY 1, 2) AS s ON s.test_key = m.test_key
                                                AND s.master_test_number = m.master_test_number
                                                AND s.master_product_id = m.master_product_id
               LEFT JOIN shared.ab_test_cms_metadata AS c
                         ON m.test_key = c.test_key
                                AND m.store_brand = c.store_brand
                                AND c.statuscode = 113
                                AND c.campaign_code = 'FLimagesort'
               LEFT JOIN _testing_calendar__eval e
                         ON m.master_test_number = e.master_test_number
                             AND m.master_product_id = e.master_product_id
      GROUP BY m.store_brand,
               m.test_label,
               m.campaign_code,
               m.test_key,
               m.test_key_version,
               m.test_framework_id,
               m.test_framework_description,
               m.test_type,
               m.master_test_number,
               m.master_product_id,
               m.activated_ab_test_start_datetime,
               m.max_ab_test_start_datetime,
               m.min_campaign_code_datetime,
               m.max_campaign_code_datetime,
               m.last_refreshed_hq_datetime,
               m.test_region,
               m.member_segments,
               m.test_framework_ticket,
               m.is_currently_avail_in_dashboard,
               m.last_refreshed_datetime_hq,
               ab_test_start_local_date,
               s.test_key_rnk,
               s.product_sku,
               s.image_sort_url_variant,
               s.image_sort_url_control,
               is_test_key_rnk,
               m.segment,
               m.gender,
               m.sub_brand,
               m.platform,
               m.color,
               m.product_name,
               c.cms_min_test_start_datetime_hq,
               c.cms_end_date,
               e.evaluation_message,
               e.datetime_added,
               m.product_effective_start_date_pst,
               m.product_effective_end_date_pst,
               m.evaluation_batch_id)
         AS t ON t.test_date::date = dd.full_date
WHERE dd.month_date BETWEEN CURRENT_DATE - INTERVAL '3 month' AND CURRENT_DATE::date;
