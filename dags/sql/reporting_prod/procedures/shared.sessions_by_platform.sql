SET target_table = 'reporting_prod.shared.sessions_by_platform';
SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
SET is_full_refresh = (SELECT IFF(public.udf_get_watermark($target_table, NULL) = '1900-01-01'::TIMESTAMP_LTZ(3), TRUE,
                                  FALSE));

MERGE INTO public.meta_table_dependency_watermark AS t
    USING (SELECT $target_table                               AS table_name,
                  NULLIF(dependent_table_name, $target_table) AS dependent_table_name,
                  new_high_watermark_datetime                 AS new_high_watermark_datetime
           FROM (SELECT -- For self table
                        NULL                      AS dependent_table_name,
                        MAX(meta_create_datetime) AS new_high_watermark_datetime
                 FROM shared.sessions_by_platform
                 UNION ALL
                 SELECT 'reporting_base_prod.shared.session' AS dependent_table_name,
                        MAX(meta_update_datetime)            AS new_high_watermark_datetime
                 FROM reporting_base_prod.shared.session
                 UNION ALL
                 SELECT 'lake_view.sharepoint.media_source_channel_mapping_adj' AS dependent_table_name,
                        MAX(meta_update_datetime)                               AS new_high_watermark_datetime
                 FROM lake_view.sharepoint.media_source_channel_mapping_adj
                 UNION ALL
                 SELECT 'reporting_base_prod.shared.media_source_channel_mapping' AS dependent_table_name,
                        MAX(meta_update_datetime)                                 AS new_high_watermark_datetime
                 FROM reporting_base_prod.shared.media_source_channel_mapping
                 UNION ALL
                 SELECT 'edw_prod.data_model.fact_registration' AS dependent_table_name,
                        MAX(meta_update_datetime)               AS new_high_watermark_datetime
                 FROM edw_prod.data_model.fact_registration) AS h) AS s
    ON t.table_name = s.table_name
        AND EQUAL_NULL(t.dependent_table_name, s.dependent_table_name)
    WHEN MATCHED
        AND NOT EQUAL_NULL(t.new_high_watermark_datetime, s.new_high_watermark_datetime)
        THEN UPDATE SET
        t.new_high_watermark_datetime = s.new_high_watermark_datetime,
        t.meta_update_datetime = CURRENT_TIMESTAMP::timestamp_ltz(3)
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

SET wm_reporting_base_prod_shared_session = public.udf_get_watermark($target_table,
                                                                     'reporting_base_prod.shared.session');
SET wm_lake_view_sharepoint_media_source_channel_mapping_adj = public.udf_get_watermark($target_table,
                                                                     'lake_view.sharepoint.media_source_channel_mapping_adj');
SET wm_edw_prod_data_model_fact_registration = public.udf_get_watermark($target_table,
                                                                        'edw_prod.data_model.fact_registration');
SET wm_reporting_base_prod_shared_media_source_channel_mapping = public.udf_get_watermark($target_table,
                                                                        'reporting_base_prod.shared.media_source_channel_mapping');

CREATE OR REPLACE TEMP TABLE _sessions_by_platform__base
(
    session_id NUMBER
);

-- full refresh
INSERT INTO _sessions_by_platform__base
SELECT session_id
FROM reporting_base_prod.shared.session
WHERE $is_full_refresh = TRUE;

-- incremental refresh
INSERT INTO _sessions_by_platform__base
SELECT DISTINCT session_id
FROM (SELECT s.session_id
      FROM reporting_base_prod.shared.session s
               LEFT JOIN edw_prod.stg.dim_store ds ON s.store_id = ds.store_id
               LEFT JOIN lake_view.sharepoint.media_source_channel_mapping_adj cm
                         ON COALESCE(cm.utm_source, 'None') =
                            IFF(COALESCE(cm.utm_source, 'None') <> 'None', s.utm_source, 'None')
                             AND COALESCE(cm.utm_medium, 'None') =
                                 IFF(COALESCE(cm.utm_medium, 'None') <> 'None', s.utm_medium, 'None')
                             AND COALESCE(cm.referrer, 'None') =
                                 IFF(COALESCE(cm.referrer, 'None') <> 'None', s.referer, 'None')
                             AND COALESCE(cm.utm_campaign, 'None') =
                                 IFF(COALESCE(cm.utm_campaign, 'None') <> 'None', s.utm_campaign, 'None')
                             AND CASE
                                     WHEN cm.brand = 'SXF' THEN 30
                                     WHEN cm.brand = 'FL' THEN 20
                                     WHEN cm.brand = 'JFB' THEN 10
                                     END = ds.company_id
               LEFT JOIN reporting_base_prod.shared.media_source_channel_mapping AS map
                         ON map.media_source_hash = s.media_source_hash
      WHERE s.meta_update_datetime > $wm_reporting_base_prod_shared_session
         OR cm.meta_update_datetime > $wm_lake_view_sharepoint_media_source_channel_mapping_adj
         OR map.meta_update_datetime > $wm_reporting_base_prod_shared_media_source_channel_mapping
      UNION ALL
      SELECT session_id
      FROM edw_prod.stg.fact_registration
      WHERE meta_update_datetime > $wm_edw_prod_data_model_fact_registration
        AND session_id IS NOT NULL) AS incr
WHERE $is_full_refresh = FALSE;

CREATE OR REPLACE TEMP TABLE _sessions_by_platform__media_channel_mapping_correction AS
SELECT DISTINCT COALESCE(utm_source, 'None') AS utm_source,
                COALESCE(utm_medium, 'None') AS utm_medium,
                COALESCE(referrer, 'None') as referrer,
                COALESCE(utm_campaign, 'None') as utm_campaign,
                channel_update,
                subchannel_update,
                brand,
                CASE
                    WHEN brand = 'SXF' THEN 30
                    WHEN brand = 'FL' THEN 20
                    WHEN brand = 'JFB' THEN 10
                    END                      AS company_id
FROM lake_view.sharepoint.media_source_channel_mapping_adj;


CREATE OR REPLACE TEMP TABLE _session_utm AS
SELECT s.session_id,
       IFF(IFNULL(utm_source, '') NOT IN
           (SELECT DISTINCT utm_source
            FROM _sessions_by_platform__media_channel_mapping_correction),
           'None', utm_source)   AS utm_source_n,
       IFF(IFNULL(utm_medium, '') NOT IN
           (SELECT DISTINCT utm_medium
            FROM _sessions_by_platform__media_channel_mapping_correction),
           'None', utm_medium)   AS utm_medium_n,
       IFF(IFNULL(utm_campaign, '') NOT IN
           (SELECT DISTINCT utm_campaign
            FROM _sessions_by_platform__media_channel_mapping_correction),
           'None', utm_campaign) AS utm_campaign_n,
       IFF(IFNULL(referer, '') NOT IN
           (SELECT DISTINCT IFF(referrer = 'placeholder', 'None', referrer)
            FROM _sessions_by_platform__media_channel_mapping_correction),
           'None', referer)      AS referrer_n,
       referer,
       store_id
FROM reporting_base_prod.shared.session s
         JOIN _sessions_by_platform__base sbpb ON s.session_id = sbpb.session_id;

DELETE
FROM shared.sessions_by_platform t
    USING _sessions_by_platform__base s
WHERE t.session_id = s.session_id;

INSERT INTO shared.sessions_by_platform (session_local_datetime,
                                         session_date,
                                         session_month_date,
                                         session_store_id,
                                         store_type,
                                         store_brand_name,
                                         store_brand_abbr,
                                         store_region_abbr,
                                         store_country_abbr,
                                         store_name,
                                         store_group,
                                         is_mobile_app_store,
                                         customer_id,
                                         visitor_id,
                                         session_id,
                                         session_platform,
                                         raw_device,
                                         operating_system,
                                         browser,
                                         membership_state,
                                         customer_gender,
                                         is_atb_action,
                                         monthly_vip_tenure,
                                         daily_lead_tenure,
                                         is_lead_registration_action,
                                         registration_local_datetime,
                                         is_skip_month_action,
                                         channel,
                                         subchannel,
                                         channel_adj,
                                         subchannel_adj,
                                         is_bot,
                                         is_scrubs_gateway,
                                         is_scrubs_customer,
                                         is_scrubs_action,
                                         is_scrubs_session,
                                         is_in_segment,
                                         referrer,
                                         is_paid_media,
                                         user_segment,
                                         meta_create_datetime,
                                         meta_update_datetime)
SELECT d.session_local_datetime,
       CAST(d.session_local_datetime AS DATE)                      AS session_date,
       CAST(DATE_TRUNC('month', d.session_local_datetime) AS DATE) AS session_month_date,
       ds.store_id                                                 AS session_store_id,
       ds.store_type,
       ds.store_brand                                              AS store_brand_name,
       ds.store_brand_abbr,
       ds.store_region                                             AS store_region_abbr,
       ds.store_country                                            AS store_country_abbr,
       ds.store_name,
       ds.store_group,
       IFF(ds.store_brand IN ('JustFab', 'Fabletics')
               AND ds.store_country IN ('DE', 'ES', 'FR', 'UK', 'US'), TRUE,
           FALSE)                                                  AS is_mobile_app_store,
       d.customer_id,
       d.visitor_id,
       d.session_id,
       CASE
           WHEN d.platform = 'Tablet' THEN 'Desktop'
           WHEN d.platform = 'Unknown' THEN 'Mobile'
           ELSE d.platform END                                     AS session_platform,
       d.platform                                                  AS raw_device,
       COALESCE(IFF(d.os <> 'Unknown' AND d.os IS NOT NULL, d.os, ma.os),
                'Unknown')                                         AS operating_system,
       d.browser,
       d.membership_state,
       CASE
           WHEN d.is_male_customer = TRUE THEN 'Male'
           WHEN d.membership_state = 'Prospect' AND d.is_male_session = TRUE THEN 'Male'
           ELSE 'Female' END                                       AS customer_gender,
       d.is_atb_action,
       d.monthly_vip_tenure,
       d.daily_lead_tenure,
       d.is_lead_registration_action,
       reg.registration_local_datetime,
       d.is_skip_month_action,
       map.channel,
       map.subchannel,
       COALESCE(c.channel_update, map.channel)                     AS channel_adj,
       COALESCE(c.subchannel_update, map.subchannel)               AS subchannel_adj,
       d.is_bot,
       d.is_scrubs_gateway,
       d.is_scrubs_customer,
       d.is_scrubs_action,
       d.is_scrubs_session,
       NVL(d.is_in_segment, TRUE)                                  AS is_in_segment,
       c.referrer,
       IFF(map.channel_type = 'Paid', TRUE, FALSE)                 AS is_paid_media,
       d.user_segment,
       $execution_start_time                                       AS meta_create_datetime,
       $execution_start_time                                       AS meta_update_datetime
FROM _sessions_by_platform__base AS base
         JOIN reporting_base_prod.shared.session d
              ON d.session_id = base.session_id
         JOIN _session_utm sum
              ON sum.session_id = base.session_id
         JOIN edw_prod.stg.dim_store AS ds ON sum.store_id = ds.store_id
         LEFT JOIN edw_prod.data_model.fact_registration AS reg
                   ON reg.session_id = d.session_id
                       AND reg.customer_id = d.customer_id
         LEFT JOIN reporting_base_prod.shared.media_source_channel_mapping AS map
                   ON map.media_source_hash = d.media_source_hash
         LEFT JOIN reporting_base_prod.shared.mobile_app_session_os_updated ma
                   ON ma.session_id = d.session_id
         LEFT JOIN _sessions_by_platform__media_channel_mapping_correction c
                   ON c.utm_source = sum.utm_source_n
                       AND c.utm_medium = sum.utm_medium_n
                       AND c.referrer = sum.referrer_n
                       AND c.utm_campaign = sum.utm_campaign_n
                       AND c.company_id = ds.company_id
WHERE ds.store_type <> 'Retail'
  AND d.is_test_customer_account = FALSE
  AND NVL(is_in_segment, TRUE) = TRUE
  AND d.is_bot = FALSE;

UPDATE public.meta_table_dependency_watermark
SET high_watermark_datetime = new_high_watermark_datetime,
    meta_update_datetime    = CURRENT_TIMESTAMP::timestamp_ltz(3)
WHERE table_name = $target_table;
