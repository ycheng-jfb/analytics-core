SET target_table = 'reporting_prod.shared.sessions_by_platform_stg';
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
                 FROM shared.sessions_by_platform_stg
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

CREATE OR REPLACE TEMP TABLE _sessions_by_platform_stg AS
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
       d.is_test_customer_account,
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
                       AND c.company_id = ds.company_id;

MERGE INTO shared.sessions_by_platform_stg AS t
USING _sessions_by_platform_stg AS s
ON t.session_id = s.session_id
WHEN MATCHED AND (NOT EQUAL_NULL(t.session_local_datetime, s.session_local_datetime)
    OR NOT EQUAL_NULL(t.session_date, s.session_date)
    OR NOT EQUAL_NULL(t.session_month_date, s.session_month_date)
    OR NOT EQUAL_NULL(t.session_store_id, s.session_store_id)
    OR NOT EQUAL_NULL(t.store_type, s.store_type)
    OR NOT EQUAL_NULL(t.store_brand_name, s.store_brand_name)
    OR NOT EQUAL_NULL(t.store_brand_abbr, s.store_brand_abbr)
    OR NOT EQUAL_NULL(t.store_region_abbr, s.store_region_abbr)
    OR NOT EQUAL_NULL(t.store_country_abbr, s.store_country_abbr)
    OR NOT EQUAL_NULL(t.store_name, s.store_name)
    OR NOT EQUAL_NULL(t.store_group, s.store_group)
    OR NOT EQUAL_NULL(t.is_mobile_app_store, s.is_mobile_app_store)
    OR NOT EQUAL_NULL(t.customer_id, s.customer_id)
    OR NOT EQUAL_NULL(t.visitor_id, s.visitor_id)
    OR NOT EQUAL_NULL(t.session_id, s.session_id)
    OR NOT EQUAL_NULL(t.session_platform, s.session_platform)
    OR NOT EQUAL_NULL(t.raw_device, s.raw_device)
    OR NOT EQUAL_NULL(t.operating_system, s.operating_system)
    OR NOT EQUAL_NULL(t.browser, s.browser)
    OR NOT EQUAL_NULL(t.membership_state, s.membership_state)
    OR NOT EQUAL_NULL(t.customer_gender, s.customer_gender)
    OR NOT EQUAL_NULL(t.is_atb_action, s.is_atb_action)
    OR NOT EQUAL_NULL(t.monthly_vip_tenure, s.monthly_vip_tenure)
    OR NOT EQUAL_NULL(t.daily_lead_tenure, s.daily_lead_tenure)
    OR NOT EQUAL_NULL(t.is_lead_registration_action, s.is_lead_registration_action)
    OR NOT EQUAL_NULL(t.registration_local_datetime, s.registration_local_datetime)
    OR NOT EQUAL_NULL(t.is_skip_month_action, s.is_skip_month_action)
    OR NOT EQUAL_NULL(t.channel, s.channel)
    OR NOT EQUAL_NULL(t.subchannel, s.subchannel)
    OR NOT EQUAL_NULL(t.channel_adj, s.channel_adj)
    OR NOT EQUAL_NULL(t.subchannel_adj, s.subchannel_adj)
    OR NOT EQUAL_NULL(t.is_bot, s.is_bot)
    OR NOT EQUAL_NULL(t.is_test_customer_account, s.is_test_customer_account)
    OR NOT EQUAL_NULL(t.is_scrubs_gateway, s.is_scrubs_gateway)
    OR NOT EQUAL_NULL(t.is_scrubs_customer, s.is_scrubs_customer)
    OR NOT EQUAL_NULL(t.is_scrubs_action, s.is_scrubs_action)
    OR NOT EQUAL_NULL(t.is_scrubs_session, s.is_scrubs_session)
    OR NOT EQUAL_NULL(t.is_in_segment, s.is_in_segment)
    OR NOT EQUAL_NULL(t.referrer, s.referrer)
    OR NOT EQUAL_NULL(t.is_paid_media, s.is_paid_media)
    OR NOT EQUAL_NULL(t.user_segment, s.user_segment)) THEN
UPDATE SET
     t.session_local_datetime = s.session_local_datetime,
     t.session_date = s.session_date,
     t.session_month_date = s.session_month_date,
     t.session_store_id = s.session_store_id,
     t.store_type = s.store_type,
     t.store_brand_name = s.store_brand_name,
     t.store_brand_abbr = s.store_brand_abbr,
     t.store_region_abbr = s.store_region_abbr,
     t.store_country_abbr = s.store_country_abbr,
     t.store_name = s.store_name,
     t.store_group = s.store_group,
     t.is_mobile_app_store = s.is_mobile_app_store,
     t.customer_id = s.customer_id,
     t.visitor_id = s.visitor_id,
     t.session_id = s.session_id,
     t.session_platform = s.session_platform,
     t.raw_device = s.raw_device,
     t.operating_system = s.operating_system,
     t.browser = s.browser,
     t.membership_state = s.membership_state,
     t.customer_gender = s.customer_gender,
     t.is_atb_action = s.is_atb_action,
     t.monthly_vip_tenure = s.monthly_vip_tenure,
     t.daily_lead_tenure = s.daily_lead_tenure,
     t.is_lead_registration_action = s.is_lead_registration_action,
     t.registration_local_datetime = s.registration_local_datetime,
     t.is_skip_month_action = s.is_skip_month_action,
     t.channel = s.channel,
     t.subchannel = s.subchannel,
     t.channel_adj = s.channel_adj,
     t.subchannel_adj = s.subchannel_adj,
     t.is_bot = s.is_bot,
     t.is_test_customer_account = s.is_test_customer_account,
     t.is_scrubs_gateway = s.is_scrubs_gateway,
     t.is_scrubs_customer = s.is_scrubs_customer,
     t.is_scrubs_action = s.is_scrubs_action,
     t.is_scrubs_session = s.is_scrubs_session,
     t.is_in_segment = s.is_in_segment,
     t.referrer = s.referrer,
     t.is_paid_media = s.is_paid_media,
     t.meta_update_datetime = s.meta_update_datetime,
     t.user_segment = s.user_segment
WHEN NOT MATCHED THEN
    INSERT(session_local_datetime,session_date,session_month_date,session_store_id,store_type,store_brand_name,store_brand_abbr,
           store_region_abbr,store_country_abbr,store_name,store_group,is_mobile_app_store,customer_id,visitor_id,session_id,
           session_platform,raw_device,operating_system,browser,membership_state,customer_gender,is_atb_action,monthly_vip_tenure,
           daily_lead_tenure,is_lead_registration_action,registration_local_datetime,is_skip_month_action,channel,subchannel,channel_adj,
           subchannel_adj,is_bot,is_test_customer_account,is_scrubs_gateway,is_scrubs_customer,is_scrubs_action,is_scrubs_session,is_in_segment,referrer,is_paid_media,user_segment,meta_create_datetime,meta_update_datetime)


    VALUES(s.session_local_datetime, s.session_date, s.session_month_date, s.session_store_id, s.store_type, s.store_brand_name, s.store_brand_abbr,
           s.store_region_abbr, s.store_country_abbr, s.store_name, s.store_group, s.is_mobile_app_store, s.customer_id, s.visitor_id, s.session_id,
           s.session_platform, s.raw_device, s.operating_system, s.browser, s.membership_state, s.customer_gender, s.is_atb_action,
           s.monthly_vip_tenure, s.daily_lead_tenure, s.is_lead_registration_action, s.registration_local_datetime, s.is_skip_month_action,
           s.channel, s.subchannel, s.channel_adj, s.subchannel_adj, s.is_bot, s.is_test_customer_account, s.is_scrubs_gateway, s.is_scrubs_customer, s.is_scrubs_action,
           s.is_scrubs_session, s.is_in_segment, s.referrer, s.is_paid_media,s.user_segment,s.meta_create_datetime,s.meta_update_datetime);


UPDATE public.meta_table_dependency_watermark
SET high_watermark_datetime = new_high_watermark_datetime,
    meta_update_datetime    = CURRENT_TIMESTAMP::timestamp_ltz(3)
WHERE table_name = $target_table;
