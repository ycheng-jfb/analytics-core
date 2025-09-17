ALTER SESSION SET TIMEZONE='America/Los_Angeles';

SET target_table = 'reporting_base_prod.shared.session';
SET execution_start_time = current_timestamp()::TIMESTAMP_LTZ(3);
SET is_full_refresh = (SELECT IFF(public.udf_get_watermark($target_table, NULL) = '1900-01-01'::TIMESTAMP_LTZ(3), TRUE, FALSE));
SET is_exception_records = (SELECT count(*) > 1 FROM staging.session_excp);
SET min_refresh_datetime = (SELECT CASE
                                       WHEN $is_full_refresh THEN '2022-01-01'
                                       WHEN $is_exception_records THEN (SELECT MIN(datetime_added) AS min_datetime
                                                                        FROM staging.session_excp AS exc
                                                                                 JOIN lake_consolidated.ultra_merchant.session s
                                                                                      ON exc.session_id = s.session_id)
                                       ELSE DATEADD(MONTH, -4, CURRENT_DATE()) END);

-- Switch warehouse if performing a full refresh
SET warehouse_to_be_used = IFF($is_full_refresh, 'da_wh_adhoc_large', current_warehouse());
USE WAREHOUSE IDENTIFIER ($warehouse_to_be_used);

MERGE INTO public.meta_table_dependency_watermark AS t
    USING (
        SELECT
            $target_table AS table_name,
            NULLIF(dependent_table_name,$target_table) AS dependent_table_name,
            high_watermark_datetime AS new_high_watermark_datetime
            FROM (
            SELECT
                $target_table as dependent_table_name,
                $execution_start_time as high_watermark_datetime
            UNION ALL
            SELECT
                'edw_prod.data_model.fact_order' AS dependent_table_name,
                MAX(meta_update_datetime) AS high_watermark_datetime
            FROM edw_prod.data_model.fact_order
            UNION ALL
            SELECT
                'reporting_base_prod.staging.session_uri' AS dependent_table_name,
                MAX(meta_update_datetime) AS high_watermark_datetime
            FROM staging.session_uri
            UNION ALL
            SELECT
                'reporting_base_prod.staging.membership_state' AS dependent_table_name,
                MAX(meta_update_datetime) AS high_watermark_datetime
            FROM staging.membership_state
            UNION ALL
            SELECT
                'reporting_base_prod.staging.site_visit_metrics' AS dependent_table_name,
                MAX(meta_update_datetime) AS high_watermark_datetime
            FROM staging.site_visit_metrics
            UNION ALL
            SELECT
                'reporting_base_prod.staging.visitor_session' AS dependent_table_name,
                MAX(meta_update_datetime) AS high_watermark_datetime
            FROM staging.visitor_session
            UNION ALL
            SELECT
                'reporting_base_prod.staging.migrated_session' AS dependent_table_name,
                MAX(meta_update_datetime) AS high_watermark_datetime
            FROM staging.migrated_session
            UNION ALL
            SELECT
                'reporting_base_prod.shared.session_ab_test_start' AS dependent_table_name,
                MAX(meta_update_datetime) AS high_watermark_datetime
            FROM shared.session_ab_test_start
            UNION ALL
            SELECT
                'reporting_base_prod.staging.customer_session' AS dependent_table_name,
                MAX(meta_update_datetime) AS high_watermark_datetime
            FROM staging.customer_session
            UNION ALL
            SELECT
                'lake_consolidated_view.ultra_merchant.session_detail' AS dependent_table_name,
                MAX(meta_update_datetime) AS high_watermark_datetime
            FROM lake_consolidated_view.ultra_merchant.session_detail
            UNION ALL
            SELECT
                'lake_consolidated_view.ultra_merchant.session' AS dependent_table_name,
                MAX(meta_update_datetime) AS high_watermark_datetime
            FROM lake_consolidated_view.ultra_merchant.session
            UNION ALL
            SELECT
                'lake_consolidated_view.ultra_merchant.session_log' AS dependent_table_name,
                MAX(meta_update_datetime) AS high_watermark_datetime
            FROM lake_consolidated_view.ultra_merchant.session_log
            UNION ALL
            SELECT
                'lake_consolidated_view.ultra_merchant.membership_skip' AS dependent_table_name,
                MAX(meta_update_datetime) AS high_watermark_datetime
            FROM lake_consolidated_view.ultra_merchant.membership_skip
            UNION ALL
            SELECT
                'reporting_base_prod.staging.session_segment_events' AS dependent_table_name,
                MAX(meta_create_datetime) AS high_watermark_datetime
            FROM staging.session_segment_events
            ) AS h
        ) AS s
    ON t.table_name = s.table_name
        AND EQUAL_NULL(t.dependent_table_name, s.dependent_table_name)
    WHEN MATCHED
        AND NOT EQUAL_NULL(t.new_high_watermark_datetime, s.new_high_watermark_datetime)
        THEN UPDATE SET
            t.new_high_watermark_datetime = s.new_high_watermark_datetime,
            t.meta_update_datetime = $execution_start_time
    WHEN NOT MATCHED
    THEN INSERT (
        table_name,
        dependent_table_name,
        high_watermark_datetime,
        new_high_watermark_datetime
        )
    VALUES (
        s.table_name,
        s.dependent_table_name,
        '1900-01-01'::timestamp_ltz,
        s.new_high_watermark_datetime
    );


SET wm_edw_data_model_fact_order = public.udf_get_watermark($target_table,'edw_prod.data_model.fact_order');
SET wm_reporting_base_prod_staging_session_uri = public.udf_get_watermark($target_table,'reporting_base_prod.staging.session_uri');
SET wm_reporting_base_prod_staging_session_ga = public.udf_get_watermark($target_table,'reporting_base_prod.staging.session_ga');
SET wm_reporting_base_prod_staging_membership_state = public.udf_get_watermark($target_table,'reporting_base_prod.staging.membership_state');
SET wm_reporting_base_prod_staging_site_visit_metrics = public.udf_get_watermark($target_table,'reporting_base_prod.staging.site_visit_metrics');
SET wm_reporting_base_prod_staging_visitor_session = public.udf_get_watermark($target_table,'reporting_base_prod.staging.visitor_session');
SET wm_reporting_base_prod_staging_migrated_session = public.udf_get_watermark($target_table,'reporting_base_prod.staging.migrated_session');
SET wm_reporting_base_prod_shared_session_ab_test_start = public.udf_get_watermark($target_table,'reporting_base_prod.shared.session_ab_test_start');
SET wm_reporting_base_prod_staging_customer_session = public.udf_get_watermark($target_table,'reporting_base_prod.staging.customer_session');
SET wm_lake_consolidated_view_ultra_merchant_session_detail = public.udf_get_watermark($target_table,'lake_consolidated_view.ultra_merchant.session_detail');
SET wm_lake_consolidated_view_ultra_merchant_session = public.udf_get_watermark($target_table,'lake_consolidated_view.ultra_merchant.session');
SET wm_lake_consolidated_view_ultra_merchant_session_log = public.udf_get_watermark($target_table,'lake_consolidated_view.ultra_merchant.session_log');
SET wm_lake_consolidated_view_ultra_merchant_membership_skip = public.udf_get_watermark($target_table,'lake_consolidated_view.ultra_merchant.membership_skip');
SET wm_reporting_base_prod_staging_session_segment_events = public.udf_get_watermark($target_table,'reporting_base_prod.staging.session_segment_events');


CREATE OR REPLACE TEMP TABLE _session_base (session_id NUMERIC);

INSERT INTO _session_base
SELECT DISTINCT session_id
FROM lake_consolidated.ultra_merchant.session
WHERE $is_full_refresh
    AND datetime_added >= $min_refresh_datetime;

INSERT INTO _session_base
SELECT DISTINCT s.session_id
FROM (
    SELECT session_id
    FROM staging.session_excp
    UNION ALL
    SELECT session_id
    FROM edw_prod.data_model.fact_order
    WHERE meta_update_datetime > $wm_edw_data_model_fact_order
    UNION ALL
    SELECT session_id
    FROM staging.session_uri
    WHERE meta_update_datetime > $wm_reporting_base_prod_staging_session_uri
    UNION ALL
    SELECT session_id
    FROM staging.session_ga
    WHERE meta_update_datetime > $wm_reporting_base_prod_staging_session_ga
    UNION ALL
    SELECT session_id
    FROM staging.membership_state
    WHERE meta_update_datetime > $wm_reporting_base_prod_staging_membership_state
    UNION ALL
    SELECT session_id
    FROM staging.site_visit_metrics
    WHERE meta_update_datetime > $wm_reporting_base_prod_staging_site_visit_metrics
    UNION ALL
    SELECT session_id
    FROM staging.visitor_session
    WHERE meta_update_datetime > $wm_reporting_base_prod_staging_visitor_session
    UNION ALL
    SELECT session_id
    FROM staging.migrated_session
    WHERE meta_update_datetime > $wm_reporting_base_prod_staging_migrated_session
    UNION ALL
    SELECT session_id
    FROM shared.session_ab_test_start
    WHERE meta_update_datetime > $wm_reporting_base_prod_shared_session_ab_test_start
    UNION ALL
    SELECT session_id
    FROM staging.customer_session
    WHERE meta_update_datetime > $wm_reporting_base_prod_staging_customer_session
    UNION ALL
    SELECT session_id
    FROM lake_consolidated_view.ultra_merchant.session_detail
    WHERE meta_update_datetime > $wm_lake_consolidated_view_ultra_merchant_session_detail
    UNION ALL
    SELECT session_id
    FROM lake_consolidated_view.ultra_merchant.session
    WHERE meta_update_datetime > $wm_lake_consolidated_view_ultra_merchant_session
    UNION ALL
    SELECT session_id
    FROM lake_consolidated_view.ultra_merchant.session_log
    WHERE meta_update_datetime > $wm_lake_consolidated_view_ultra_merchant_session_log
    UNION ALL
    SELECT session_id
    FROM lake_consolidated_view.ultra_merchant.membership_skip
    WHERE meta_update_datetime > $wm_lake_consolidated_view_ultra_merchant_membership_skip
    UNION ALL
    SELECT session_id
    FROM staging.session_segment_events
    WHERE meta_update_datetime > $wm_reporting_base_prod_staging_session_segment_events
    ) AS s
WHERE
    $is_full_refresh = FALSE
    AND s.session_id > 0
ORDER BY s.session_id;

CREATE OR REPLACE TEMP TABLE _session_gateway AS
SELECT DISTINCT
    base.session_id,
    s.dm_gateway_id,
    s.dm_site_id,
    s.dm_gateway_test_site_id
FROM _session_base AS base
JOIN lake_consolidated_view.ultra_merchant.session AS s
    ON s.session_id = base.session_id
    AND s.datetime_added >= $min_refresh_datetime
LEFT JOIN lake_consolidated_view.ultra_merchant.dm_gateway_test dmgt
    ON dmgt.dm_gateway_id = s.dm_gateway_id
    AND dmgt.datetime_start <= s.datetime_added
    AND NVL(dmgt.datetime_end, '9999-12-31') > s.datetime_added;

CREATE OR REPLACE TEMP TABLE _session_data AS
SELECT DISTINCT base.session_id,
       s.store_id,
       s.customer_id,
       mem.membership_id,
       s.datetime_added,
       cs.previous_customer_session_id,
       cs.previous_customer_session_datetime,
       sg.dm_gateway_test_site_id,
       COALESCE(sg.dm_gateway_id, -2) AS dm_gateway_id,
       COALESCE(sg.dm_site_id, -2)    AS dm_site_id,
       s.ip,
       s.meta_original_session_id
FROM _session_base AS base
         LEFT JOIN lake_consolidated_view.ultra_merchant.session AS s
              ON s.session_id = base.session_id
                AND s.datetime_added >= $min_refresh_datetime
         JOIN _session_gateway sg
              ON sg.session_id = base.session_id
         LEFT JOIN staging.customer_session AS cs
                   ON cs.session_id = base.session_id
         LEFT JOIN lake_consolidated_view.ultra_merchant.membership AS mem
                   ON mem.customer_id = s.customer_id
ORDER BY base.session_id;

CREATE OR REPLACE TEMP TABLE _session_data_stg AS
SELECT DISTINCT
    base.session_id,
    base.store_id,
    base.customer_id,
    base.membership_id,
    base.datetime_added,
    base.previous_customer_session_id,
    base.previous_customer_session_datetime,
    base.dm_gateway_test_site_id,
    base.dm_gateway_id,
    base.dm_site_id,
    base.ip,
    base.meta_original_session_id,
    LOWER(dg.gateway_type) AS gateway_type,
    LOWER(dg.gateway_sub_type) AS gateway_sub_type,
    IFF(LOWER(dg.gateway_name) LIKE '%%flm%%',1,0) AS is_male_gateway,
    IFF(LOWER(dg.gateway_name) LIKE '%%yty%%' OR LOWER(dg.gateway_name) LIKE '%%yitty%%' OR LOWER(dg.gateway_name) LIKE '%%yt%%',1,0) AS is_yitty_gateway
FROM _session_data base
    -- TODO: dim_gateway needs to be modified
LEFT JOIN shared.dim_gateway AS dg
    ON dg.dm_gateway_id = base.dm_gateway_id
    AND dg.effective_start_datetime < base.datetime_added
    AND dg.effective_end_datetime >= base.datetime_added
    AND LOWER(dg.gateway_type) NOT IN ('theme (savage x only)', 'theme', 'not applicable')
QUALIFY ROW_NUMBER() OVER (PARTITION BY base.session_id ORDER BY dg.effective_start_datetime desc) = 1
ORDER BY base.session_id;

CREATE OR REPLACE TEMP TABLE _scrubs_gateway AS
SELECT DISTINCT
base.session_id
FROM _session_data base
INNER JOIN shared.dim_gateway AS dg
    ON dg.dm_gateway_id = base.dm_gateway_id
    AND dg.effective_start_datetime < base.datetime_added
    AND dg.effective_end_datetime >= base.datetime_added
WHERE LOWER(dg.gateway_name) ILIKE '%%scbus%%';

CREATE OR REPLACE TEMP TABLE _session_uri_stg AS
SELECT
    base.session_id,
    su.referer,
    su.uri,
    su.clickid,
    su.sharedid,
    su.irad,
    su.mpid,
    su.irmp,
    su.utm_medium,
    su.utm_source,
    su.utm_campaign,
    su.utm_content,
    su.utm_term,
    su.ccode,
    su.pcode,
    su.user_agent,
    su.browser,
    su.os,
    su.device,
    su.is_bot_old,
    su.is_bot
FROM _session_data AS base
    JOIN staging.session_uri AS su
        ON su.session_id = base.session_id
        AND NVL(su.meta_update_datetime, current_timestamp()) >= $min_refresh_datetime
QUALIFY ROW_NUMBER() OVER (PARTITION BY base.session_id ORDER BY user_agent desc) = 1
ORDER BY base.session_id;

CREATE OR REPLACE TEMP TABLE _session_ga_stg AS
SELECT
    base.session_id,
    ga.ga_utm_medium,
    ga.ga_utm_source,
    ga.ga_utm_campaign,
    ga.ga_utm_term,
    ga.ga_utm_content,
    ga.ga_device,
    ga.ga_browser,
    ga.ga_referral_path,
    ga.ga_is_lead,
    ga.ga_is_vip,
    ga.ga_customer_id
FROM _session_data AS base
    JOIN staging.session_ga AS ga
        ON ga.session_id = base.session_id
        AND NVL(ga.meta_update_datetime, current_timestamp()) >= $min_refresh_datetime
QUALIFY ROW_NUMBER() OVER (PARTITION BY base.session_id ORDER BY ga.meta_update_datetime desc) = 1
ORDER BY base.session_id;

CREATE OR REPLACE TEMP TABLE _site_visit_metrics_stg AS
SELECT
    base.session_id,
    ms.customer_id,
    svm.is_quiz_start_action,
    svm.is_quiz_complete_action,
    svm.is_lead_registration_action,
    svm.is_speedy_registration_action,
    svm.is_quiz_registration_action,
    svm.is_skip_quiz_registration_action
FROM _session_data AS base
LEFT JOIN staging.site_visit_metrics AS svm
    ON base.session_id = svm.session_id
    AND NVL(svm.meta_update_datetime, current_timestamp()) >= $min_refresh_datetime
LEFT JOIN lake_consolidated_view.ultra_merchant.membership_signup ms
    ON ms.session_id = base.session_id
    AND ms.datetime_added >= $min_refresh_datetime
QUALIFY ROW_NUMBER() OVER (PARTITION BY base.session_id ORDER BY ms.customer_id desc) = 1
ORDER BY base.session_id;

CREATE OR REPLACE TEMP TABLE _membership_state_stg AS
SELECT
    base.session_id,
    ms.membership_state,
    ms.daily_lead_tenure,
    ms.monthly_vip_tenure
FROM _session_data AS base
    JOIN staging.membership_state AS ms
WHERE ms.session_id = base.session_id
AND NVL(ms.meta_update_datetime, current_timestamp()) >= $min_refresh_datetime
QUALIFY ROW_NUMBER() OVER (PARTITION BY base.session_id ORDER BY ms.meta_update_datetime desc) = 1
ORDER BY base.session_id;

CREATE OR REPLACE TEMP TABLE _visitor_session_stg AS
SELECT
    base.session_id,
    vs.visitor_id,
    vs.previous_visitor_session_id,
    vs.previous_visitor_session_datetime
FROM _session_data AS base
    JOIN staging.visitor_session AS vs
WHERE vs.session_id = base.session_id
AND NVL(vs.meta_update_datetime, current_timestamp()) >= $min_refresh_datetime
QUALIFY ROW_NUMBER() OVER (PARTITION BY base.session_id ORDER BY vs.meta_update_datetime desc) = 1
ORDER BY base.session_id;

CREATE OR REPLACE TEMP TABLE _migrated_session_stg AS
SELECT base.session_id,
       ms.migration_previous_session_id
FROM _session_data AS base
         JOIN staging.migrated_session AS ms
WHERE ms.session_id = base.session_id
  AND NVL(ms.meta_update_datetime, CURRENT_TIMESTAMP()) >= $min_refresh_datetime
QUALIFY ROW_NUMBER() OVER (PARTITION BY base.session_id ORDER BY ms.meta_update_datetime desc) = 1
ORDER BY base.session_id;

CREATE OR REPLACE TEMPORARY TABLE _male_session_action AS
SELECT DISTINCT base.session_id
FROM _session_data AS base
JOIN lake_consolidated_view.ultra_merchant.session_detail AS sd
    ON base.session_id=sd.session_id
    AND sd.datetime_added >= $min_refresh_datetime
WHERE
    ((sd.name = 'gender_select_clicked_mens' AND sd.value = '1')
    OR (sd.name = 'gender' AND sd.value = 'M'));

CREATE OR REPLACE TEMPORARY TABLE _scrubs_session_action AS
SELECT DISTINCT
    sb.session_id
FROM _session_data AS sb
JOIN lake_consolidated_view.ultra_merchant.session_detail sd
    ON sb.session_id = sd.session_id
           AND sd.name = 'isScrubs'
           AND sd.value = 1
           AND sd.datetime_added >= $min_refresh_datetime;

CREATE OR REPLACE TEMPORARY TABLE _skip_quiz_action AS
SELECT DISTINCT base.session_id
FROM _session_data AS base
JOIN lake_consolidated_view.ultra_merchant.session_detail sd
    ON base.session_id=sd.session_id
    AND sd.datetime_added >= $min_refresh_datetime
WHERE name IN ('skipquiz','skip style quiz','quiz_skip','skip_quiz','quiz_skip_count');

CREATE OR REPLACE TEMPORARY TABLE _skip_month_action AS
SELECT DISTINCT base.session_id
FROM _session_data AS base
JOIN lake_consolidated_view.ultra_merchant.membership_skip ms
    ON base.session_id=ms.session_id
           AND ms.datetime_added >= $min_refresh_datetime;

CREATE OR REPLACE TEMPORARY TABLE _ab_test_session AS
SELECT
     sb.session_id
    ,'| ' || LISTAGG(ab_test_key||':'||ab_test_segment, '|') WITHIN GROUP (ORDER BY ab_test_key ASC, ab_test_segment ASC) || ' |' AS session_detail_ab_test_values
FROM _session_data sb
JOIN shared.session_ab_test_start b
    ON sb.session_id = b.session_id
    AND NVL(b.meta_update_datetime, current_timestamp()) >= $min_refresh_datetime
    AND ab_test_type = 'session'
GROUP BY sb.session_id;

CREATE OR REPLACE TEMPORARY TABLE _ab_test_membership AS
SELECT
     sb.session_id
    ,'| ' || LISTAGG(ab_test_key||':'||ab_test_segment, '|') WITHIN GROUP (ORDER BY ab_test_key ASC) || ' |' AS membership_detail_ab_test_values
FROM _session_data sb
JOIN shared.session_ab_test_start b
    ON sb.session_id = b.session_id
    AND NVL(b.meta_update_datetime, current_timestamp()) >= $min_refresh_datetime
    AND ab_test_type = 'membership'
GROUP BY sb.session_id;

CREATE OR REPLACE TEMPORARY TABLE _session_log AS
SELECT sb.session_id,
    MAX(IFF(sl.session_action_id = 5, 1, 0)) AS is_login_action,
    MAX(IFF(sl.session_action_id = 8, 1, 0)) AS is_logout_action
FROM lake_consolidated_view.ultra_merchant.session_log sl
JOIN _session_data sb
    ON sb.session_id = sl.session_id
    AND sl.datetime_added >= $min_refresh_datetime
    AND sl.session_action_id IN (5,8)
GROUP BY sb.session_id;

CREATE OR REPLACE TEMPORARY TABLE _male_customers AS
SELECT
    sb.session_id,
    IFF(cd.customer_id IS NOT NULL,1,0) AS is_male_customer
FROM _session_data AS sb
JOIN lake_consolidated_view.ultra_merchant.session AS s
    ON s.session_id = sb.session_id
    AND s.datetime_added >= $min_refresh_datetime
JOIN lake_consolidated_view.ultra_merchant.store AS r
    ON r.store_id = s.store_id
LEFT JOIN (
    SELECT customer_id, MAX(datetime_modified) AS datetime_modified
    FROM lake_consolidated_view.ultra_merchant.customer_detail
    WHERE name = 'gender'
        AND value IN ('M','m')
    GROUP BY customer_id
    ) AS cd
    ON cd.customer_id = s.customer_id
WHERE r.alias = 'Fabletics'
QUALIFY ROW_NUMBER() OVER (PARTITION BY cd.customer_id, sb.session_id ORDER BY cd.datetime_modified DESC) = 1;

CREATE OR REPLACE TEMPORARY TABLE _scrubs_customers AS
SELECT DISTINCT cd.customer_id
FROM _session_data AS sd
LEFT JOIN lake_consolidated_view.ultra_merchant.store AS r
    ON r.store_id = sd.store_id
INNER JOIN (
    SELECT customer_id, MAX(datetime_modified) AS datetime_modified
    FROM lake_consolidated_view.ultra_merchant.customer_detail
    WHERE name = 'isScrubs'
    AND value = 1
    GROUP BY customer_id) AS cd ON cd.customer_id = sd.customer_id
WHERE r.alias = 'Fabletics';

CREATE OR REPLACE TEMPORARY TABLE _cart_checkout_visit AS
SELECT DISTINCT session_id
FROM (
     SELECT fo.session_id
     FROM edw_prod.data_model.fact_order AS fo
        JOIN _session_data sb
            ON fo.session_id = sb.session_id
            AND fo.meta_update_datetime >= $min_refresh_datetime
        JOIN edw_prod.data_model.dim_order_sales_channel AS m
            ON m.order_sales_channel_key = fo.order_sales_channel_key
     WHERE order_sales_channel_l1 = 'Online Order'
);

CREATE OR REPLACE TEMPORARY TABLE _segment_events AS
SELECT DISTINCT meta_original_session_id,
                se.session_id,
                is_mobile_app,
                context_page_path,
                properties_url,
                utm_source,
                utm_campaign,
                utm_medium,
                utm_term,
                utm_content,
                user_agent,
                is_cart_checkout_session,
                is_pdp_page_view,
                is_atb_session,
                atb_count,
                is_pdp_view,
                is_registration_action,
                is_signed_in_action,
                is_order_action,
                is_activating,
                is_branch_open
FROM staging.session_segment_events se
JOIN _session_base sb on se.session_id = sb.session_id
WHERE NVL(se.meta_update_datetime, CURRENT_DATE()) >= $min_refresh_datetime;

CREATE OR REPLACE TEMPORARY TABLE _session_stg AS
SELECT DISTINCT
    s.session_id,
CASE
           WHEN sse.properties_url ILIKE '%yitty.fabletics.%' THEN
               CASE
                   WHEN s.store_id = 52 THEN 241
                   WHEN s.store_id = 151 THEN 24101
                   ELSE s.store_id
                   END
           WHEN su.user_agent ILIKE 'fabletics%' OR su.user_agent ILIKE '%fableticsapp%' THEN
               CASE s.store_id
                   WHEN 52 THEN 151
                   WHEN 65 THEN 152
                   WHEN 69 THEN 154
                   WHEN 67 THEN 153
                   WHEN 71 THEN 155
                   ELSE s.store_id
                   END
           ELSE s.store_id
           END AS stg_store_id,
    NVL(s.customer_id, svm.customer_id) AS stg_customer_id,
    s.membership_id,
    CONVERT_TIMEZONE(NVL(st.time_zone, 'America/Los_Angeles') , s.datetime_added::timestamp_tz) AS session_local_datetime,
    s.previous_customer_session_id,
    CONVERT_TIMEZONE(st.time_zone, s.previous_customer_session_datetime::timestamp_tz) AS previous_customer_session_local_datetime,
    s.dm_gateway_test_site_id,
    s.dm_gateway_id,
    s.dm_site_id,
    s.ip,
    s.gateway_type,
    s.gateway_sub_type,
    su.referer,
    su.uri,
    su.clickid,
    su.sharedid,
    su.irad,
    su.mpid,
    su.irmp,
    LOWER(COALESCE(su.utm_medium, ga.ga_utm_medium, sse.utm_medium)) AS stg_utm_medium,
    LOWER(COALESCE(su.utm_source, ga.ga_utm_source, sse.utm_source)) AS stg_utm_source,
    LOWER(COALESCE(su.utm_campaign, ga.ga_utm_campaign, sse.utm_campaign)) AS stg_utm_campaign,
    LOWER(COALESCE(su.utm_content, ga.ga_utm_content, sse.utm_content)) AS stg_utm_content,
    LOWER(COALESCE(su.utm_term, ga.ga_utm_term, sse.utm_term)) AS stg_utm_term,
    su.ccode,
    su.pcode,
    su.user_agent,
    COALESCE(su.browser, ga.ga_browser) AS stg_browser,
    su.os,
    NVL(INITCAP(IFF(sse.is_mobile_app OR sse.is_branch_open, 'Mobile App',
                       IFF(NVL(ds.store_type, 'Online') = 'Mobile App', 'Mobile App',
                           IFF(su.user_agent ILIKE 'fabletics%' OR su.user_agent ILIKE '%fableticsapp%', 'Mobile App',
                               COALESCE(su.device, ga.ga_device))))), 'Unknown')   AS platform,
    NVL(su.is_bot_old,0) AS stg_is_bot_old,
    NVL(su.is_bot,0) AS stg_is_bot,
    svm.is_quiz_start_action,
    svm.is_quiz_complete_action,
    svm.is_lead_registration_action,
    svm.is_speedy_registration_action,
    svm.is_quiz_registration_action,
    svm.is_skip_quiz_registration_action,
    NVL(ms.membership_state,'Prospect') AS stg_membership_state,
    ms.daily_lead_tenure,
    ms.monthly_vip_tenure,
    ga.ga_utm_medium,
    ga.ga_utm_source,
    ga.ga_utm_campaign,
    ga.ga_utm_term,
    ga.ga_utm_content,
    ga.ga_device,
    ga.ga_browser,
    ga.ga_referral_path,
    ga.ga_is_lead,
    ga.ga_is_vip,
    ga.ga_customer_id,
    vs.visitor_id,
    vs.previous_visitor_session_id,
    CONVERT_TIMEZONE(st.time_zone, vs.previous_visitor_session_datetime::timestamp_tz) AS previous_visitor_session_local_datetime,
    mi.migration_previous_session_id,
    IFF(mi.migration_previous_session_id IS NOT NULL, TRUE, FALSE) AS is_migrated_session,
    NVL(sl.is_login_action, FALSE) AS is_login_action,
    NVL(sl.is_logout_action, FALSE) AS is_logout_action,
    s.is_male_gateway,
    s.is_yitty_gateway,
    IFF(sg.session_id IS NOT NULL, 1, 0) AS is_scrubs_gateway,
    IFF(msa.session_id IS NOT NULL, 1, 0) AS is_male_session_action,
    IFF(s.is_male_gateway = 1 OR is_male_session_action= 1, 1, 0) AS is_male_session,
    IFF(qa.session_id IS NOT NULL, 1, 0) AS is_skip_quiz_action,
    IFF(ma.session_id IS NOT NULL,1, 0) AS is_skip_month_action,
    at.session_detail_ab_test_values,
    tm.membership_detail_ab_test_values,
    IFF(sse.is_atb_session, 1, 0) AS is_atb_action,
    IFF(tca.customer_id IS NOT NULL, 1, 0) AS is_test_customer_account,
    IFF(sse.is_cart_checkout_session
            OR ccv.session_id IS NOT NULL
            OR sse.is_order_action, 1, 0) AS is_cart_checkout_visit,
    IFF(sse.is_pdp_view OR sse.is_pdp_page_view, 1, 0) AS is_pdp_visit,
    NVL(mcus.is_male_customer, FALSE) AS stg_is_male_customer,
    s.meta_original_session_id,
    IFF(session_local_datetime::DATE < '2022-01-01'
            OR platform = 'Mobile App'
            OR sse.session_id IS NOT NULL, 1, 0)  AS is_in_segment,
    sse.context_page_path AS first_page_path,
    IFF(sc.customer_id IS NOT NULL, 1, 0) AS is_scrubs_customer,
    IFF(ssa.session_id IS NOT NULL, 1, 0)                             AS is_scrubs_action,
    IFF(ssa.session_id IS NOT NULL OR is_scrubs_gateway, TRUE, FALSE) AS is_scrubs_session,
    sse.properties_url                                                AS first_page_url,
    IFF(ds.store_brand_abbr = 'FL',
        CASE
            WHEN NVL(s.customer_id, svm.customer_id) IS NOT NULL AND
                 sc.customer_id IS NOT NULL THEN 'Scrubs'
            WHEN NVL(s.customer_id, svm.customer_id) IS NULL AND ssa.session_id IS NOT NULL
                THEN 'Scrubs'
            WHEN NVL(s.customer_id, svm.customer_id) IS NULL AND sg.session_id IS NOT NULL
                THEN 'Scrubs'
            WHEN NVL(s.customer_id, svm.customer_id) IS NULL AND is_yitty_gateway
                THEN 'Yitty'
            ELSE CONCAT('Fabletics', ' ',
                        CASE
                            WHEN
                                NVL(s.customer_id, svm.customer_id) IS NOT NULL AND
                                is_male_customer = TRUE THEN 'Mens'
                            WHEN
                                NVL(s.customer_id, svm.customer_id) IS NULL AND
                                msa.session_id IS NOT NULL THEN 'Mens'
                            WHEN NVL(s.customer_id, svm.customer_id) IS NULL AND is_male_gateway = TRUE
                                THEN 'Mens'
                            ELSE 'Womens'
                            END)
            END, ds.store_brand) AS user_segment,
    IFF(LOWER(stg_utm_medium) IS NULL
    AND LOWER(stg_utm_source) IS NULL
    AND NULLIF(s.gateway_type, 'brand site') IS NULL, lower(CASE
            WHEN su.referer = 'android-app://com.google.android.gm' THEN NULL
            WHEN su.referer ILIKE '%google%' THEN 'google'
            WHEN su.referer ILIKE '%yahoo%' THEN 'yahoo'
            WHEN su.referer ILIKE '%bing%' THEN 'bing'
        END), NULL) AS seo_vendor,
    HASH(
    s.session_id, stg_store_id, stg_customer_id, s.membership_id, session_local_datetime, previous_customer_session_id,
    previous_customer_session_local_datetime, vs.visitor_id, vs.previous_visitor_session_id, previous_visitor_session_local_datetime,
    is_migrated_session, mi.migration_previous_session_id, is_login_action, is_logout_action, s.is_male_gateway,
    s.is_yitty_gateway, is_male_session_action,is_male_session,stg_is_male_customer, s.dm_gateway_test_site_id,
    s.dm_gateway_id, s.dm_site_id, s.gateway_type, s.gateway_sub_type, svm.is_quiz_start_action, svm.is_quiz_complete_action,
    is_skip_quiz_action, svm.is_lead_registration_action,svm.is_speedy_registration_action, svm.is_quiz_registration_action,
    svm.is_skip_quiz_registration_action, is_skip_month_action, is_atb_action,  stg_membership_state,
    at.session_detail_ab_test_values, tm.membership_detail_ab_test_values, ms.daily_lead_tenure, ms.monthly_vip_tenure,
    s.ip, su.referer, seo_vendor, su.uri, su.user_agent, stg_browser, su.os, platform, stg_is_bot_old, stg_is_bot,
    is_test_customer_account, is_cart_checkout_visit, is_pdp_visit, su.clickid, su.sharedid, su.irad, su.mpid,
    su.irmp, stg_utm_medium, stg_utm_source, stg_utm_campaign, stg_utm_content, stg_utm_term, su.ccode, su.pcode, ga.ga_utm_medium,
    ga.ga_utm_source, ga.ga_utm_campaign, ga.ga_utm_term, ga.ga_utm_content, ga.ga_device, ga.ga_browser, ga.ga_referral_path,
    ga.ga_is_lead, ga.ga_is_vip, ga.ga_customer_id, is_in_segment, first_page_path, is_scrubs_customer,
    is_scrubs_action, is_scrubs_session, first_page_url, user_segment
    ) as meta_row_hash
FROM _session_data_stg AS s
    LEFT JOIN _session_uri_stg AS su
        ON su.session_id = s.session_id
    LEFT JOIN _session_ga_stg AS ga
        ON ga.session_id = s.session_id
    LEFT JOIN _site_visit_metrics_stg AS svm
        ON svm.session_id = s.session_id
    LEFT JOIN _membership_state_stg AS ms
        ON ms.session_id = s.session_id
    LEFT JOIN _visitor_session_stg AS vs
        ON vs.session_id = s.session_id
    LEFT JOIN _migrated_session_stg AS mi
        ON mi.session_id = s.session_id
    LEFT JOIN _session_log AS sl
        ON s.session_id = sl.session_id
    LEFT JOIN edw_prod.reference.store_timezone AS st
        ON st.store_id = s.store_id
    LEFT JOIN _male_session_action AS msa
        ON msa.session_id = s.session_id
    LEFT JOIN _skip_quiz_action AS qa
        ON s.session_id = qa.session_id
    LEFT JOIN _skip_month_action  AS ma
        ON s.session_id = ma.session_id
    LEFT JOIN _ab_test_session AS at
        ON s.session_id = at.session_id
    LEFT JOIN _ab_test_membership AS tm
        ON s.session_id = tm.session_id
    LEFT JOIN edw_prod.reference.test_customer AS tca
        ON tca.customer_id = s.customer_id
    LEFT JOIN _male_customers AS mcus
        ON mcus.session_id = s.session_id
    LEFT JOIN _cart_checkout_visit AS ccv
        ON ccv.session_id = s.session_id
    LEFT JOIN _scrubs_customers AS sc
        ON s.customer_id = sc.customer_id
    LEFT JOIN _scrubs_session_action AS ssa
        ON s.session_id = ssa.session_id
    LEFT JOIN _scrubs_gateway AS sg
        ON s.session_id = sg.session_id
    LEFT JOIN _segment_events sse
        ON s.session_id = sse.session_id
    LEFT JOIN edw_prod.stg.dim_store AS ds
        ON ds.store_id = stg_store_id
WHERE session_local_datetime >= '2022-01-01 00:00:00.000'
ORDER BY s.session_id;


MERGE INTO shared.session t
    USING _session_stg src
    ON EQUAL_NULL(t.session_id, src.session_id)
    WHEN NOT MATCHED THEN
        INSERT (session_id,
                store_id,
                customer_id,
                membership_id,
                session_local_datetime,
                previous_customer_session_id,
                previous_customer_session_local_datetime,
                next_customer_session_id,
                next_customer_session_local_datetime,
                visitor_id,
                previous_visitor_session_id,
                previous_visitor_session_local_datetime,
                next_visitor_session_id,
                next_visitor_session_local_datetime,
                is_migrated_session,
                migration_previous_session_id,
                add_to_cart_count,
                echo_add_to_cart_count,
                is_login_action,
                is_logout_action,
                is_male_gateway,
                is_yitty_gateway,
                is_scrubs_gateway,
                is_male_session_action,
                is_male_session,
                is_male_customer,
                dm_gateway_test_site_id,
                dm_gateway_id,
                dm_site_id,
                gateway_type,
                gateway_sub_type,
                is_quiz_start_action,
                is_quiz_complete_action,
                is_skip_quiz_action,
                is_lead_registration_action,
                is_speedy_registration_action,
                is_quiz_registration_action,
                is_skip_quiz_registration_action,
                is_skip_month_action,
                is_atb_action,
                membership_state,
                session_detail_ab_test_values,
                membership_detail_ab_test_values,
                daily_lead_tenure,
                monthly_vip_tenure,
                ip,
                referer,
                seo_vendor,
                uri,
                user_agent,
                browser,
                os,
                platform,
                is_bot_old,
                is_bot,
                is_test_customer_account,
                is_cart_checkout_visit,
                is_pdp_visit,
                clickid,
                sharedid,
                irad,
                mpid,
                irmp,
                utm_medium,
                utm_source,
                utm_campaign,
                utm_content,
                utm_term,
                ccode,
                pcode,
                ga_utm_medium,
                ga_utm_source,
                ga_utm_campaign,
                ga_utm_term,
                ga_utm_content,
                ga_device,
                ga_browser,
                ga_referral_path,
                ga_is_lead,
                ga_is_vip,
                ga_customer_id,
                media_source_hash,
                meta_original_session_id,
                is_in_segment,
                first_page_path,
                is_scrubs_customer,
                is_scrubs_action,
                is_scrubs_session,
                first_page_url,
                user_segment,
                meta_row_hash,
                meta_create_datetime,
                meta_update_datetime)
            VALUES (src.session_id,
                    src.stg_store_id,
                    src.stg_customer_id,
                    src.membership_id,
                    src.session_local_datetime,
                    src.previous_customer_session_id,
                    src.previous_customer_session_local_datetime,
                    NULL,
                    NULL,
                    src.visitor_id,
                    src.previous_visitor_session_id,
                    src.previous_visitor_session_local_datetime,
                    NULL,
                    NULL,
                    src.is_migrated_session,
                    src.migration_previous_session_id,
                    NULL,
                    NULL,
                    src.is_login_action,
                    src.is_logout_action,
                    src.is_male_gateway,
                    src.is_yitty_gateway,
                    src.is_scrubs_gateway,
                    src.is_male_session_action,
                    src.is_male_session,
                    src.stg_is_male_customer,
                    src.dm_gateway_test_site_id,
                    src.dm_gateway_id,
                    src.dm_site_id,
                    src.gateway_type,
                    src.gateway_sub_type,
                    src.is_quiz_start_action,
                    src.is_quiz_complete_action,
                    src.is_skip_quiz_action,
                    src.is_lead_registration_action,
                    src.is_speedy_registration_action,
                    src.is_quiz_registration_action,
                    src.is_skip_quiz_registration_action,
                    src.is_skip_month_action,
                    src.is_atb_action,
                    src.stg_membership_state,
                    src.session_detail_ab_test_values,
                    src.membership_detail_ab_test_values,
                    src.daily_lead_tenure,
                    src.monthly_vip_tenure,
                    src.ip,
                    src.referer,
                    src.seo_vendor,
                    src.uri,
                    src.user_agent,
                    src.stg_browser,
                    src.os,
                    src.platform,
                    src.stg_is_bot_old,
                    src.stg_is_bot,
                    src.is_test_customer_account,
                    src.is_cart_checkout_visit,
                    src.is_pdp_visit,
                    src.clickid,
                    src.sharedid,
                    src.irad,
                    src.mpid,
                    src.irmp,
                    src.stg_utm_medium,
                    src.stg_utm_source,
                    src.stg_utm_campaign,
                    src.stg_utm_content,
                    src.stg_utm_term,
                    src.ccode,
                    src.pcode,
                    src.ga_utm_medium,
                    src.ga_utm_source,
                    src.ga_utm_campaign,
                    src.ga_utm_term,
                    src.ga_utm_content,
                    src.ga_device,
                    src.ga_browser,
                    src.ga_referral_path,
                    src.ga_is_lead,
                    src.ga_is_vip,
                    src.ga_customer_id,
                    HASH(src.stg_utm_source, src.stg_utm_medium, src.stg_utm_campaign, src.gateway_type,
                         src.gateway_sub_type, src.seo_vendor),
                    src.meta_original_session_id,
                    src.is_in_segment,
                    src.first_page_path,
                    src.is_scrubs_customer,
                    src.is_scrubs_action,
                    src.is_scrubs_session,
                    src.first_page_url,
                    src.user_segment,
                    src.meta_row_hash,
                    $execution_start_time,
                    $execution_start_time)
    WHEN MATCHED AND NOT (
        EQUAL_NULL(t.store_id, src.stg_store_id)
            AND EQUAL_NULL(t.customer_id, stg_customer_id)
            AND EQUAL_NULL(t.membership_id, src.membership_id)
            AND EQUAL_NULL(t.session_local_datetime, src.session_local_datetime)
            AND EQUAL_NULL(t.previous_customer_session_id, src.previous_customer_session_id)
            AND EQUAL_NULL(t.previous_customer_session_local_datetime, src.previous_visitor_session_local_datetime)
            AND EQUAL_NULL(t.visitor_id, src.visitor_id)
            AND EQUAL_NULL(t.previous_visitor_session_id, src.previous_visitor_session_id)
            AND EQUAL_NULL(t.previous_visitor_session_local_datetime, src.previous_visitor_session_local_datetime)
            AND EQUAL_NULL(t.is_migrated_session, src.is_migrated_session)
            AND EQUAL_NULL(t.migration_previous_session_id, src.migration_previous_session_id)
            AND EQUAL_NULL(t.is_login_action, src.is_login_action)
            AND EQUAL_NULL(t.is_logout_action, src.is_logout_action)
            AND EQUAL_NULL(t.is_male_gateway, src.is_male_gateway)
            AND EQUAL_NULL(t.is_yitty_gateway, src.is_yitty_gateway)
            AND EQUAL_NULL(t.is_scrubs_gateway, src.is_scrubs_gateway)
            AND EQUAL_NULL(t.is_male_session_action, src.is_male_session_action)
            AND EQUAL_NULL(t.is_male_session, src.is_male_session)
            AND EQUAL_NULL(t.is_male_customer, src.stg_is_male_customer)
            AND EQUAL_NULL(t.dm_gateway_test_site_id, src.dm_gateway_test_site_id)
            AND EQUAL_NULL(t.dm_gateway_id, src.dm_gateway_id)
            AND EQUAL_NULL(t.dm_site_id, src.dm_site_id)
            AND EQUAL_NULL(t.gateway_type, src.gateway_type)
            AND EQUAL_NULL(t.gateway_sub_type, src.gateway_sub_type)
            AND EQUAL_NULL(t.is_quiz_start_action, src.is_quiz_start_action)
            AND EQUAL_NULL(t.is_quiz_complete_action, src.is_quiz_complete_action)
            AND EQUAL_NULL(t.is_skip_quiz_action, src.is_skip_quiz_action)
            AND EQUAL_NULL(t.is_lead_registration_action, src.is_lead_registration_action)
            AND EQUAL_NULL(t.is_speedy_registration_action, src.is_speedy_registration_action)
            AND EQUAL_NULL(t.is_quiz_registration_action, src.is_quiz_registration_action)
            AND EQUAL_NULL(t.is_skip_quiz_registration_action, src.is_skip_quiz_registration_action)
            AND EQUAL_NULL(t.is_skip_month_action, src.is_skip_month_action)
            AND EQUAL_NULL(t.is_atb_action, src.is_atb_action)
            AND EQUAL_NULL(t.membership_state, src.stg_membership_state)
            AND EQUAL_NULL(t.session_detail_ab_test_values, src.session_detail_ab_test_values)
            AND EQUAL_NULL(t.membership_detail_ab_test_values, src.membership_detail_ab_test_values)
            AND EQUAL_NULL(t.daily_lead_tenure, src.daily_lead_tenure)
            AND EQUAL_NULL(t.monthly_vip_tenure, src.monthly_vip_tenure)
            AND EQUAL_NULL(t.ip, src.ip)
            AND EQUAL_NULL(t.referer, src.referer)
            AND EQUAL_NULL(t.seo_vendor, src.seo_vendor)
            AND EQUAL_NULL(t.uri, src.uri)
            AND EQUAL_NULL(t.user_agent, src.user_agent)
            AND EQUAL_NULL(t.browser, src.stg_browser)
            AND EQUAL_NULL(t.os, src.os)
            AND EQUAL_NULL(t.platform, src.platform)
            AND EQUAL_NULL(t.is_bot_old, src.stg_is_bot_old)
            AND EQUAL_NULL(t.is_bot, src.stg_is_bot)
            AND EQUAL_NULL(t.is_test_customer_account, src.is_test_customer_account)
            AND EQUAL_NULL(t.is_cart_checkout_visit, src.is_cart_checkout_visit)
            AND EQUAL_NULL(t.is_pdp_visit, src.is_pdp_visit)
            AND EQUAL_NULL(t.clickid, src.clickid)
            AND EQUAL_NULL(t.sharedid, src.sharedid)
            AND EQUAL_NULL(t.irad, src.irad)
            AND EQUAL_NULL(t.mpid, src.mpid)
            AND EQUAL_NULL(t.irmp, src.irmp)
            AND EQUAL_NULL(t.utm_medium, src.stg_utm_medium)
            AND EQUAL_NULL(t.utm_source, src.stg_utm_source)
            AND EQUAL_NULL(t.utm_campaign, src.stg_utm_campaign)
            AND EQUAL_NULL(t.utm_content, src.stg_utm_content)
            AND EQUAL_NULL(t.utm_term, src.stg_utm_term)
            AND EQUAL_NULL(t.ccode, src.ccode)
            AND EQUAL_NULL(t.pcode, src.pcode)
            AND EQUAL_NULL(t.is_in_segment, src.is_in_segment)
            AND EQUAL_NULL(t.first_page_path, src.first_page_path)
            AND EQUAL_NULL(t.is_scrubs_customer, src.is_scrubs_customer)
            AND EQUAL_NULL(t.is_scrubs_action, src.is_scrubs_action)
            AND EQUAL_NULL(t.is_scrubs_session, src.is_scrubs_session)
            AND EQUAL_NULL(t.first_page_url, src.first_page_url)
            AND EQUAL_NULL(t.user_segment, src.user_segment)
        )
        THEN
        UPDATE SET
            t.store_id = src.stg_store_id,
            t.meta_original_session_id = src.meta_original_session_id,
            t.customer_id = src.stg_customer_id,
            t.membership_id = src.membership_id,
            t.session_local_datetime = src.session_local_datetime,
            t.previous_customer_session_id = src.previous_customer_session_id,
            t.previous_customer_session_local_datetime = src.previous_customer_session_local_datetime,
            t.visitor_id = src.visitor_id,
            t.previous_visitor_session_id = src.previous_visitor_session_id,
            t.previous_visitor_session_local_datetime = src.previous_visitor_session_local_datetime,
            t.is_migrated_session = src.is_migrated_session,
            t.migration_previous_session_id = src.migration_previous_session_id,
            t.is_login_action = src.is_login_action,
            t.is_logout_action = src.is_logout_action,
            t.is_male_gateway = src.is_male_gateway,
            t.is_yitty_gateway = src.is_yitty_gateway,
            t.is_scrubs_gateway = src.is_scrubs_gateway,
            t.is_male_session_action = src.is_male_session_action,
            t.is_male_session = src.is_male_session,
            t.is_male_customer = src.stg_is_male_customer,
            t.dm_gateway_test_site_id = src.dm_gateway_test_site_id,
            t.dm_gateway_id = src.dm_gateway_id,
            t.dm_site_id = src.dm_site_id,
            t.gateway_type = src.gateway_type,
            t.gateway_sub_type = src.gateway_sub_type,
            t.is_quiz_start_action = src.is_quiz_start_action,
            t.is_quiz_complete_action = src.is_quiz_complete_action,
            t.is_skip_quiz_action = src.is_skip_quiz_action,
            t.is_lead_registration_action = src.is_lead_registration_action,
            t.is_speedy_registration_action = src.is_speedy_registration_action,
            t.is_quiz_registration_action = src.is_quiz_registration_action,
            t.is_skip_quiz_registration_action = src.is_skip_quiz_registration_action,
            t.is_skip_month_action = src.is_skip_month_action,
            t.is_atb_action = src.is_atb_action,
            t.membership_state = src.stg_membership_state,
            t.session_detail_ab_test_values = src.session_detail_ab_test_values,
            t.membership_detail_ab_test_values = src.membership_detail_ab_test_values,
            t.daily_lead_tenure = src.daily_lead_tenure,
            t.monthly_vip_tenure = src.monthly_vip_tenure,
            t.ip = src.ip,
            t.referer = src.referer,
            t.seo_vendor = src.seo_vendor,
            t.uri = src.uri,
            t.user_agent = src.user_agent,
            t.browser = src.stg_browser,
            t.os = src.os,
            t.platform = src.platform,
            t.is_bot_old = src.stg_is_bot_old,
            t.is_bot = src.stg_is_bot,
            t.is_test_customer_account = src.is_test_customer_account,
            t.is_cart_checkout_visit = src.is_cart_checkout_visit,
            t.is_pdp_visit = src.is_pdp_visit,
            t.clickid = src.clickid,
            t.sharedid = src.sharedid,
            t.irad = src.irad,
            t.mpid = src.mpid,
            t.irmp = src.irmp,
            t.utm_medium = src.stg_utm_medium,
            t.utm_source = src.stg_utm_source,
            t.utm_campaign = src.stg_utm_campaign,
            t.utm_content = src.stg_utm_content,
            t.utm_term = src.stg_utm_term,
            t.ccode = src.ccode,
            t.pcode = src.pcode,
            t.ga_utm_medium = src.ga_utm_medium,
            t.ga_utm_source = src.ga_utm_source,
            t.ga_utm_campaign = src.ga_utm_campaign,
            t.ga_utm_term = src.ga_utm_term,
            t.ga_utm_content = src.ga_utm_content,
            t.ga_device = src.ga_device,
            t.ga_browser = src.ga_browser,
            t.ga_referral_path = src.ga_referral_path,
            t.ga_is_lead = src.ga_is_lead,
            t.ga_is_vip = src.ga_is_vip,
            t.ga_customer_id = src.ga_customer_id,
            t.is_in_segment = src.is_in_segment,
            t.first_page_path = src.first_page_path,
            t.is_scrubs_customer = src.is_scrubs_customer,
            t.is_scrubs_action = src.is_scrubs_action,
            t.is_scrubs_session = src.is_scrubs_session,
            t.first_page_url = src.first_page_url,
            t.user_segment = src.user_segment,
            t.media_source_hash = HASH(src.stg_utm_source, src.stg_utm_medium, src.stg_utm_campaign, src.gateway_type,
                                       src.gateway_sub_type, src.seo_vendor),
            t.meta_row_hash = src.meta_row_hash,
            t.meta_update_datetime = $execution_start_time;

UPDATE public.meta_table_dependency_watermark
SET
    high_watermark_datetime = new_high_watermark_datetime,
    meta_update_datetime = $execution_start_time
WHERE table_name = $target_table;


TRUNCATE TABLE staging.session_excp;
