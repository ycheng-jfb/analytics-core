SET target_table = 'reporting_prod.shared.mobile_app_sessions_stg';
SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
SET is_full_refresh = (SELECT IFF(IFNULL(public.udf_get_watermark($target_table, NULL), '1900-01-01') =
                                  '1900-01-01'::TIMESTAMP_LTZ(3), TRUE,
                                  FALSE));

SET warehouse_to_be_used = IFF($is_full_refresh, 'da_wh_adhoc_large', CURRENT_WAREHOUSE());
USE WAREHOUSE IDENTIFIER ($warehouse_to_be_used);

MERGE INTO public.meta_table_dependency_watermark AS w
    USING
        (SELECT $target_table AS table_name,
                t.dependent_table_name,
                new_high_watermark_datetime
         FROM (SELECT -- For self table
                      NULL                      AS dependent_table_name,
                      MAX(meta_update_datetime) AS new_high_watermark_datetime
               FROM shared.mobile_app_sessions_stg
               UNION ALL
               SELECT 'reporting_prod.shared.sessions_by_platform' AS dependent_table_name,
                      MAX(meta_update_datetime)                        AS new_high_watermark_datetime
               FROM shared.sessions_by_platform
               UNION ALL
               SELECT 'edw_prod.stg.dim_customer' AS dependent_table_name,
                      MAX(meta_update_datetime)   AS new_high_watermark_datetime
               FROM edw_prod.stg.dim_customer) AS t
         ORDER BY COALESCE(t.dependent_table_name, '')) AS s
    ON w.table_name = s.table_name
        AND EQUAL_NULL(w.dependent_table_name, s.dependent_table_name)
    WHEN MATCHED
        AND NOT EQUAL_NULL(w.new_high_watermark_datetime, s.new_high_watermark_datetime)
        THEN UPDATE SET
        w.new_high_watermark_datetime = s.new_high_watermark_datetime,
        w.meta_update_datetime = CURRENT_TIMESTAMP::timestamp_ltz(3)
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

SET wm_reporting_prod_shared_sessions_by_platform = public.udf_get_watermark($target_table,
                                                                             'reporting_prod.shared.sessions_by_platform');
SET wm_edw_prod_stg_dim_customer = public.udf_get_watermark($target_table,
                                                            'edw_prod.stg.dim_customer');

CREATE OR REPLACE TEMP TABLE _mobile_app_sessions_stg__base
(
    session_id NUMBER
);

-- full refresh
INSERT INTO _mobile_app_sessions_stg__base
SELECT DISTINCT session_id
FROM shared.sessions_by_platform
WHERE $is_full_refresh = TRUE;

-- incremental refresh
INSERT INTO _mobile_app_sessions_stg__base
SELECT DISTINCT session_id
FROM (SELECT session_id
      FROM shared.sessions_by_platform
      WHERE meta_update_datetime > $wm_reporting_prod_shared_sessions_by_platform
      UNION ALL
      SELECT s.session_id
      FROM edw_prod.stg.dim_customer dc
               JOIN shared.sessions_by_platform s
                    ON dc.customer_id = s.customer_id
      WHERE dc.meta_update_datetime > $wm_edw_prod_stg_dim_customer) AS incr
WHERE $is_full_refresh = FALSE;

MERGE INTO shared.mobile_app_sessions_stg AS t
USING (
    SELECT DISTINCT sp.store_brand_name                                                                  AS brand,
                sp.store_region_abbr                                                                 AS region,
                sp.store_country_abbr                                                                AS country,
                sp.store_name                                                                        AS store,
                sp.customer_gender,
                sp.membership_state,
                sp.customer_id,
                sp.monthly_vip_tenure,
                CASE
                    WHEN sp.membership_state = 'VIP' AND sp.monthly_vip_tenure BETWEEN 1 AND 12
                        THEN CONCAT('M', sp.monthly_vip_tenure)
                    WHEN sp.membership_state = 'VIP' AND sp.monthly_vip_tenure BETWEEN 13 AND 24 THEN 'M13-24'
                    WHEN sp.membership_state = 'VIP' AND sp.monthly_vip_tenure >= 25 THEN 'M25+'
                    ELSE 'Non-VIP' END                                                              AS vip_tenure_group,
                dc.first_mobile_app_session_id,
                dc.first_mobile_app_session_local_datetime                                          AS first_app_session_hq_datetime,
                dc.first_mobile_app_session_local_datetime::DATE                                    AS first_app_session_date,
                dc.mobile_app_cohort_month_date                                                     AS first_app_session_month_date,
                IFF(dc.first_mobile_app_session_id = sp.session_id, TRUE, FALSE)                     AS is_first_app_session,
                dc.first_mobile_app_order_id,
                dc.first_mobile_app_order_local_datetime,
                sp.operating_system,
                sp.session_id,
                sp.session_local_datetime                                                            AS session_local_datetime,
                sp.session_date,
                DATE_TRUNC('month', sp.session_date)                                                 AS session_month_date,
                RANK() OVER (PARTITION BY sp.customer_id,sp.session_date ORDER BY sp.session_id)       AS rnk_daily,
                RANK() OVER (PARTITION BY sp.customer_id,sp.session_month_date ORDER BY sp.session_id) AS rnk_monthly,
                $execution_start_time                                                               AS meta_create_datetime,
                $execution_start_time                                                               AS meta_update_datetime
    FROM _mobile_app_sessions_stg__base base
             JOIN shared.sessions_by_platform AS sp
                  ON base.session_id = sp.session_id
             LEFT JOIN edw_prod.data_model.dim_customer AS dc ON dc.customer_id = sp.customer_id
    WHERE sp.session_platform = 'Mobile App'
) AS s
ON t.session_id = s.session_id
WHEN MATCHED AND NOT (
    NOT EQUAL_NULL(t.brand, s.brand)
    OR NOT EQUAL_NULL(t.region, s.region)
    OR NOT EQUAL_NULL(t.country, s.country)
    OR NOT EQUAL_NULL(t.store, s.store)
    OR NOT EQUAL_NULL(t.customer_gender, s.customer_gender)
    OR NOT EQUAL_NULL(t.membership_state, s.membership_state)
    OR NOT EQUAL_NULL(t.customer_id, s.customer_id)
    OR NOT EQUAL_NULL(t.monthly_vip_tenure, s.monthly_vip_tenure)
    OR NOT EQUAL_NULL(t.vip_tenure_group, s.vip_tenure_group)
    OR NOT EQUAL_NULL(t.first_mobile_app_session_id, s.first_mobile_app_session_id)
    OR NOT EQUAL_NULL(t.first_app_session_hq_datetime, s.first_app_session_hq_datetime)
    OR NOT EQUAL_NULL(t.first_app_session_date, s.first_app_session_date)
    OR NOT EQUAL_NULL(t.first_app_session_month_date, s.first_app_session_month_date)
    OR NOT EQUAL_NULL(t.is_first_app_session, s.is_first_app_session)
    OR NOT EQUAL_NULL(t.first_mobile_app_order_id, s.first_mobile_app_order_id)
    OR NOT EQUAL_NULL(t.first_mobile_app_order_local_datetime, s.first_mobile_app_order_local_datetime)
    OR NOT EQUAL_NULL(t.operating_system, s.operating_system)
    OR NOT EQUAL_NULL(t.session_local_datetime, s.session_local_datetime)
    OR NOT EQUAL_NULL(t.session_date, s.session_date)
    OR NOT EQUAL_NULL(t.session_month_date, s.session_month_date)
    OR NOT EQUAL_NULL(t.rnk_daily, s.rnk_daily)
    OR NOT EQUAL_NULL(t.rnk_monthly, s.rnk_monthly)
    ) THEN
    UPDATE SET
        t.brand = s.brand,
        t.region = s.region,
        t.country = s.country,
        t.store = s.store,
        t.customer_gender = s.customer_gender,
        t.membership_state = s.membership_state,
        t.customer_id = s.customer_id,
        t.monthly_vip_tenure = s.monthly_vip_tenure,
        t.vip_tenure_group = s.vip_tenure_group,
        t.first_mobile_app_session_id = s.first_mobile_app_session_id,
        t.first_app_session_hq_datetime = s.first_app_session_hq_datetime,
        t.first_app_session_date = s.first_app_session_date,
        t.first_app_session_month_date = s.first_app_session_month_date,
        t.is_first_app_session = s.is_first_app_session,
        t.first_mobile_app_order_id = s.first_mobile_app_order_id,
        t.first_mobile_app_order_local_datetime = s.first_mobile_app_order_local_datetime,
        t.operating_system = s.operating_system,
        t.session_local_datetime = s.session_local_datetime,
        t.session_date = s.session_date,
        t.session_month_date = s.session_month_date,
        t.rnk_daily = s.rnk_daily,
        t.rnk_monthly = s.rnk_monthly,
        t.meta_update_datetime = s.meta_update_datetime
WHEN NOT MATCHED THEN
    INSERT (brand, region, country, store, customer_gender, membership_state, customer_id, monthly_vip_tenure,
            vip_tenure_group, first_mobile_app_session_id, first_app_session_hq_datetime, first_app_session_date,
            first_app_session_month_date, is_first_app_session, first_mobile_app_order_id,
            first_mobile_app_order_local_datetime, operating_system, session_id, session_local_datetime,
            session_date, session_month_date, rnk_daily, rnk_monthly, meta_create_datetime, meta_update_datetime)
    VALUES (s.brand, s.region, s.country, s.store, s.customer_gender, s.membership_state,
            s.customer_id, s.monthly_vip_tenure, s.vip_tenure_group, s.first_mobile_app_session_id,
            s.first_app_session_hq_datetime, s.first_app_session_date, s.first_app_session_month_date,
            s.is_first_app_session, s.first_mobile_app_order_id, s.first_mobile_app_order_local_datetime,
            s.operating_system, s.session_id, s.session_local_datetime, s.session_date,
            s.session_month_date, s.rnk_daily, s.rnk_monthly, s.meta_create_datetime,
            s.meta_update_datetime);

UPDATE public.meta_table_dependency_watermark
SET high_watermark_datetime = new_high_watermark_datetime,
    meta_update_datetime    = CURRENT_TIMESTAMP::timestamp_ltz(3)
WHERE table_name = $target_table;
