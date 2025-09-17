ALTER SESSION SET TIMEZONE='America/Los_Angeles';

SET target_table = 'reporting_base_prod.shared.session_indicator';

MERGE INTO public.meta_table_dependency_watermark AS t
    USING (
        SELECT
            $target_table AS table_name,
            NULLIF(dependent_table_name, $target_table) AS dependent_table_name,
            high_watermark_datetime AS new_high_watermark_datetime
            FROM (
                SELECT
                    'edw_prod.data_model.fact_order' AS dependent_table_name,
                    max(meta_update_datetime) AS high_watermark_datetime
                FROM edw_prod.data_model.fact_order

                UNION ALL
                SELECT
                    'reporting_base_prod.shared.session' AS dependent_table_name,
                    max(meta_update_datetime) AS high_watermark_datetime
                FROM shared.session
            ) AS h
        ) AS s
    ON t.table_name = s.table_name
        AND equal_null(t.dependent_table_name, s.dependent_table_name)
    WHEN MATCHED
        AND not equal_null(t.new_high_watermark_datetime, s.new_high_watermark_datetime)
        THEN UPDATE SET
            t.new_high_watermark_datetime = s.new_high_watermark_datetime,
            t.meta_update_datetime = current_timestamp::timestamp_ltz(3)
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
SET wm_reporting_base_prod_shared_session = public.udf_get_watermark($target_table,'reporting_base_prod.shared.session');

CREATE OR REPLACE TEMP TABLE _session_base AS
SELECT DISTINCT session_id
FROM (
    SELECT session_id
    FROM edw_prod.data_model.fact_order
    WHERE meta_update_datetime > $wm_edw_data_model_fact_order

    UNION ALL
    SELECT session_id
    FROM shared.session
    WHERE meta_update_datetime > $wm_reporting_base_prod_shared_session
    ) AS a
WHERE session_id > 0
ORDER BY session_id ASC;

CREATE OR REPLACE TEMP TABLE _staging_session_indicator as
SELECT
    S.SESSION_ID,
    S.STORE_ID,
    T.store_brand,
    T.store_region,
    T.STORE_COUNTRY,
    S.membership_state,
    S.platform,
    S.session_local_datetime,
    CONVERT_TIMEZONE('America/Los_Angeles', S.session_local_datetime)::DATETIME AS session_hq_datetime,
    s.is_lead_registration_action,
    S.is_migrated_session,
    CASE
        WHEN DATEDIFF('min', CONVERT_TIMEZONE('America/Los_Angeles', S.previous_visitor_session_local_datetime)::DATETIME, CONVERT_TIMEZONE('America/Los_Angeles', S.session_local_datetime)::datetime) <= 1
        THEN TRUE ELSE FALSE END
        AS is_spawned_session,
    CASE WHEN S.VISITOR_ID IS NULL THEN TRUE ELSE FALSE END AS is_session_without_visitor_id,
    S.IS_TEST_CUSTOMER_ACCOUNT,
    S.is_bot_old,
    S.is_bot,
    CASE WHEN OO.order_session_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_session_with_order,
    CASE
        WHEN S.is_bot_old = FALSE AND S.is_test_customer_account = FALSE THEN 'Normal'
        WHEN S.is_bot_old = TRUE AND S.is_test_customer_account = TRUE THEN 'Bot & Test Acct'
        WHEN S.is_bot_old = FALSE AND S.is_test_customer_account = TRUE THEN 'Test Acct'
        WHEN S.is_bot_old = TRUE AND S.is_test_customer_account = FALSE THEN 'Bot'
        ELSE NULL END
    AS traffic_type_old,
    CASE
        WHEN S.is_bot = FALSE AND S.is_test_customer_account = FALSE THEN 'Normal'
        WHEN S.is_bot = TRUE AND S.is_test_customer_account = TRUE THEN 'Bot & Test Acct'
        WHEN S.is_bot = FALSE AND S.is_test_customer_account = TRUE THEN 'Test Acct'
        WHEN S.is_bot = TRUE AND S.is_test_customer_account = FALSE THEN 'Bot'
        ELSE NULL END
    AS traffic_type,
    CASE
        WHEN visitor_id IS NOT NULL
            AND DATEDIFF('min', CONVERT_TIMEZONE('America/Los_Angeles', S.previous_visitor_session_local_datetime)::DATETIME, CONVERT_TIMEZONE('America/Los_Angeles', S.session_local_datetime)::DATETIME) <= 1
            AND is_migrated_session = TRUE
        THEN 'Spawned + Migrated'
        WHEN visitor_id IS NOT NULL
            AND DATEDIFF('min', CONVERT_TIMEZONE('America/Los_Angeles', S.previous_visitor_session_local_datetime)::DATETIME, CONVERT_TIMEZONE('America/Los_Angeles', S.session_local_datetime)::datetime) <= 1
            AND is_migrated_session = FALSE
            THEN 'Spawned'
        WHEN visitor_id IS NULL
            AND DATEDIFF('min', CONVERT_TIMEZONE('America/Los_Angeles', S.previous_visitor_session_local_datetime)::DATETIME, CONVERT_TIMEZONE('America/Los_Angeles', S.session_local_datetime)::DATETIME) >= 2
            AND is_migrated_session = FALSE
        THEN 'Without Visitor ID'
        WHEN visitor_id IS NULL
            AND DATEDIFF('min', CONVERT_TIMEZONE('America/Los_Angeles', S.previous_visitor_session_local_datetime)::DATETIME, CONVERT_TIMEZONE('America/Los_Angeles', S.session_local_datetime)::DATETIME) <= 1
            AND is_migrated_session = TRUE
        THEN 'Spawned + Migrated Without Visitor ID'
        WHEN visitor_id IS NULL
            AND DATEDIFF('min', CONVERT_TIMEZONE('America/Los_Angeles', S.previous_visitor_session_local_datetime)::DATETIME, CONVERT_TIMEZONE('America/Los_Angeles', S.session_local_datetime)::DATETIME) <= 1
            AND S.is_migrated_session = FALSE
        THEN 'Spawned Without Visitor ID'
        WHEN S.visitor_id IS NULL
            AND S.is_migrated_session = TRUE
        THEN 'Migrated Without Visitor ID'
        WHEN S.is_migrated_session = TRUE
        THEN 'Migrated'
        WHEN S.visitor_id IS NULL
        THEN 'Without Visitor ID'
    ELSE 'Normal' END
    AS traffic_issue_type,
    S.user_agent,
    S.uri,
    S.meta_original_session_id,
    nvl(OO.orders,0) as orders,
    NVL(s.is_in_segment, TRUE) as is_in_segment
FROM shared.session AS S
JOIN edw_prod.data_model.dim_store AS T
    ON T.store_id = S.store_id
LEFT JOIN (
    SELECT DISTINCT
        F.session_id AS order_session_id,
        count(f.order_id) as orders
    FROM edw_prod.data_model.fact_order AS F
    JOIN EDW_PROD.DATA_MODEL.DIM_ORDER_SALES_CHANNEL AS OSC on osc.ORDER_SALES_CHANNEL_KEY = f.ORDER_SALES_CHANNEL_KEY
        AND ORDER_SALES_CHANNEL_L1 = 'Online Order'
        AND ORDER_CLASSIFICATION_L1 = 'Product Order'
    group by 1
    ) AS OO
    ON OO.order_session_id = S.session_id
WHERE T.store_type <> 'Retail'
    AND NOT (
        T.store_brand = 'Yitty'
        AND S.session_local_datetime <= '2022-04-02'
    )
    AND S.session_id IN (
        SELECT DISTINCT session_id
        FROM _session_base
    )
;

MERGE INTO shared.session_indicator AS T
USING _staging_session_indicator AS S
    ON S.session_id = T.session_id
    WHEN MATCHED AND (
        COALESCE(T.store_id, -1) <> COALESCE(S.store_id, -1)
        OR COALESCE(T.store_brand, '') <> COALESCE(S.store_brand, '')
        OR COALESCE(T.store_region, '') <> COALESCE(S.store_region, '')
        OR COALESCE(T.store_country, '') <> COALESCE(S.store_country, '')
        OR COALESCE(T.membership_state, '') <> COALESCE(S.membership_state, '')
        OR COALESCE(T.platform, '') <> COALESCE(S.platform, '')
        OR COALESCE(T.session_local_datetime, '1970-01-01 00:00:00') <> COALESCE(S.session_local_datetime, '1970-01-01 00:00:00')
        OR COALESCE(T.session_hq_datetime, '1970-01-01 00:00:00') <> COALESCE(S.session_hq_datetime, '1970-01-01 00:00:00')
        OR COALESCE(CAST(T.is_lead_registration_action AS VARCHAR), '') <> COALESCE(CAST(S.is_lead_registration_action AS VARCHAR), '')
        OR COALESCE(CAST(T.IS_MIGRATED_SESSION AS VARCHAR), '') <> COALESCE(CAST(S.IS_MIGRATED_SESSION AS VARCHAR), '')
        OR COALESCE(CAST(T.IS_SPAWNED_SESSION AS VARCHAR), '') <> COALESCE(CAST(S.IS_SPAWNED_SESSION AS VARCHAR), '')
        OR COALESCE(CAST(T.IS_SESSION_WITHOUT_VISITOR_ID AS VARCHAR), '') <> COALESCE(CAST(S.IS_SESSION_WITHOUT_VISITOR_ID AS VARCHAR), '')
        OR COALESCE(CAST(T.IS_TEST_CUSTOMER_ACCOUNT AS VARCHAR), '') <> COALESCE(CAST(S.IS_TEST_CUSTOMER_ACCOUNT AS VARCHAR), '')
        OR COALESCE(CAST(T.IS_BOT_OLD AS VARCHAR), '') <> COALESCE(CAST(S.IS_BOT_OLD AS VARCHAR), '')
        OR COALESCE(CAST(T.IS_BOT AS VARCHAR), '') <> COALESCE(CAST(S.IS_BOT AS VARCHAR), '')
        OR COALESCE(CAST(T.IS_SESSION_WITH_ORDER AS VARCHAR), '') <> COALESCE(CAST(S.IS_SESSION_WITH_ORDER AS VARCHAR), '')
        OR COALESCE(T.platform, '') <> COALESCE(S.platform, '')
        OR COALESCE(T.traffic_type_old, '') <> COALESCE(S.traffic_type_old, '')
        OR COALESCE(T.traffic_type, '') <> COALESCE(S.traffic_type, '')
        OR COALESCE(T.traffic_issue_type, '') <> COALESCE(S.traffic_issue_type, '')
        OR COALESCE(T.user_agent, '') <> COALESCE(S.user_agent, '')
        OR COALESCE(T.uri, '') <> COALESCE(S.uri, '')
        OR COALESCE(T.meta_original_session_id, -1) <> COALESCE(S.meta_original_session_id, -1)
        OR COALESCE(T.orders, 0) <> COALESCE(S.orders, 0)
        OR NVL(T.is_in_segment, TRUE) <> NVL(S.is_in_segment, TRUE)
    ) THEN
    UPDATE SET
        T.store_id = S.store_id,
        T.store_brand = S.store_brand,
        T.store_region = S.store_region,
        T.store_country = S.store_country,
        T.membership_state = S.membership_state,
        T.platform = S.platform,
        T.session_local_datetime = S.session_local_datetime,
        T.session_hq_datetime = S.session_hq_datetime,
        T.is_lead_registration_action = S.is_lead_registration_action,
        T.is_migrated_session = S.is_migrated_session,
        T.is_spawned_session = S.is_spawned_session,
        T.is_session_without_visitor_id = S.is_session_without_visitor_id,
        T.is_test_customer_account = S.is_test_customer_account,
        T.is_bot_old = S.is_bot_old,
        T.is_bot = S.is_bot,
        T.is_session_with_order = S.is_session_with_order,
        T.traffic_type_old = S.traffic_type_old,
        T.traffic_type = S.traffic_type,
        T.traffic_issue_type = S.traffic_issue_type,
        T.user_agent = S.user_agent,
        T.uri = S.uri,
        T.meta_original_session_id = S.meta_original_session_id,
        T.orders = S.orders,
        T.is_in_segment = S.is_in_segment,
        T.meta_update_datetime = CURRENT_TIMESTAMP
    WHEN NOT MATCHED THEN INSERT(
        session_id,
        store_id,
        store_brand,
        store_region,
        store_country,
        membership_state,
        platform,
        session_local_datetime,
        session_hq_datetime,
        is_lead_registration_action,
        is_migrated_session,
        is_spawned_session,
        is_session_without_visitor_id,
        is_test_customer_account,
        is_bot_old,
        is_bot,
        is_session_with_order,
        traffic_type_old,
        traffic_type,
        traffic_issue_type,
        user_agent,
        uri,
        meta_original_session_id,
        orders,
        meta_create_datetime,
        meta_update_datetime,
        is_in_segment
    ) VALUES (
        S.session_id,
        S.store_id,
        S.store_brand,
        S.store_region,
        S.store_country,
        S.membership_state,
        S.platform,
        S.session_local_datetime,
        S.session_hq_datetime,
        S.is_lead_registration_action,
        S.is_migrated_session,
        S.is_spawned_session,
        S.is_session_without_visitor_id,
        S.is_test_customer_account,
        S.is_bot_old,
        S.is_bot,
        S.is_session_with_order,
        S.traffic_type_old,
        S.traffic_type,
        S.traffic_issue_type,
        S.user_agent,
        S.uri,
        S.meta_original_session_id,
        S.orders,
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP,
        S.is_in_segment
    );

UPDATE public.meta_table_dependency_watermark
SET
    high_watermark_datetime = new_high_watermark_datetime,
    meta_update_datetime = current_timestamp::timestamp_ltz(3)
WHERE table_name = $target_table;

