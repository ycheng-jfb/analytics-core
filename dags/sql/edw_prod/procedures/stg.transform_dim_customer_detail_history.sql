SET target_table = 'stg.dim_customer_detail_history';
SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
SET is_full_refresh = (SELECT IFF(stg.udf_get_watermark($target_table, NULL) = '1900-01-01'::TIMESTAMP_LTZ(3), TRUE, FALSE));

-- Switch warehouse if performing a full refresh
SET warehouse_to_be_used = IFF($is_full_refresh, 'da_wh_adhoc_large', current_warehouse());
USE WAREHOUSE IDENTIFIER ($warehouse_to_be_used);


-- Use watermark variables for each dependent table to allow pruning of micro-partitions which doesn't happen with UDFs.
SET wm_self = (SELECT stg.udf_get_watermark($target_table, NULL));
SET wm_lake_history_ultra_merchant_customer = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant_history.customer'));
SET wm_lake_history_ultra_merchant_membership = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant_history.membership'));
SET wm_lake_ultra_merchant_membership_type = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.membership_type'));
SET wm_edw_stg_fact_membership_event = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.fact_membership_event'));
SET wm_edw_stg_dim_customer_sailthru_history = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.dim_customer_sailthru_history'));
SET wm_edw_reference_test_customer = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.reference.test_customer'));
SET wm_lake_history_emarsys_email_subscribes = (SELECT stg.udf_get_watermark($target_table, 'lake_history.emarsys.email_subscribes'));
SET wm_med_db_staging_attentive_attentive_sms= (SELECT stg.udf_get_watermark($target_table, 'med_db_staging.attentive.attentive_sms'));
SET wm_edw_reference_iterable_subscription_log = (SELECT stg.udf_get_watermark($target_table,'edw_prod.reference.iterable_subscription_log'));
/*
SELECT
    $wm_self,
    $wm_lake_history_ultra_merchant_customer,
    $wm_lake_history_ultra_merchant_membership,
    $wm_lake_ultra_merchant_membership_type,
    $wm_edw_stg_fact_membership_event,
    $wm_edw_stg_dim_customer_sailthru_history,
    $wm_edw_reference_test_customer,
    $wm_lake_history_emarsys_email_subscribes,
    $wm_edw_reference_iterable_subscription_log;
*/

CREATE OR REPLACE TEMP TABLE _dim_customer_detail_history__customer_base
(
    customer_id                      INT,
    current_effective_start_datetime TIMESTAMP_LTZ(3)
);

-- Full Refresh
INSERT INTO _dim_customer_detail_history__customer_base (customer_id)
SELECT DISTINCT c.customer_id
FROM lake_consolidated.ultra_merchant_history.customer AS c
WHERE $is_full_refresh = TRUE
ORDER BY c.customer_id;

CREATE OR REPLACE TEMP TABLE _med_db_staging_attentive__attentive_sms_base AS
SELECT
     (CASE
        WHEN CONTAINS(LOWER(att.client_id), 'c:') THEN try_to_numeric(ltrim(split(replace(att.client_id,'E','undefined'), '-')[0], 'c:'))
        WHEN CONTAINS(LOWER(att.client_id), '_') THEN try_to_numeric(ltrim(split(att.client_id, '_')[0], ' '))
        ELSE try_to_numeric(att.client_id)
      END || st.company_id)::INT AS customer_id
 FROM lake.media.attentive_attentive_sms_legacy att
 JOIN lake_view.sharepoint.med_account_mapping_media am
     ON am.source_id = att.company_id
     AND am.source ilike 'attentive'
 JOIN stg.dim_store st
     ON st.store_id = am.store_id
 WHERE NOT $is_full_refresh
    AND att.type IN ('JOIN', 'OPT_OUT')
    AND att.meta_update_datetime > $wm_med_db_staging_attentive_attentive_sms;

-- Incremental Refresh
INSERT INTO _dim_customer_detail_history__customer_base (customer_id)
SELECT DISTINCT incr.customer_id
FROM (
    /* Self-check for manual updates */
    SELECT dcdh.customer_id
    FROM stg.dim_customer_detail_history AS dcdh
    WHERE dcdh.meta_update_datetime > $wm_self
    UNION ALL
    /* Check for dependency table updates */
    SELECT c.customer_id
    FROM lake_consolidated.ultra_merchant_history.customer c
         LEFT JOIN lake_history.emarsys.email_subscribes AS es
                   ON TRY_TO_NUMBER(es.customer_id) = c.meta_original_customer_id
                       AND es.opt_in IS NOT NULL
    WHERE (c.meta_update_datetime > $wm_lake_history_ultra_merchant_customer
        OR es.meta_update_datetime > $wm_lake_history_emarsys_email_subscribes)
    UNION ALL
    SELECT m.customer_id
    FROM lake_consolidated.ultra_merchant_history.membership AS m
         LEFT JOIN lake_consolidated.ultra_merchant.membership_type AS mt
                   ON mt.membership_type_id = m.membership_type_id
    WHERE (m.meta_update_datetime > $wm_lake_history_ultra_merchant_membership
        OR mt.meta_update_datetime > $wm_lake_ultra_merchant_membership_type)
    UNION ALL
    SELECT customer_id
    FROM stg.fact_membership_event
    WHERE meta_update_datetime > $wm_edw_stg_fact_membership_event
    UNION ALL
    SELECT customer_id
    FROM _med_db_staging_attentive__attentive_sms_base
    UNION ALL
    SELECT customer_id
    FROM stg.dim_customer_sailthru_history
    WHERE meta_update_datetime > $wm_edw_stg_dim_customer_sailthru_history
    UNION ALL
    SELECT customer_id
    FROM reference.test_customer
    WHERE meta_update_datetime > $wm_edw_reference_test_customer
    UNION ALL
    SELECT customer_id
    FROM reference.iterable_subscription_log
    WHERE meta_update_datetime > $wm_edw_reference_iterable_subscription_log
    UNION ALL
    /* Previously errored rows */
    SELECT customer_id
    FROM excp.dim_customer_detail_history
    WHERE meta_is_current_excp
        AND meta_data_quality = 'error'
    ) AS incr
WHERE NOT $is_full_refresh
ORDER BY incr.customer_id;
-- SELECT * FROM _dim_customer_detail_history__customer_base;

-- Switch warehouse if the delta is more than medium warehouse can handle
SET warehouse_to_be_used = (SELECT IFF(COUNT(1) > 50000000, 'da_wh_adhoc_large', current_warehouse()) FROM _dim_customer_detail_history__customer_base);
USE WAREHOUSE IDENTIFIER ($warehouse_to_be_used);


UPDATE _dim_customer_detail_history__customer_base AS base
SET base.current_effective_start_datetime = dcdh.effective_start_datetime
FROM (SELECT customer_id, effective_start_datetime FROM stg.dim_customer_detail_history WHERE is_current) AS dcdh
WHERE base.customer_id = dcdh.customer_id
    AND NOT $is_full_refresh;

UPDATE _dim_customer_detail_history__customer_base AS base
SET base.current_effective_start_datetime = '1900-01-01'
WHERE base.current_effective_start_datetime IS NULL;
-- SELECT * FROM _dim_customer_detail_history__customer_base;

-- Only allow a single membership record per customer_id
CREATE OR REPLACE TEMP TABLE _dim_customer_detail_history__invalid_membership AS
SELECT
    m.customer_id,
    m.membership_id
FROM _dim_customer_detail_history__customer_base AS base
    JOIN lake_consolidated.ultra_merchant_history.membership AS m
        ON m.customer_id = base.customer_id
QUALIFY RANK() OVER (PARTITION BY m.customer_id ORDER BY m.membership_id DESC) > 1;
-- SELECT * FROM _dim_customer_detail_history__invalid_membership;

CREATE OR REPLACE TEMP TABLE _dim_customer_detail_history__test_customers AS
SELECT base.customer_id
FROM _dim_customer_detail_history__customer_base AS base
    JOIN reference.test_customer AS tc
        ON tc.customer_id = base.customer_id;
-- SELECT * FROM _dim_customer_detail_history__test_customers;

CREATE OR REPLACE TEMP TABLE _dim_customer_detail_history__emarsys AS
SELECT base.customer_id,
       NOT (es.opt_in) AS is_opt_out,
       effective_start_datetime,
       LEAD(DATEADD(MILLISECOND, -1, effective_start_datetime), 1, '9999-12-31') OVER (PARTITION BY base.customer_id ORDER BY effective_start_datetime) AS effective_end_datetime
FROM _dim_customer_detail_history__customer_base base
         JOIN lake_history.emarsys.email_subscribes AS es
              ON TRY_TO_NUMBER(es.customer_id) = stg.udf_unconcat_brand(base.customer_id)
WHERE es.opt_in IS NOT NULL
    AND effective_start_datetime::DATE < '2024-07-01';

CREATE OR REPLACE TEMP TABLE _dim_customer_detail_history__iterable_output AS
SELECT base.customer_id,
       es.email_mkt_opt_out,
       es.sms_opt_out,
       es.meta_event_datetime AS effective_start_datetime,
       LEAD(DATEADD(MILLISECOND, -1, es.meta_event_datetime), 1, '9999-12-31') OVER (PARTITION BY base.customer_id ORDER BY es.meta_event_datetime) AS effective_end_datetime
FROM _dim_customer_detail_history__customer_base base
         JOIN reference.iterable_subscription_log AS es
              ON es.customer_id = base.customer_id;

CREATE OR REPLACE TEMP TABLE _dim_customer_detail_history__sms_optin_date  AS
SELECT
    customer_id,
    is_opt_out,
    effective_start_datetime,
    LEAD(DATEADD(MILLISECOND, -1, effective_start_datetime), 1, '9999-12-31') OVER (PARTITION BY customer_id ORDER BY effective_start_datetime) AS effective_end_datetime
FROM (
        SELECT
            base.customer_id,
            IFF(att.type = 'OPT_OUT', 1, 0) as is_opt_out,
            try_to_timestamp_ltz(att.timestamp) as effective_start_datetime
        FROM lake.media.attentive_attentive_sms_legacy att
        JOIN lake_view.sharepoint.med_account_mapping_media am
            ON am.source_id = att.company_id
            AND am.source ilike 'attentive'
        JOIN stg.dim_store st
            ON st.store_id = am.store_id
        JOIN _dim_customer_detail_history__customer_base AS base
            ON base.customer_id = (CASE
                                   WHEN CONTAINS(LOWER(att.client_id), 'c:') THEN try_to_numeric(ltrim(split(replace(att.client_id,'E','undefined'), '-')[0], 'c:'))
                                   WHEN CONTAINS(LOWER(att.client_id), '_') THEN try_to_numeric(ltrim(split(att.client_id, '_')[0], ' '))
                                   ELSE try_to_numeric(att.client_id)
                                 END || st.company_id)::INT
        WHERE att.type IN ('JOIN', 'OPT_OUT')
        QUALIFY ROW_NUMBER() OVER(PARTITION BY base.customer_id, try_to_timestamp_ltz(att.TIMESTAMP) ORDER BY IFF(att.type = 'OPT_OUT', 1, 0)  DESC) = 1
);


-- Action Dates (base table combining effective_start_dates of all hist tables)
CREATE OR REPLACE TEMP TABLE _dim_customer_detail_history__action_datetime AS
SELECT DISTINCT
    customer_id,
    effective_start_datetime::TIMESTAMP_LTZ(3) AS action_datetime
FROM (
    SELECT
        base.customer_id,
        c.effective_start_datetime
    FROM _dim_customer_detail_history__customer_base AS base
        JOIN lake_consolidated.ultra_merchant_history.customer AS c
            ON c.customer_id = base.customer_id
    WHERE NOT c.effective_end_datetime < base.current_effective_start_datetime
    UNION ALL
    SELECT
        base.customer_id,
        m.effective_start_datetime
    FROM _dim_customer_detail_history__customer_base AS base
        JOIN lake_consolidated.ultra_merchant_history.membership AS m
            ON m.customer_id = base.customer_id
            AND m.membership_id NOT IN (SELECT membership_id FROM _dim_customer_detail_history__invalid_membership)
    WHERE NOT m.effective_end_datetime < base.current_effective_start_datetime
    UNION ALL
    SELECT
        base.customer_id,
        mt.meta_update_datetime AS effective_start_datetime
    FROM _dim_customer_detail_history__customer_base AS base
        JOIN lake_consolidated.ultra_merchant.membership AS m /* OK to use current data for JOIN */
            ON m.customer_id = base.customer_id
        JOIN lake_consolidated.ultra_merchant.membership_type AS mt
            ON mt.membership_type_id = m.membership_type_id
    WHERE mt.meta_update_datetime > base.current_effective_start_datetime
    UNION ALL
    SELECT
        base.customer_id,
        fme.event_start_local_datetime::TIMESTAMP_LTZ(3) AS effective_start_datetime
    FROM _dim_customer_detail_history__customer_base AS base
        JOIN stg.fact_membership_event AS fme
            ON fme.customer_id = base.customer_id
    WHERE NOT fme.event_end_local_datetime::TIMESTAMP_LTZ(3) < base.current_effective_start_datetime
        AND NOT fme.is_deleted
    UNION ALL
    SELECT
        base.customer_id,
        dcsh.effective_start_datetime
    FROM _dim_customer_detail_history__customer_base AS base
        JOIN stg.dim_customer_sailthru_history AS dcsh
            ON dcsh.customer_id = base.customer_id
    WHERE NOT dcsh.effective_end_datetime < base.current_effective_start_datetime
        AND dcsh.effective_start_datetime < '2022-01-01'
    UNION ALL
    SELECT
        base.customer_id,
        e.effective_start_datetime
    FROM _dim_customer_detail_history__customer_base AS base
        JOIN _dim_customer_detail_history__emarsys AS e
            ON e.customer_id = base.customer_id
    WHERE NOT e.effective_end_datetime < base.current_effective_start_datetime
    UNION ALL
    SELECT base.customer_id,
           dcdhio.effective_start_datetime
    FROM _dim_customer_detail_history__customer_base AS base
        JOIN _dim_customer_detail_history__iterable_output dcdhio
            ON base.customer_id = dcdhio.customer_id
    WHERE NOT dcdhio.effective_end_datetime < base.current_effective_start_datetime
    UNION ALL
    SELECT
        base.customer_id,
        e.effective_start_datetime
    FROM _dim_customer_detail_history__customer_base AS base
        JOIN _dim_customer_detail_history__sms_optin_date AS e
            ON e.customer_id = base.customer_id
    WHERE NOT e.effective_end_datetime < base.current_effective_start_datetime
    ) AS dt;
-- SELECT * FROM _dim_customer_detail_history__action_datetime;

CREATE OR REPLACE TEMP TABLE _dim_customer_detail_history__data AS
SELECT
    c.customer_id,
    c.meta_original_customer_id,
    COALESCE(m.membership_id, -1) AS membership_id,
    COALESCE(fme.membership_event_type, 'Unknown') AS membership_event_type,
    COALESCE(fme.membership_type_detail, 'Unknown') AS membership_type_detail,
    COALESCE(m.membership_plan_id, -1) AS membership_plan_id,
    COALESCE(m.price, 0.00) AS membership_price,
    COALESCE(mt.label, 'Unknown') AS membership_type,
    IFF(ctest.customer_id IS NOT NULL, TRUE, FALSE) AS is_test_customer,
	COALESCE(io.email_mkt_opt_out, e.is_opt_out, dcsh.is_opt_out, FALSE) AS is_opt_out,
	COALESCE(io.sms_opt_out, so.is_opt_out, FALSE) AS is_sms_opt_out,
    FALSE AS is_deleted,
    base.action_datetime AS meta_event_datetime
FROM _dim_customer_detail_history__action_datetime AS base
    JOIN lake_consolidated.ultra_merchant_history.customer AS c
        ON c.customer_id = base.customer_id
        AND base.action_datetime BETWEEN c.effective_start_datetime AND c.effective_end_datetime
    LEFT JOIN lake_consolidated.ultra_merchant_history.membership AS m
        ON m.customer_id = base.customer_id
        AND base.action_datetime BETWEEN m.effective_start_datetime AND m.effective_end_datetime
        AND m.membership_id NOT IN (SELECT membership_id FROM _dim_customer_detail_history__invalid_membership)
    LEFT JOIN lake_consolidated.ultra_merchant.membership_type AS mt
        ON mt.membership_type_id = m.membership_type_id
    LEFT JOIN stg.fact_membership_event AS fme
        ON fme.customer_id = base.customer_id
        AND base.action_datetime BETWEEN fme.event_start_local_datetime::TIMESTAMP_LTZ(3) AND fme.event_end_local_datetime::TIMESTAMP_LTZ(3)
        AND NOT fme.is_deleted
    LEFT JOIN stg.dim_customer_sailthru_history AS dcsh
        ON dcsh.customer_id = base.customer_id
        AND dcsh.store_id = c.store_id
        AND base.action_datetime BETWEEN dcsh.effective_start_datetime AND dcsh.effective_end_datetime
    LEFT JOIN _dim_customer_detail_history__emarsys AS e
        ON e.customer_id = base.customer_id
        AND base.action_datetime BETWEEN e.effective_start_datetime AND e.effective_end_datetime
    LEFT JOIN _dim_customer_detail_history__iterable_output as io
        ON base.customer_id = io.customer_id
        AND base.action_datetime BETWEEN io.effective_start_datetime AND io.effective_end_datetime
    LEFT JOIN _dim_customer_detail_history__test_customers AS ctest
        ON ctest.customer_id = base.customer_id
    LEFT JOIN _dim_customer_detail_history__sms_optin_date AS so
        ON so.customer_id = base.customer_id
        AND base.action_datetime BETWEEN so.effective_start_datetime AND so.effective_end_datetime
    LEFT JOIN reference.store_timezone AS stz
        ON stz.store_id = c.store_id;
-- SELECT * FROM _dim_customer_detail_history__data;

CREATE OR REPLACE TEMP TABLE _dim_customer_detail_history__stg AS
/*
-- Delete history records with no matching values in underlying table (Only performed during full refresh)
SELECT
    dcdh.customer_id,
    dcdh.membership_id,
    dcdh.membership_event_type,
    dcdh.membership_type_detail,
    dcdh.membership_plan_id,
    dcdh.membership_price,
    dcdh.membership_type,
    dcdh.is_test_customer,
    dcdh.is_opt_out,
    TRUE AS is_deleted,
    dcdh.meta_event_datetime,
    dcdh.meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM stg.dim_customer_detail_history AS dcdh
    JOIN _dim_customer_detail_history__customer_base AS base
        ON base.customer_id = dcdh.customer_id
WHERE $is_full_refresh
    AND dcdh.is_current
    AND NOT dcdh.is_deleted
    AND NOT EXISTS (
        SELECT 1
        FROM _dim_customer_detail_history__data AS data
        WHERE data.customer_id = dcdh.customer_id
            AND data.meta_event_datetime = dcdh.meta_event_datetime
        )
UNION ALL
*/
-- Insert unknown record if one is not detected
SELECT
    -1 AS customer_id,
    -1 as meta_original_customer_id,
    -1 AS membership_id,
    'Unknown' AS membership_event_type,
    'Unknown' AS membership_type_detail,
    -1 AS membership_plan_id,
    0.00 AS membership_price,
    'Unknown' AS membership_type,
    FALSE AS is_test_customer,
    FALSE AS is_opt_out,
    FALSE AS is_sms_opt_out,
    FALSE AS is_deleted,
    '1900-01-01' AS meta_event_datetime,
    $execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_update_datetime
WHERE $is_full_refresh
    OR NOT EXISTS (SELECT 1 FROM stg.dim_customer_detail_history WHERE customer_id = -1)
UNION ALL
-- Insert processed data eliminating unchanged consecutive rows
SELECT
    customer_id,
    meta_original_customer_id,
    membership_id,
    membership_event_type,
    membership_type_detail,
    membership_plan_id,
    membership_price,
    membership_type,
    is_test_customer,
    is_opt_out,
    is_sms_opt_out,
    is_deleted,
    meta_event_datetime,
    $execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM (
    SELECT
        customer_id,
        meta_original_customer_id,
        membership_id,
        membership_event_type,
        membership_type_detail,
        membership_plan_id,
        membership_price,
        membership_type,
        is_test_customer,
        is_opt_out,
        is_sms_opt_out,
        is_deleted,
        HASH (
            customer_id,
            meta_original_customer_id,
            membership_id,
            membership_event_type,
            membership_type_detail,
            membership_plan_id,
            membership_price,
            membership_type,
            is_test_customer,
            is_opt_out,
            is_sms_opt_out,
            is_deleted
            ) AS meta_row_hash,
        LAG(meta_row_hash) OVER (PARTITION BY customer_id ORDER BY meta_event_datetime) AS prev_meta_row_hash,
        meta_event_datetime::TIMESTAMP_LTZ(3) AS meta_event_datetime
    FROM _dim_customer_detail_history__data
    ) AS data
WHERE NOT EQUAL_NULL(prev_meta_row_hash, meta_row_hash);
-- SELECT * FROM _dim_customer_detail_history__stg WHERE customer_id = 671551603;
-- SELECT * FROM _dim_customer_detail_history__action_datetime WHERE customer_id = 671551603;
-- SELECT DISTINCT * FROM _dim_customer_detail_history__data WHERE customer_id = 671551603;
-- SELECT COUNT(1) FROM _dim_customer_detail_history__stg;
-- SELECT customer_id, meta_event_datetime, COUNT(1) FROM _dim_customer_detail_history__stg GROUP BY 1, 2 HAVING COUNT(1) > 1;

INSERT INTO stg.dim_customer_detail_history_stg (
    customer_id,
    meta_original_customer_id,
    membership_id,
    membership_event_type,
    membership_type_detail,
    membership_plan_id,
    membership_price,
    membership_type,
    is_test_customer,
    is_opt_out,
    is_sms_opt_out,
    is_deleted,
    meta_event_datetime,
    meta_create_datetime,
    meta_update_datetime
    )
SELECT
    customer_id,
    meta_original_customer_id,
    membership_id,
    membership_event_type,
    membership_type_detail,
    membership_plan_id,
    membership_price,
    membership_type,
    is_test_customer,
    is_opt_out,
    is_sms_opt_out,
    is_deleted,
    meta_event_datetime,
    meta_create_datetime,
    meta_update_datetime
FROM _dim_customer_detail_history__stg
ORDER BY
    customer_id,
    meta_event_datetime;

-- Whenever a full refresh (using '1900-01-01') is performed, we will truncate the existing table.  This is
-- because the Snowflake SCD operator cannot process historical data prior to the current row.  We truncate
-- at the end of the transform to prevent the table from being empty longer than necessary during processing.
DELETE FROM stg.dim_customer_detail_history WHERE $is_full_refresh = TRUE;
