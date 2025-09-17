SET target_table = 'stg.dim_customer_sailthru_history';
SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
SET is_full_refresh = (SELECT IFF(stg.udf_get_watermark($target_table,NULL) = '1900-01-01'::TIMESTAMP_LTZ(3), TRUE, FALSE));

SET wm_lake_view_sailthru_data_exporter_profile = (SELECT stg.udf_get_watermark($target_table, 'lake_view.sailthru.data_exporter_profile'));

/*
-- Initial Load / Full Refresh
UPDATE stg.meta_table_dependency_watermark
SET high_watermark_datetime = '1900-01-01',
    meta_update_datetime = CURRENT_TIMESTAMP()
WHERE table_name = $target_table;
*/

-- No need to build a base table, since everything is sourced from a single table
CREATE OR REPLACE TEMP TABLE _dim_customer_sailthru_history__data_exporter_profile (
    customer_id INT,
    store_id INT,
    is_opt_out BOOLEAN,
    meta_event_datetime TIMESTAMP_LTZ(3)
    ) CLUSTER BY (customer_id);

INSERT INTO _dim_customer_sailthru_history__data_exporter_profile (
    customer_id,
    store_id,
    is_opt_out,
    meta_event_datetime
    )
SELECT
    dep.customer_id,
    dep.store_id,
    IFF(dep.latest_optout_time > dep.latest_signup_time, TRUE, FALSE) AS is_opt_out,
    dep.effective_date AS meta_event_datetime
FROM (
    SELECT
        TRY_TO_NUMBER(extid)::INT AS customer_id,
        TRY_TO_NUMBER(vars:store_id::STRING)::INT AS store_id,
        signup_time AS latest_signup_time,
        COALESCE(optout_time, bounce_time) AS latest_optout_time,
        HASH (
            customer_id,
            store_id,
            latest_signup_time,
            latest_optout_time
            ) AS meta_row_hash,
        LAG(meta_row_hash) OVER (PARTITION BY customer_id, store_id ORDER BY updated_at) AS prev_meta_row_hash,
        updated_at::TIMESTAMP_LTZ(3) AS effective_date,
        ROW_NUMBER() OVER (PARTITION BY customer_id, store_id ORDER BY updated_at) AS row_num
    FROM lake_view.sailthru.data_exporter_profile
    WHERE meta_update_datetime > $wm_lake_view_sailthru_data_exporter_profile
        AND customer_id IS NOT NULL
        AND store_id IS NOT NULL
    QUALIFY row_num = 1
    ) AS dep
WHERE NOT EQUAL_NULL(dep.prev_meta_row_hash, dep.meta_row_hash);
-- SELECT * FROM _dim_customer_sailthru_history__data_exporter_profile;

CREATE OR REPLACE TEMP TABLE _dim_customer_sailthru_history__customer_base (customer_id INT) CLUSTER BY (customer_id);
INSERT INTO _dim_customer_sailthru_history__customer_base (customer_id)
SELECT DISTINCT customer_id
FROM _dim_customer_sailthru_history__data_exporter_profile;
-- SELECT * FROM _dim_customer_sailthru_history__customer_base;

CREATE OR REPLACE TEMP TABLE _dim_customer_sailthru_history__test_customers AS
SELECT base.customer_id
FROM _dim_customer_sailthru_history__customer_base AS base
    JOIN reference.test_customer AS tc
        ON tc.customer_id = base.customer_id;
-- SELECT * FROM _dim_customer_sailthru_history__test_customers;

CREATE OR REPLACE TEMP TABLE _dim_customer_sailthru_history__data AS
SELECT
    dep.customer_id,
    dep.store_id,
    IFF(ctest.customer_id IS NOT NULL, TRUE, FALSE) AS is_test_customer,
    dep.is_opt_out,
    COALESCE(c.meta_row_is_deleted, FALSE) AS is_deleted,
    dep.meta_event_datetime
FROM _dim_customer_sailthru_history__data_exporter_profile AS dep
    JOIN lake_history.ultra_merchant.customer AS c
        ON c.customer_id = dep.customer_id
        AND c.store_id = dep.store_id
        AND dep.meta_event_datetime BETWEEN c.effective_start_datetime AND c.effective_end_datetime
    LEFT JOIN _dim_customer_sailthru_history__test_customers AS ctest
        ON ctest.customer_id = dep.customer_id;
-- SELECT * FROM _dim_customer_sailthru_history__data;

CREATE OR REPLACE TEMP TABLE _dim_customer_sailthru_history__stg AS
-- Insert unknown record if one is not detected
SELECT
    -1 AS customer_id,
    -1 AS store_id,
    FALSE AS is_test_customer,
    FALSE AS is_opt_out,
    FALSE AS is_deleted,
    '1900-01-01' AS meta_event_datetime,
    $execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_update_datetime
WHERE $is_full_refresh
    OR NOT EXISTS (SELECT 1 FROM stg.dim_customer_sailthru_history WHERE customer_id = -1)
UNION ALL
-- Insert processed data eliminating unchanged consecutive rows
SELECT
    customer_id,
    store_id,
    is_test_customer,
    is_opt_out,
    is_deleted,
    meta_event_datetime,
    $execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM (
    SELECT
        customer_id,
        store_id,
        is_test_customer,
        is_opt_out,
        is_deleted,
        HASH (
            customer_id,
            store_id,
            is_test_customer,
            is_opt_out,
            is_deleted
            ) AS meta_row_hash,
        LAG(meta_row_hash) OVER (PARTITION BY customer_id ORDER BY meta_event_datetime) AS prev_meta_row_hash,
        meta_event_datetime::TIMESTAMP_LTZ(3) AS meta_event_datetime
    FROM _dim_customer_sailthru_history__data
    ) AS data
WHERE NOT EQUAL_NULL(prev_meta_row_hash, meta_row_hash);
-- SELECT * FROM _dim_customer_sailthru_history__stg WHERE customer_id = 671551603;
-- SELECT * FROM _dim_customer_sailthru_history__action_datetime WHERE customer_id = 671551603;
-- SELECT DISTINCT * FROM _dim_customer_sailthru_history__data WHERE customer_id = 671551603;
-- SELECT COUNT(1) FROM _dim_customer_sailthru_history__stg;
-- SELECT customer_id, meta_event_datetime, COUNT(1) FROM _dim_customer_sailthru_history__stg GROUP BY 1, 2 HAVING COUNT(1) > 1;






-- dbsplit concat scripts
-- dim_customer_sailthru_history is a static table. So the below scripts should be run manually and one time only

ALTER TABLE _dim_customer_sailthru_history__stg ADD meta_original_customer_id NUMERIC(38,0);

UPDATE _dim_customer_sailthru_history__stg
SET meta_original_customer_id = _dim_customer_sailthru_history__stg.customer_id;

UPDATE _dim_customer_sailthru_history__stg
SET customer_id = -1;

UPDATE _dim_customer_sailthru_history__stg cs
SET cs.customer_id = c.customer_id
FROM lake_consolidated.ultra_merchant.customer c
WHERE c.meta_original_customer_id = cs.meta_original_customer_id;

/*
SELECT *
FROM stg.dim_customer_sailthru_history
WHERE customer_id = -1;

SELECT *
FROM stg.dim_customer_sailthru_history
WHERE meta_original_customer_id IS NULL;
*/


INSERT INTO stg.dim_customer_sailthru_history_stg (
    customer_id,
    meta_original_customer_id,
    store_id,
    is_test_customer,
    is_opt_out,
    is_deleted,
    meta_event_datetime,
    meta_create_datetime,
    meta_update_datetime
    )
SELECT
    customer_id,
    meta_original_customer_id,
    store_id,
    is_test_customer,
    is_opt_out,
    is_deleted,
    meta_event_datetime,
    meta_create_datetime,
    meta_update_datetime
FROM _dim_customer_sailthru_history__stg
ORDER BY
    customer_id,
    meta_event_datetime;

-- Whenever a full refresh (using '1900-01-01') is performed, we will truncate the existing table.  This is
-- because the Snowflake SCD operator cannot process historical data prior to the current row.  We truncate
-- at the end of the transform to prevent the table from being empty longer than necessary during processing.
DELETE FROM stg.dim_customer_sailthru_history WHERE $is_full_refresh;
