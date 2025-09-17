SET target_table = 'stg.lkp_membership_event';
SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
SET is_full_refresh = (SELECT IFF(stg.udf_get_watermark($target_table, NULL) = '1900-01-01'::TIMESTAMP_LTZ(3), TRUE, FALSE));

SET warehouse_to_be_used = IFF($is_full_refresh, 'da_wh_adhoc_large', CURRENT_WAREHOUSE());
USE WAREHOUSE IDENTIFIER ($warehouse_to_be_used);

/*
-- Initial load
UPDATE stg.meta_table_dependency_watermark
SET high_watermark_datetime = '1900-01-01',
    meta_update_datetime = CURRENT_TIMESTAMP()
WHERE table_name = $target_table;
SET is_full_refresh = TRUE;
*/

MERGE INTO stg.meta_table_dependency_watermark AS w
USING (
    SELECT
        $target_table AS table_name,
        t.dependent_table_name,
        IFF(t.dependent_table_name IS NOT NULL, t.new_high_watermark_datetime, (
            SELECT MAX(meta_update_datetime) AS new_high_watermark_datetime
            FROM stg.lkp_membership_event
        )) AS new_high_watermark_datetime
    FROM (
        SELECT -- For self table
            NULL AS dependent_table_name,
            NULL AS new_high_watermark_datetime
        UNION
        SELECT
            'edw_prod.stg.lkp_membership_event_type' AS dependent_table_name,
            MAX(meta_update_datetime) AS new_high_watermark_datetime
        FROM stg.lkp_membership_event_type
        ) AS t
    ORDER BY COALESCE(t.dependent_table_name, '')
    ) AS s
    ON w.table_name = s.table_name
    AND w.dependent_table_name IS NOT DISTINCT FROM s.dependent_table_name
WHEN NOT MATCHED THEN
    INSERT (
        table_name,
        dependent_table_name,
        high_watermark_datetime,
        new_high_watermark_datetime
        )
    VALUES (
        s.table_name,
        s.dependent_table_name,
        '1900-01-01', -- current high_watermark_datetime
        s.new_high_watermark_datetime
        )
WHEN MATCHED AND w.new_high_watermark_datetime IS DISTINCT FROM s.new_high_watermark_datetime THEN
    UPDATE
    SET w.new_high_watermark_datetime = s.new_high_watermark_datetime,
        w.meta_update_datetime = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);

-- Use watermark variables for each dependent table to allow pruning of micro-partitions which doesn't happen with UDFs.
SET wm_self = (SELECT stg.udf_get_watermark($target_table, NULL));
SET wm_edw_stg_lkp_membership_event_type = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.lkp_membership_event_type'));

/*
SELECT
    $wm_self,
    $wm_edw_stg_lkp_membership_event_type;
*/

--	Customer Base Table
CREATE OR REPLACE TEMP TABLE _lkp_membership_event__base (customer_id INT, meta_original_customer_id INT);

-- Full Refresh
INSERT INTO _lkp_membership_event__base (customer_id, meta_original_customer_id)
SELECT DISTINCT lmet.customer_id, lmet.meta_original_customer_id
FROM stg.lkp_membership_event_type AS lmet
--WHERE lmet.customer_id = 713068309; -- for testing
WHERE $is_full_refresh = TRUE
ORDER BY lmet.customer_id;

-- Incremental Refresh
INSERT INTO _lkp_membership_event__base (customer_id, meta_original_customer_id)
SELECT DISTINCT m.customer_id, m.meta_original_customer_id
FROM (
    /* Self-check for manual updates */
    SELECT customer_id, meta_original_customer_id
    FROM stg.lkp_membership_event
    WHERE meta_update_datetime > $wm_self
    UNION ALL
    /* Check for dependency table updates */
    SELECT customer_id, meta_original_customer_id
    FROM stg.lkp_membership_event_type
    WHERE meta_update_datetime > $wm_edw_stg_lkp_membership_event_type
    UNION ALL
    /* Previously errored rows */
    SELECT customer_id, meta_original_customer_id
    FROM excp.lkp_membership_event
    WHERE meta_is_current_excp
        AND meta_data_quality = 'error'
    ) AS m
WHERE NOT $is_full_refresh
ORDER BY m.customer_id;

CREATE OR REPLACE TEMP TABLE _lkp_membership_event__rank AS
SELECT
    lmet.customer_id,
    lmet.meta_original_customer_id,
    lmet.store_id,
    dmet.membership_event_type_key,
    lmet.membership_event_type,
    lmet.membership_type_detail,
    lmet.event_local_datetime,
    lmet.is_deleted
FROM _lkp_membership_event__base AS base
    JOIN stg.lkp_membership_event_type AS lmet
        ON lmet.customer_id = base.customer_id
    JOIN stg.dim_membership_event_type AS dmet
        ON LOWER(lmet.membership_event_type) = LOWER(dmet.membership_event_type)
ORDER BY
    lmet.customer_id,
    lmet.event_local_datetime;

INSERT INTO _lkp_membership_event__rank (
    customer_id,
    meta_original_customer_id,
    store_id,
    membership_event_type_key,
    membership_event_type,
    membership_type_detail,
    event_local_datetime,
    is_deleted
    )
SELECT DISTINCT
    s.customer_id,
    s.meta_original_customer_id,
    s.store_id,
    s.membership_event_type_key,
    s.membership_event_type,
    s.membership_type_detail,
    s.event_local_datetime,
    s.is_deleted
FROM excp.lkp_membership_event AS s
    LEFT JOIN _lkp_membership_event__rank AS r
        ON r.customer_id = s.customer_id
        AND r.event_local_datetime = s.event_local_datetime
WHERE r.customer_id IS NULL
    AND s.meta_is_current_excp
    AND s.meta_data_quality = 'error';

CREATE OR REPLACE TEMP TABLE _lkp_membership_event__hist AS
SELECT
    lme.customer_id,
    lme.meta_original_customer_id,
    lme.store_id,
    lme.membership_event_type_key,
    lme.membership_event_type,
    lme.membership_type_detail,
    lme.event_local_datetime,
    lme.is_deleted
FROM stg.lkp_membership_event AS lme
    JOIN (SELECT DISTINCT customer_id FROM _lkp_membership_event__rank) AS c
        ON c.customer_id = lme.customer_id;

-- Creating Mapping Table
CREATE OR REPLACE TEMP TABLE _lkp_membership_event__valid_mappings AS
SELECT '' AS prior_event, 'Registration' AS current_event
UNION SELECT '', 'Email Signup'
UNION SELECT 'Registration', 'Activation'
UNION SELECT 'Email Signup', 'Activation'
UNION SELECT 'Cancellation', 'Activation'
UNION SELECT 'Cancellation', 'Guest Purchasing Member'
UNION SELECT 'Guest Purchasing Member', 'Activation'
UNION SELECT 'Failed Activation', 'Activation'
UNION SELECT 'Activation', 'Activation'
UNION SELECT 'Activation', 'Failed Activation'
UNION SELECT 'Activation', 'Cancellation'
UNION SELECT 'Guest Purchasing Member', 'Cancellation'
UNION SELECT 'Registration', 'Guest Purchasing Member'
UNION SELECT 'Email Signup', 'Guest Purchasing Member'
UNION SELECT 'Failed Activation', 'Guest Purchasing Member'
UNION SELECT 'Registration', 'Free Trial Activation'
UNION SELECT 'Free Trial Activation', 'Activation'
UNION SELECT 'Free Trial Activation', 'Free Trial Downgrade'
UNION SELECT 'Free Trial Downgrade', 'Activation'
UNION SELECT 'Free Trial Downgrade', 'Failed Activation'
UNION SELECT 'Registration', 'Deactivated Lead'
UNION SELECT 'Cancellation', 'Deactivated Lead';

-- Cleanup invalid events
CREATE OR REPLACE TEMP TABLE _lkp_membership_event__invalid_events AS
WITH all_events AS (
    SELECT
        customer_id,
        meta_original_customer_id,
        event_local_datetime,
        membership_event_type,
        is_deleted
    FROM (
        SELECT
            customer_id,
            meta_original_customer_id,
            event_local_datetime,
            membership_event_type,
            is_deleted,
            'curr' AS source
        FROM _lkp_membership_event__rank
        UNION ALL
        SELECT
            customer_id,
            meta_original_customer_id,
            event_local_datetime,
            membership_event_type,
            is_deleted,
            'hist' AS source
        FROM _lkp_membership_event__hist
        ) AS ae
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ae.customer_id, ae.event_local_datetime ORDER BY ae.source) = 1
    ),
prior_events AS (
    SELECT
        customer_id,
        meta_original_customer_id,
        event_local_datetime,
        membership_event_type,
        LAG(membership_event_type) OVER (PARTITION BY customer_id ORDER BY event_local_datetime) as prior_event
    FROM all_events
    WHERE NOT NVL(is_deleted, FALSE)
    )
SELECT
    pe.customer_id,
    pe.meta_original_customer_id,
    pe.event_local_datetime
FROM prior_events AS pe
    LEFT JOIN _lkp_membership_event__valid_mappings AS vm
        ON COALESCE(pe.prior_event, '') = vm.prior_event
        AND pe.membership_event_type = vm.current_event
WHERE vm.current_event IS NULL;

/* MANUALLY UPDATE 'is_deleted' FIELD
-- Remove orphan records not found in stg.lkp_membership_event_type table
UPDATE stg.lkp_membership_event AS lme
SET lme.is_deleted = TRUE
WHERE NOT NVL(lme.is_deleted, FALSE)
    AND NOT EXISTS (
        SELECT 1
        FROM stg.lkp_membership_event_type AS lmet
        WHERE lmet.customer_id = lme.customer_id
            AND lmet.event_local_datetime = lme.event_local_datetime
        );
*/

CREATE OR REPLACE TEMP TABLE _lkp_membership_event__pre_stg AS
SELECT
    h.customer_id,
    h.meta_original_customer_id,
    h.store_id,
    h.membership_event_type_key,
    h.membership_event_type,
    h.membership_type_detail,
    h.event_local_datetime,
    h.is_deleted,
    'hist' AS source
FROM _lkp_membership_event__hist AS h
UNION ALL
SELECT
    r.customer_id,
    r.meta_original_customer_id,
    r.store_id,
    r.membership_event_type_key,
    r.membership_event_type,
    r.membership_type_detail,
    r.event_local_datetime,
    r.is_deleted,
    'curr' AS source
FROM _lkp_membership_event__rank AS r
    LEFT JOIN _lkp_membership_event__invalid_events AS ie
        ON ie.customer_id = r.customer_id
        AND ie.event_local_datetime = r.event_local_datetime
WHERE ie.customer_id IS NULL;
-- SELECT * FROM _lkp_membership_event__pre_stg;

CREATE OR REPLACE TEMP TABLE _lkp_membership_event__stg AS
SELECT
    customer_id,
    meta_original_customer_id,
    store_id,
    membership_event_type_key,
    membership_event_type,
    membership_type_detail,
    event_local_datetime,
    is_deleted
FROM _lkp_membership_event__pre_stg
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id, event_local_datetime
    ORDER BY source, membership_event_type, membership_type_detail) = 1;
-- SELECT * FROM _lkp_membership_event__stg;

-- Delete the orphans
INSERT INTO _lkp_membership_event__stg (
    customer_id,
    meta_original_customer_id,
    store_id,
    membership_event_type_key,
    membership_event_type,
    membership_type_detail,
    event_local_datetime,
    is_deleted
    )
SELECT
    h.customer_id,
    h.meta_original_customer_id,
    h.store_id,
    h.membership_event_type_key,
    h.membership_event_type,
    h.membership_type_detail,
    h.event_local_datetime,
    TRUE AS is_deleted
FROM _lkp_membership_event__hist AS h
WHERE NOT NVL(h.is_deleted, FALSE)
    AND (NOT EXISTS (
            SELECT 1
            FROM _lkp_membership_event__stg AS s
            WHERE s.customer_id = h.customer_id
            AND s.event_local_datetime = h.event_local_datetime
            )
        OR EXISTS (
            SELECT 1
            FROM _lkp_membership_event__invalid_events AS ie
            WHERE ie.customer_id = h.customer_id
            AND ie.event_local_datetime = h.event_local_datetime
            )
        );
-- SELECT COUNT(1) FROM _lkp_membership_event__stg;

-- FINAL OUTPUT - Merge into lkp table
MERGE INTO stg.lkp_membership_event AS t
USING (
    SELECT
        customer_id,
        meta_original_customer_id,
        store_id,
        membership_event_type_key,
        membership_event_type,
        membership_type_detail,
        event_local_datetime,
        is_deleted,
        HASH(
            customer_id,
            meta_original_customer_id,
            store_id,
            membership_event_type_key,
            membership_event_type,
            membership_type_detail,
            event_local_datetime::TIMESTAMP_NTZ,
            is_deleted
            ) AS meta_row_hash,
        $execution_start_time AS meta_create_datetime,
        $execution_start_time AS meta_update_datetime
    FROM _lkp_membership_event__stg
    ORDER BY customer_id, event_local_datetime
    ) AS s
    ON s.customer_id = t.customer_id
    AND s.event_local_datetime = t.event_local_datetime
WHEN NOT MATCHED THEN
    INSERT (
        customer_id,
        meta_original_customer_id,
        store_id,
        membership_event_type_key,
        membership_event_type,
        membership_type_detail,
        event_local_datetime,
        is_deleted,
        meta_row_hash,
        meta_create_datetime,
        meta_update_datetime
        )
    VALUES (
        customer_id,
        meta_original_customer_id,
        store_id,
        membership_event_type_key,
        membership_event_type,
        membership_type_detail,
        event_local_datetime,
        is_deleted,
        meta_row_hash,
        meta_create_datetime,
        meta_update_datetime
        )
WHEN MATCHED AND t.meta_row_hash != s.meta_row_hash THEN
    UPDATE
    SET --t.customer_id = s.customer_id,
        t.meta_original_customer_id = s.meta_original_customer_id,
        t.store_id = s.store_id,
        t.membership_event_type_key = s.membership_event_type_key,
        t.membership_event_type = s.membership_event_type,
        t.membership_type_detail = s.membership_type_detail,
        --t.event_local_datetime = s.event_local_datetime,
        t.is_deleted = s.is_deleted,
        t.meta_row_hash = s.meta_row_hash,
        --t.meta_create_datetime = s.meta_create_datetime,
        t.meta_update_datetime = s.meta_update_datetime;

--  Success
UPDATE stg.meta_table_dependency_watermark
SET high_watermark_datetime = IFF(dependent_table_name IS NOT NULL, new_high_watermark_datetime,
        (SELECT MAX(meta_update_datetime) AS new_high_watermark_datetime FROM stg.lkp_membership_event)),
    meta_update_datetime = CURRENT_TIMESTAMP()
WHERE table_name = $target_table;
-- SELECT * FROM stg.meta_table_dependency_watermark WHERE table_name = $target_table;

UPDATE excp.lkp_membership_event
SET meta_is_current_excp = FALSE
WHERE meta_is_current_excp;

-- Insert invalid events into exception table
MERGE INTO excp.lkp_membership_event AS t
USING (
    SELECT
        d.customer_id,
        d.meta_original_customer_id,
        d.store_id,
        d.membership_event_type_key,
        d.membership_event_type,
        d.membership_type_detail,
        d.event_local_datetime,
        d.is_deleted,
        d.meta_row_hash,
        d.meta_data_quality,
        d.meta_create_datetime,
        d.meta_update_datetime,
        d.excp_message,
        d.meta_is_current_excp
    FROM (
        SELECT
            r.customer_id,
            r.meta_original_customer_id,
            r.store_id,
            r.membership_event_type_key,
            r.membership_event_type,
            r.membership_type_detail,
            r.event_local_datetime,
            r.is_deleted,
            HASH(
                r.customer_id,
                r.meta_original_customer_id,
                r.store_id,
                r.membership_event_type_key,
                r.membership_event_type,
                r.membership_type_detail,
                r.event_local_datetime::TIMESTAMP_NTZ,
                r.is_deleted
                ) AS meta_row_hash,
            'error' AS meta_data_quality,
            $execution_start_time AS meta_create_datetime,
            $execution_start_time AS meta_update_datetime,
            'Invalid events' AS excp_message,
            TRUE AS meta_is_current_excp
        FROM _lkp_membership_event__rank AS r
            JOIN _lkp_membership_event__invalid_events AS ie
                ON ie.customer_id = r.customer_id
                AND ie.event_local_datetime = r.event_local_datetime
         ) AS d
    ORDER BY d.customer_id, d.event_local_datetime
    ) AS s
    ON s.customer_id = t.customer_id
    AND s.event_local_datetime = t.event_local_datetime
WHEN NOT MATCHED THEN
    INSERT (
        customer_id,
        meta_original_customer_id,
        store_id,
        membership_event_type_key,
        membership_event_type,
        membership_type_detail,
        event_local_datetime,
        is_deleted,
        meta_row_hash,
        meta_data_quality,
        meta_create_datetime,
        meta_update_datetime,
        excp_message,
        meta_is_current_excp
        )
    VALUES (
        s.customer_id,
        s.meta_original_customer_id,
        s.store_id,
        s.membership_event_type_key,
        s.membership_event_type,
        s.membership_type_detail,
        s.event_local_datetime,
        s.is_deleted,
        s.meta_row_hash,
        s.meta_data_quality,
        s.meta_create_datetime,
        s.meta_update_datetime,
        s.excp_message,
        s.meta_is_current_excp
        )
WHEN MATCHED AND t.meta_row_hash != s.meta_row_hash THEN
    UPDATE
    SET --t.customer_id = s.customer_id,
        t.meta_original_customer_id = s.meta_original_customer_id,
        t.store_id = s.store_id,
        t.membership_event_type_key = s.membership_event_type_key,
        t.membership_event_type = s.membership_event_type,
        t.membership_type_detail = s.membership_type_detail,
        --t.event_local_datetime = s.event_local_datetime,
        t.is_deleted = s.is_deleted,
        t.meta_row_hash = s.meta_row_hash,
        t.meta_data_quality = s.meta_data_quality,
        --t.meta_create_datetime = s.meta_create_datetime,
        t.meta_update_datetime = s.meta_update_datetime,
        t.excp_message = s.excp_message,
        t.meta_is_current_excp = s.meta_is_current_excp;
