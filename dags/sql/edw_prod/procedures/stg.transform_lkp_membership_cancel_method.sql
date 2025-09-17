SET target_table = 'stg.lkp_membership_cancel_method';
SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
SET is_full_refresh = (SELECT IFF(stg.udf_get_watermark($target_table, NULL) = '1900-01-01'::TIMESTAMP_LTZ(3), TRUE, FALSE));

/*
-- Initial Load / Full Refresh
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
            FROM stg.lkp_membership_cancel_method
        )) AS new_high_watermark_datetime
    FROM (
        SELECT -- For self table
            NULL AS dependent_table_name,
            NULL AS new_high_watermark_datetime
        UNION ALL
        SELECT
            'edw_prod.stg.fact_membership_event' AS dependent_table_name,
            MAX(meta_update_datetime) AS new_high_watermark_datetime
        FROM stg.fact_membership_event
        UNION ALL
        SELECT
            'lake_consolidated.ultra_merchant.case' AS dependent_table_name,
            MAX(meta_update_datetime) AS new_high_watermark_datetime
        FROM lake_consolidated.ultra_merchant.case
        UNION ALL
        SELECT
            'lake_consolidated.ultra_merchant.case_customer' AS dependent_table_name,
            MAX(meta_update_datetime) AS new_high_watermark_datetime
        FROM lake_consolidated.ultra_merchant.case_customer
        UNION ALL
        SELECT
            'lake_consolidated.ultra_merchant.customer_log' AS dependent_table_name,
            MAX(meta_update_datetime) AS new_high_watermark_datetime
        FROM lake_consolidated.ultra_merchant.customer_log
        UNION ALL
        SELECT
            'lake_consolidated.ultra_merchant.fraud_customer' AS dependent_table_name,
            MAX(meta_update_datetime) AS new_high_watermark_datetime
        FROM lake_consolidated.ultra_merchant.fraud_customer
        UNION ALL
        SELECT
            'lake_consolidated.ultra_merchant.membership_downgrade' AS dependent_table_name,
            MAX(meta_update_datetime) AS new_high_watermark_datetime
        FROM lake_consolidated.ultra_merchant.membership_downgrade
        UNION ALL
        SELECT
            'lake_consolidated.ultra_merchant.case_classification' AS dependent_table_name,
            MAX(meta_update_datetime) AS new_high_watermark_datetime
        FROM lake_consolidated.ultra_merchant.case_classification
        UNION ALL
        SELECT
            'lake_consolidated.ultra_merchant.membership_passive_cancels' AS dependent_table_name,
            MAX(meta_update_datetime) AS new_high_watermark_datetime
        FROM lake_consolidated.ultra_merchant.membership_passive_cancels
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
SET wm_edw_stg_fact_membership_event = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.fact_membership_event'));
SET wm_lake_ultra_merchant_case = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.case'));
SET wm_lake_ultra_merchant_case_customer = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.case_customer'));
SET wm_lake_ultra_merchant_customer_log = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.customer_log'));
SET wm_lake_ultra_merchant_fraud_customer = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.fraud_customer'));
SET wm_lake_ultra_merchant_membership_downgrade = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.membership_downgrade'));
SET wm_lake_ultra_merchant_case_classification = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.case_classification'));
SET wm_lake_ultra_merchant_membership_passive_cancels = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.membership_passive_cancels'));

/*
SELECT
    $wm_self,
    $wm_edw_stg_fact_membership_event,
    $wm_lake_ultra_merchant_case,
    $wm_lake_ultra_merchant_case_customer,
    $wm_lake_ultra_merchant_customer_log,
    $wm_lake_ultra_merchant_fraud_customer,
    $wm_lake_ultra_merchant_membership_downgrade,
    $wm_lake_ultra_merchant_case_classification,
    $wm_lake_ultra_merchant_membership_passive_cancels;
*/


--	Order Base Table
CREATE OR REPLACE TEMP TABLE _lkp_membership_cancel_method__base (customer_id INT);

-- Full Refresh
INSERT INTO _lkp_membership_cancel_method__base (customer_id)
SELECT DISTINCT
    fme.customer_id
FROM stg.fact_membership_event AS fme
WHERE NOT NVL(fme.is_deleted, FALSE)
    AND membership_event_type = 'Cancellation'
    AND $is_full_refresh  /* SET is_full_refresh = TRUE; */
ORDER BY fme.customer_id;

-- Incremental Refresh
INSERT INTO _lkp_membership_cancel_method__base (customer_id)
SELECT DISTINCT incr.customer_id
FROM (
    /* updated rows from self table */
    SELECT customer_id
    FROM stg.lkp_membership_cancel_method
    WHERE meta_update_datetime > $wm_self

    UNION ALL

    /* rows from dependent tables */
    SELECT
        fme.customer_id
    FROM stg.fact_membership_event AS fme
    WHERE NOT NVL(fme.is_deleted, FALSE)
        AND membership_event_type = 'Cancellation'
        AND meta_update_datetime > $wm_edw_stg_fact_membership_event

    UNION ALL

    SELECT cc.customer_id
    FROM lake_consolidated.ultra_merchant.case c
    JOIN lake_consolidated.ultra_merchant.case_customer cc
        ON cc.case_id = c.case_id
    WHERE cc.meta_update_datetime > $wm_lake_ultra_merchant_case_customer
        OR c.meta_update_datetime > $wm_lake_ultra_merchant_case

    UNION ALL

    SELECT cl.customer_id
    FROM lake_consolidated.ultra_merchant.customer_log cl
    WHERE cl.meta_update_datetime > $wm_lake_ultra_merchant_customer_log

    UNION ALL

    SELECT fc.customer_id
    FROM lake_consolidated.ultra_merchant.fraud_customer fc
    WHERE fc.meta_update_datetime > $wm_lake_ultra_merchant_fraud_customer

    UNION ALL

     SELECT cu.customer_id
    FROM lake_consolidated.ultra_merchant.case c
    JOIN lake_consolidated.ultra_merchant.case_customer cu
        ON cu.case_id = c.case_id
    JOIN lake_consolidated.ultra_merchant.case_classification cc
        ON cc.case_id = c.meta_original_case_id
    JOIN lake_consolidated.ultra_merchant.case_flag_disposition_case_flag_type cfdc
        ON cfdc.case_flag_type_id = cc.case_flag_type_id
    JOIN lake_consolidated.ultra_merchant.case_flag_disposition cfd
        ON cfd.meta_original_case_flag_disposition_id = cfdc.case_flag_disposition_id
    WHERE cfd.meta_original_case_flag_disposition_id = 9
    AND
        (
            cu.meta_update_datetime > $wm_lake_ultra_merchant_case_customer
            OR c.meta_update_datetime > $wm_lake_ultra_merchant_case
            OR cc.meta_update_datetime > $wm_lake_ultra_merchant_case_classification

        )

    UNION ALL

    SELECT m.customer_id
    FROM lake_consolidated.ultra_merchant.membership_downgrade md
    JOIN lake_consolidated.ultra_merchant.membership_downgrade_reason mdr
        ON md.membership_downgrade_reason_id = mdr.membership_downgrade_reason_id
    JOIN lake_consolidated.ultra_merchant.membership m
        ON md.membership_id = m.membership_id
    WHERE mdr.access = 'online_cancel'
        AND md.meta_update_datetime > $wm_lake_ultra_merchant_membership_downgrade

    UNION ALL

    SELECT m.customer_id
    FROM lake_consolidated.ultra_merchant.membership_passive_cancels mpc
    JOIN lake_consolidated.ultra_merchant.membership m
        ON mpc.membership_id = m.membership_id
    WHERE mpc.meta_update_datetime > $wm_lake_ultra_merchant_membership_passive_cancels

    ) AS incr
WHERE NOT $is_full_refresh
ORDER BY incr.customer_id;
-- SELECT * FROM _lkp_membership_cancel_method__base;

CREATE OR REPLACE TEMP TABLE _lkp_membership_cancel_method__event_base AS
SELECT DISTINCT fme.membership_event_key,
                fme.customer_id,
                fme.meta_original_customer_id,
                fme.event_start_local_datetime AS cancellation_local_datetime,
                CONVERT_TIMEZONE( 'America/Los_Angeles' , fme.event_start_local_datetime)::DATE cancel_date,
                fme.store_id
FROM _lkp_membership_cancel_method__base base
JOIN stg.fact_membership_event fme
    ON base.customer_id = fme.customer_id
WHERE NOT NVL(fme.is_deleted, FALSE)
    AND fme.membership_event_type = 'Cancellation'
ORDER BY fme.customer_id, cancellation_local_datetime;


CREATE OR REPLACE TEMP TABLE _lkp_membership_cancel_method__gms_cases AS
SELECT
    eb.customer_id,
    eb.cancellation_local_datetime,
    eb.cancel_date,
    c.meta_original_case_id,
    cs.label as case_source
FROM _lkp_membership_cancel_method__event_base eb
JOIN lake_consolidated.ultra_merchant.case_customer cc
    ON cc.customer_id = eb.customer_id
JOIN lake_consolidated.ultra_merchant.case c
    ON c.case_id = cc.case_id
    AND c.datetime_added::DATE = eb.cancel_date
JOIN lake_consolidated.ultra_merchant.case_source cs
    ON cs.case_source_id = c.case_source_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY eb.customer_id, eb.cancellation_local_datetime ORDER BY c.datetime_added DESC, cc.datetime_added DESC) = 1;

CREATE OR REPLACE TEMP TABLE _lkp_membership_cancel_method__gms_cancels AS
SELECT
     cl.customer_id
    ,base.cancellation_local_datetime
    ,CASE
        WHEN cl.comment ILIKE 'Membership downgraded to PAYGO. Reason: IVR cancel'
            THEN CONCAT('IVR ', x.case_source)
        WHEN cl.comment IS NOT NULL
            THEN x.case_source
        ELSE 'GMS Other'
    END cancel_channel
FROM _lkp_membership_cancel_method__event_base base
JOIN lake_consolidated.ultra_merchant.customer_log cl
    ON base.customer_id = cl.customer_id
    AND cl.datetime_added::DATE = base.cancel_date
LEFT JOIN _lkp_membership_cancel_method__gms_cases x
    ON x.customer_id = cl.customer_id
    AND x.cancel_date = cl.datetime_added::DATE
WHERE
       cl.comment ILIKE 'Membership downgraded to Guest Checkout. Reason%'
    OR cl.comment ILIKE 'Membership cancellation number%'
    OR cl.comment ILIKE 'Fee membership scheduled for cancel immediately'
    OR cl.comment ILIKE 'Fee membership scheduled for cancel at the end of period%'
    OR cl.comment ILIKE 'Membership has been set to Guest Checkout by%'
    OR cl.comment ILIKE 'Membership downgraded to PAYGO. Reason: IVR cancel'
    OR cl.comment ILIKE 'VIP+ Membership scheduled for downgrade immediately'
    OR cl.comment ILIKE 'VIP+ Membership downgraded to Guest Checkout.%'
QUALIFY ROW_NUMBER() OVER (PARTITION BY cl.customer_id, base.cancellation_local_datetime ORDER BY cl.datetime_added DESC) = 1;


CREATE OR REPLACE TEMP TABLE _lkp_membership_cancel_method__online_cancels AS
SELECT DISTINCT
     base.customer_id
    ,base.cancellation_local_datetime
    ,'Online' AS cancel_channel
FROM lake_consolidated.ultra_merchant.customer_log  cl
join _lkp_membership_cancel_method__event_base base
    ON base.customer_id = cl.customer_id
    AND abs(datediff('day', base.cancel_date, cl.datetime_added::DATE)) <=1
WHERE lower(cl.comment) = 'membership has been set to pay as you go by customer using online cancel.';


CREATE OR REPLACE TEMP TABLE _lkp_membership_cancel_method__passive_cancels AS
SELECT DISTINCT
     base.customer_id
    ,base.cancellation_local_datetime
    ,'Passive' AS cancel_channel
    ,'Passive - unspecific reason' as cancel_reason
FROM lake_consolidated.ultra_merchant.membership_passive_cancels pc
JOIN lake_consolidated.ultra_merchant.membership m
    ON pc.membership_id = m.membership_id
JOIN lake_consolidated.ultra_merchant.period p
    ON p.period_id = pc.curr_period_id
JOIN _lkp_membership_cancel_method__event_base base
    ON base.customer_id = m.customer_id
    AND base.cancel_date BETWEEN p.date_period_Start AND p.date_period_end;

CREATE OR REPLACE TEMP TABLE _lkp_membership_cancel_method__gms_cancel_reason AS
SELECT
    c.customer_id,
    c.cancellation_local_datetime,
    CF.label as cancel_reason
FROM _lkp_membership_cancel_method__gms_cases c
JOIN lake_consolidated.ultra_merchant.case_classification cc
    ON cc.case_id = c.meta_original_case_id
    AND cc.datetime_added::DATE = c.cancel_date
JOIN lake_consolidated.ultra_merchant.case_flag_disposition_case_flag_type cfdc
    ON cfdc.case_flag_type_id = cc.case_flag_type_id
JOIN lake_consolidated.ultra_merchant.case_flag_disposition CFD
    ON cfd.meta_original_case_flag_disposition_id = cfdc.case_flag_disposition_id
JOIN lake_consolidated.ultra_merchant.case_flag CF
    ON cf.case_flag_id = cc.case_flag_id
WHERE
    cfd.case_disposition_type_id = 9 --RETENTION SURVEY
    AND cc.case_flag_type_id IN (7,54,78,102,126,150,174,198,222,246,270,294,318,342,365,388,411,434,457,480,503,526,553,580,607,634,661,688)
QUALIFY ROW_NUMBER() OVER(PARTITION BY c.customer_id, c.cancellation_local_datetime ORDER BY cc.datetime_modified DESC, cf.datetime_added DESC) = 1;


CREATE OR REPLACE TEMP TABLE _lkp_membership_cancel_method__online_cancel_reason AS
SELECT
     base.customer_id
    ,base.cancellation_local_datetime
    ,mdr.label as CANCEL_REASON
FROM _lkp_membership_cancel_method__event_base base
join lake_consolidated.ultra_merchant.membership m
    on m.customer_id = base.customer_id
join lake_consolidated.ultra_merchant.membership_downgrade md
    on m.membership_id = md.membership_id
    and md.datetime_added::DATE = base.cancel_date
JOIN lake_consolidated.ultra_merchant.membership_downgrade_reason mdr
    ON md.membership_downgrade_reason_id = mdr.membership_downgrade_reason_id
WHERE MDR.ACCESS = 'online_cancel'
QUALIFY ROW_NUMBER() OVER(PARTITION BY base.customer_id, base.cancellation_local_datetime ORDER BY md.datetime_added DESC) = 1;


CREATE OR REPLACE TEMP TABLE _lkp_membership_cancel_method__blacklisted AS
SELECT
    distinct base.customer_id,
             base.cancellation_local_datetime
            ,'Blacklisted' AS cancel_channel
            ,'Blacklisted - unspecific reason' as cancel_reason
FROM _lkp_membership_cancel_method__event_base base
JOIN lake_consolidated.ultra_merchant.fraud_customer fc
    ON fc.customer_id = base.customer_id
    AND
    (
           fc.datetime_added::DATE = base.cancel_date
        OR fc.datetime_modified::DATE = base.cancel_date
    );


CREATE OR REPLACE TEMP TABLE _lkp_membership_cancel_method__pre_Stg AS
SELECT
    eb.membership_event_key,
    eb.customer_id,
    eb.meta_original_customer_id,
    eb.cancellation_local_datetime,
    COALESCE(gc.cancel_channel,pc.cancel_channel,oc.cancel_channel,b.cancel_channel) AS cancel_method,
    COALESCE(gcr.cancel_reason, pc.cancel_reason,ocr.cancel_reason,b.cancel_reason) AS cancel_reason
FROM _lkp_membership_cancel_method__event_base eb
LEFT JOIN _lkp_membership_cancel_method__gms_cancels gc
    ON eb.customer_id = gc.customer_id
    AND eb.cancellation_local_datetime = gc.cancellation_local_datetime
LEFT JOIN _lkp_membership_cancel_method__gms_cancel_reason gcr
    ON eb.customer_id = gcr.customer_id
    AND eb.cancellation_local_datetime = gcr.cancellation_local_datetime
LEFT JOIN _lkp_membership_cancel_method__passive_cancels pc
    ON eb.customer_id = pc.customer_id
    AND eb.cancellation_local_datetime = pc.cancellation_local_datetime
LEFT JOIN _lkp_membership_cancel_method__online_cancels oc
    ON eb.customer_id = oc.customer_id
    AND eb.cancellation_local_datetime = oc.cancellation_local_datetime
LEFT JOIN _lkp_membership_cancel_method__online_cancel_reason  ocr
    ON eb.customer_id = ocr.customer_id
    AND eb.cancellation_local_datetime = ocr.cancellation_local_datetime
LEFT JOIN _lkp_membership_cancel_method__blacklisted b
    ON eb.customer_id = b.customer_id
    AND eb.cancellation_local_datetime = b.cancellation_local_datetime
ORDER BY eb.customer_id;

CREATE OR REPLACE TEMP TABLE _lkp_membership_cancel_method__stg AS
SELECT
    membership_event_key,
    customer_id,
    meta_original_customer_id,
    cancellation_local_datetime,
    cancel_method,
    cancel_reason,
    HASH(
        membership_event_key,
        customer_id,
        meta_original_customer_id,
        cancellation_local_datetime,
        cancel_method,
        cancel_reason
    ) AS meta_row_hash,
    $execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM _lkp_membership_cancel_method__pre_stg
WHERE cancel_method is not null or cancel_reason is not null;

MERGE INTO stg.lkp_membership_cancel_method as t
USING _lkp_membership_cancel_method__stg as s
    ON s.customer_id = t.customer_id
    AND s.cancellation_local_datetime = t.cancellation_local_datetime
WHEN NOT MATCHED THEN
    INSERT (
        membership_event_key,
        customer_id,
        meta_original_customer_id,
        cancellation_local_datetime,
        cancel_method,
        cancel_reason,
        meta_row_hash,
        meta_create_datetime,
        meta_update_datetime
        )
    VALUES (
        membership_event_key,
        customer_id,
        meta_original_customer_id,
        cancellation_local_datetime,
        cancel_method,
        cancel_reason,
        meta_row_hash,
        meta_create_datetime,
        meta_update_datetime
        )
WHEN MATCHED AND t.meta_row_hash != s.meta_row_hash THEN
    UPDATE SET
        t.membership_event_key = s.membership_event_key,
        t.meta_original_customer_id = s.meta_original_customer_id,
        t.cancel_method = s.cancel_method,
        t.cancel_reason = s.cancel_reason,
        t.meta_row_hash = s.meta_row_hash,
        t.meta_update_datetime = s.meta_update_datetime;


DELETE FROM stg.lkp_membership_cancel_method AS t
WHERE NOT $is_full_refresh
    AND EXISTS (
        SELECT TRUE AS is_exists
        FROM stg.fact_membership_event AS fme
        WHERE fme.membership_event_key = t.membership_event_key
            AND fme.meta_update_datetime > $wm_edw_stg_fact_membership_event
            AND fme.is_deleted
        );

--  Success
UPDATE stg.meta_table_dependency_watermark
SET high_watermark_datetime = IFF(dependent_table_name IS NOT NULL, new_high_watermark_datetime,
        (SELECT MAX(meta_update_datetime) AS new_high_watermark_datetime FROM stg.lkp_membership_cancel_method)),
    meta_update_datetime = CURRENT_TIMESTAMP()
WHERE table_name = $target_table;
-- SELECT * FROM stg.meta_table_dependency_watermark WHERE table_name = $target_table;
