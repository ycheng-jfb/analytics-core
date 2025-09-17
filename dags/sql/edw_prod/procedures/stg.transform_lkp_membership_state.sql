SET target_table = 'stg.lkp_membership_state';
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
            FROM stg.lkp_membership_state
        )) AS new_high_watermark_datetime
    FROM (
        SELECT -- For self table
            NULL AS dependent_table_name,
            NULL AS new_high_watermark_datetime
        UNION
        SELECT
            'edw_prod.stg.lkp_membership_event' AS dependent_table_name,
            MAX(meta_update_datetime) AS new_high_watermark_datetime
        FROM stg.lkp_membership_event
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
SET wm_edw_stg_lkp_membership_event = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.lkp_membership_event'));

/*
SELECT
    $wm_self,
    $wm_edw_stg_lkp_membership_event;
*/

--	Customer Base Table
CREATE OR REPLACE TEMP TABLE _lkp_membership_state__base (customer_id INT, meta_original_customer_id INT);

-- Full Refresh
INSERT INTO _lkp_membership_state__base (customer_id, meta_original_customer_id)
SELECT DISTINCT lme.customer_id, lme.meta_original_customer_id
FROM stg.lkp_membership_event AS lme
WHERE $is_full_refresh = TRUE AND
      lme.is_deleted = FALSE
ORDER BY lme.customer_id;

-- Incremental Refresh
INSERT INTO _lkp_membership_state__base (customer_id, meta_original_customer_id)
SELECT DISTINCT m.customer_id, m.meta_original_customer_id
FROM (
    /* Self-check for manual updates */
    SELECT customer_id, meta_original_customer_id
    FROM stg.lkp_membership_state
    WHERE meta_update_datetime > $wm_self
    UNION ALL
    /* Check for dependency table updates */
    SELECT customer_id, meta_original_customer_id
    FROM stg.lkp_membership_event
    WHERE meta_update_datetime > $wm_edw_stg_lkp_membership_event
    ) AS m
WHERE NOT $is_full_refresh
ORDER BY m.customer_id;

CREATE OR REPLACE TEMP TABLE _lkp_membership_state__membership_events AS
SELECT
    lme.customer_id,
    lme.meta_original_customer_id,
    lme.store_id,
    lme.membership_event_type_key,
    lme.membership_event_type,
    lme.membership_type_detail,
    lme.event_local_datetime,
    LAG(lme.membership_event_type) OVER (PARTITION BY lme.customer_id ORDER BY lme.event_local_datetime) AS prior_event,
    LEAD(lme.membership_event_type) OVER (PARTITION BY lme.customer_id ORDER BY lme.event_local_datetime) AS next_event,
    LAG(lme.membership_type_detail) OVER (PARTITION BY lme.customer_id ORDER BY lme.event_local_datetime) AS prior_event_type,
    LEAD(lme.membership_type_detail) OVER (PARTITION BY lme.customer_id ORDER BY lme.event_local_datetime) AS next_event_type,
    LAG(lme.event_local_datetime) OVER (PARTITION BY lme.customer_id ORDER BY lme.event_local_datetime) AS prior_event_local_datetime,
    LEAD(lme.event_local_datetime) OVER (PARTITION BY lme.customer_id ORDER BY lme.event_local_datetime) AS next_event_local_datetime,
    lme.is_deleted,
    FALSE AS is_ignored_activation,
    FALSE AS is_ignored_cancellation,
    FALSE AS is_hard_cancellation_from_ecom
FROM _lkp_membership_state__base AS base
    JOIN stg.lkp_membership_event AS lme
        ON lme.customer_id = base.customer_id
WHERE lme.customer_id <> -1
    AND NOT NVL(lme.is_deleted, FALSE);

-- Ignore Failed Activations and VIP Level Changes and activations that came before registrations
UPDATE _lkp_membership_state__membership_events
SET is_ignored_activation = TRUE
WHERE membership_event_type = 'Activation'
    AND (COALESCE(next_event,'') = 'Failed Activation'
        OR (COALESCE(prior_event,'') = 'Activation'
            AND membership_type_detail = prior_event_type)
        OR prior_event IS NULL);

-- Reclassifying Failed Activations that were missed

-- Commented out on 02/11/2022
-- This code ended up causing problems with many customers whose statuscode was not set back to 'lead' in source
-- after a failed or cancelled activating order.  Because the customer remained as VIP status, there is no other
-- signal that would indicate a new activation attempt upon the next order.  The source issue occurred as part of
-- the rollout when token memberships were added to the system.  The problem seems to be limited to Credit
-- memberships (see DA-17731 for more info).

-- Resetting prior_event and next_event to now reflect the newly classified failed activations
UPDATE _lkp_membership_state__membership_events AS me
SET
    me.prior_event = e.prior_event,
    me.next_event = e.next_event
FROM (
    SELECT
        customer_id,
        membership_event_type,
        event_local_datetime,
        LAG(membership_event_type) OVER (PARTITION BY customer_id ORDER BY event_local_datetime) AS prior_event,
        LEAD(membership_event_type) OVER (PARTITION BY customer_id ORDER BY event_local_datetime) AS next_event
    FROM _lkp_membership_state__membership_events
    ) as e
WHERE me.customer_id = e.customer_id
    AND me.event_local_datetime = e.event_local_datetime
    AND (e.prior_event = 'Failed Activation' OR e.next_event = 'Failed Activation');

-- Ignoring cancellations that occur after a failed activation
UPDATE _lkp_membership_state__membership_events
SET is_ignored_cancellation = TRUE
WHERE membership_event_type = 'Cancellation'
    AND prior_event = 'Failed Activation';

-- Ignoring any leftover activations that occurred after reclassifying failed activation
UPDATE _lkp_membership_state__membership_events
SET is_ignored_activation = TRUE
WHERE membership_event_type = 'Activation'
    AND next_event = 'Failed Activation';

CREATE OR REPLACE TEMP TABLE _failed_activation_specification as
with cte as (
    SELECT
           customer_id,
           membership_event_type,
           membership_type_detail,
           event_local_datetime,
           lag(membership_event_type)over(PARTITION BY customer_id ORDER BY event_local_datetime) AS prior_event,
           lag(membership_type_detail)over(PARTITION BY customer_id ORDER BY event_local_datetime) AS prior_membership_detail,
           row_number() OVER (PARTITION BY customer_id ORDER BY event_local_datetime) -
           row_number() OVER (PARTITION BY customer_id,membership_event_type ORDER BY event_local_datetime) AS rn_group
    FROM _lkp_membership_state__membership_events
    WHERE NOT is_ignored_activation
)

SELECT
    customer_id,
    membership_event_type,
    membership_type_detail,
    event_local_datetime,
    rn_group,
    prior_event,
    prior_membership_detail,
    row_number()OVER (PARTITION BY customer_id,membership_event_type,rn_group order by event_local_datetime) as group_sequence
from cte;

UPDATE _failed_activation_specification AS t
SET
    t.membership_event_type = s.failed_activation_type
FROM (
    SELECT
        customer_id,
        event_local_datetime,
        membership_event_type,
        CASE
            WHEN membership_event_type = 'Failed Activation'
                AND (prior_event = 'Registration' OR prior_event IS NULL OR prior_event = 'Email Signup') THEN 'Failed Activation from Lead'
            WHEN membership_event_type = 'Failed Activation'
                AND (prior_event = 'Cancellation'OR (prior_event = 'Activation' AND prior_membership_detail = 'Monthly')) THEN 'Failed Activation from Cancel'
            WHEN membership_event_type = 'Failed Activation' AND prior_event = 'Guest Purchasing Member' THEN 'Failed Activation from Guest'
            WHEN membership_event_type = 'Failed Activation' AND prior_event = 'Activation' AND prior_membership_detail = 'Classic' THEN 'Failed Activation from Classic VIP'
            WHEN membership_event_type = 'Failed Activation' AND prior_event = 'Free Trial Downgrade' THEN 'Failed Activation from Free Trial Downgrade'
            WHEN membership_event_type = 'Failed Activation' AND prior_event =  'Free Trial Activation' THEN 'Failed Activation from Free Trial Activation'
            ELSE membership_event_type
        END AS failed_activation_type
    FROM _failed_activation_specification
) as s
WHERE
    s.customer_id= t.customer_id
    AND s.event_local_datetime = t.event_local_datetime
    AND t.membership_event_type = 'Failed Activation';

-- setting consecutive failed activations to the membership state prior to the first failed activation
UPDATE _failed_activation_specification as t
SET
    t.membership_event_type = s.membership_event_type
FROM (
    SELECT
        customer_id,
        rn_group,
        membership_event_type
    FROM _failed_activation_specification
    WHERE group_sequence = 1
    AND membership_event_type ilike '%Failed Activation%'
) as s
WHERE
    t.customer_id = s.customer_id
    AND t.rn_group = s.rn_group
    AND t.membership_event_type = 'Failed Activation';

-- updating with new specified failed activation values
UPDATE _lkp_membership_state__membership_events as t
SET t.membership_event_type = s.membership_event_type
FROM _failed_activation_specification AS s
WHERE
    s.customer_id = t.customer_id
    AND s.event_local_datetime = t.event_local_datetime
    AND s.membership_event_type ilike '%Failed Activation%';


-- Identify Hard Cancellations from Ecom Events
UPDATE _lkp_membership_state__membership_events
SET is_hard_cancellation_from_ecom = TRUE
WHERE membership_event_type = 'Guest Purchasing Member'
    AND COALESCE(prior_event,'') = 'Registration'
    AND COALESCE(next_event,'') = 'Cancellation';

CREATE OR REPLACE TEMP TABLE _lkp_membership_state__pre_stg AS
SELECT
    lms.customer_id,
    lms.meta_original_customer_id,
    lms.store_id,
    lms.membership_event_type_key,
    lms.membership_event_type,
    CASE WHEN lms.prior_event = 'Cancellation' AND
        lms.membership_event_type = 'Guest Purchasing Member' THEN 'Previous VIP'
    ELSE lms.membership_type_detail END AS membership_type_detail,
    lms.event_local_datetime,
    CASE
	    WHEN lms.membership_event_type IN ('Registration','Failed Activation from Lead') THEN 'Lead'
        WHEN lms.membership_event_type = 'Deactivated Lead' THEN 'Deactivated Lead'
	    WHEN lms.membership_event_type IN ('Activation','Failed Activation from Classic VIP') THEN 'VIP'
        WHEN lms.membership_event_type = 'Guest Purchasing Member' AND lms.membership_type_detail = 'Unidentified' THEN 'Guest - Unidentified'
        WHEN (lms.membership_event_type = 'Guest Purchasing Member' AND lms.membership_type_detail = 'Previous VIP') OR (lms.prior_event = 'Cancellation' AND lms.membership_event_type = 'Guest Purchasing Member') THEN 'Guest - Previous VIP'
	    WHEN lms.membership_event_type IN ('Guest Purchasing Member','Failed Activation from Guest') THEN 'Guest'
	    WHEN lms.membership_event_type IN ('Cancellation','Failed Activation from Cancel') THEN 'Cancelled'
        WHEN lms.membership_event_type = 'Free Trial Activation' THEN 'Free Trial VIP'
        WHEN lms.membership_event_type IN ('Free Trial Downgrade', 'Failed Activation from Free Trial Downgrade', 'Failed Activation from Free Trial Activation') THEN 'Free Trial Downgrade'
	    WHEN lms.membership_event_type IN ('Email Signup') THEN 'Prospect'
	    ELSE 'Unknown'
	END AS membership_state,
    lms.is_ignored_activation,
    lms.is_ignored_cancellation,
    lms.is_hard_cancellation_from_ecom,
    lms.is_deleted
FROM _lkp_membership_state__membership_events AS lms;

-- Delete the orphans
INSERT INTO _lkp_membership_state__pre_stg
(
    customer_id,
    meta_original_customer_id,
    store_id,
    membership_event_type_key,
    membership_event_type,
    membership_type_detail,
    event_local_datetime,
    membership_state,
    is_ignored_activation,
    is_ignored_cancellation,
    is_hard_cancellation_from_ecom,
    is_deleted
)
SELECT
    lms.customer_id,
    lms.meta_original_customer_id,
    lms.store_id,
    lms.membership_event_type_key,
    lms.membership_event_type,
    lms.membership_type_detail,
    lms.event_local_datetime,
    lms.membership_state,
    lms.is_ignored_activation,
    lms.is_ignored_cancellation,
    lms.is_hard_cancellation_from_ecom,
    TRUE AS is_deleted
FROM stg.lkp_membership_state lms
JOIN _lkp_membership_state__base base
    ON base.customer_id = lms.customer_id
WHERE NOT NVL(lms.is_deleted, FALSE)
    AND NOT EXISTS(
                    SELECT 1
                    FROM _lkp_membership_state__pre_stg as s
                    WHERE s.customer_id = lms.customer_id
                    AND s.event_local_datetime = lms.event_local_datetime
                 );

CREATE OR REPLACE TEMP TABLE _lkp_membership_state__stg AS
SELECT
    customer_id,
    meta_original_customer_id,
    store_id,
    membership_event_type_key,
    membership_event_type,
    membership_type_detail,
    event_local_datetime,
    membership_state,
    is_ignored_activation,
    is_ignored_cancellation,
    is_hard_cancellation_from_ecom,
    is_deleted,
    HASH(
            customer_id,
            meta_original_customer_id,
            store_id,
            membership_event_type_key,
            membership_event_type,
            membership_type_detail,
            event_local_datetime,
            membership_state,
            is_ignored_activation,
            is_ignored_cancellation,
            is_hard_cancellation_from_ecom,
            is_deleted
        ) AS meta_row_hash,
    $execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM _lkp_membership_state__pre_stg
ORDER BY customer_id, event_local_datetime;


MERGE INTO stg.lkp_membership_state AS t
USING _lkp_membership_state__stg AS s
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
            membership_state,
            is_ignored_activation,
            is_ignored_cancellation,
            is_hard_cancellation_from_ecom,
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
            membership_state,
            is_ignored_activation,
            is_ignored_cancellation,
            is_hard_cancellation_from_ecom,
            is_deleted,
            meta_row_hash,
            meta_create_datetime,
            meta_update_datetime
        )
WHEN MATCHED AND t.meta_row_hash != s.meta_row_hash THEN
    UPDATE
    SET
        t.meta_original_customer_id = s.meta_original_customer_id,
        t.store_id = s.store_id,
        t.membership_event_type_key = s.membership_event_type_key,
        t.membership_event_type = s.membership_event_type,
        t.membership_type_detail = s.membership_type_detail,
        t.membership_state = s.membership_state,
        t.is_ignored_activation = s.is_ignored_activation,
        t.is_ignored_cancellation = s.is_ignored_cancellation,
        t.is_hard_cancellation_from_ecom = s.is_hard_cancellation_from_ecom,
        t.is_deleted = s.is_deleted,
        t.meta_row_hash = s.meta_row_hash,
        t.meta_update_datetime = s.meta_update_datetime;

-- update watermark
UPDATE stg.meta_table_dependency_watermark
    SET high_watermark_datetime = IFF(dependent_table_name IS NOT NULL,
                                        new_high_watermark_datetime,
                                        (
                                            SELECT MAX(meta_update_datetime) AS new_high_watermark_datetime
                                            FROM stg.lkp_membership_state
                                        )
                                     ),
        meta_update_datetime = CURRENT_TIMESTAMP()
WHERE table_name = $target_table;
