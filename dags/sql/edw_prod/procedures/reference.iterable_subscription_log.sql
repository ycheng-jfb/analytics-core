SET target_table = 'reference.iterable_subscription_log';
SET execution_start_time = CURRENT_TIMESTAMP()::TIMESTAMP_LTZ(3);


/*
MERGE INTO stg.meta_table_dependency_watermark AS t
    USING (SELECT $target_table                               AS table_name,
                  NULLIF(dependent_table_name, $target_table) AS dependent_table_name,
                  high_watermark_datetime                     AS new_high_watermark_datetime
           FROM (SELECT 'campaign_event_data.org_3223.users'   AS dependent_table_name,
                        MAX(profile_updated_at)::TIMESTAMP_LTZ AS high_watermark_datetime
                 FROM campaign_event_data.org_3223.users
                 UNION ALL
                 SELECT 'CAMPAIGN_EVENT_DATA.ORG_3223.MESSAGE_TYPES' AS dependent_table_name,
                        MAX(created_at)::TIMESTAMP_LTZ               AS high_watermark_datetime
                 FROM campaign_event_data.org_3223.message_types
                 UNION ALL
                 SELECT 'campaign_event_data.org_3223.projects' AS dependent_table_name,
                        MAX(created_at)::TIMESTAMP_LTZ          AS high_watermark_datetime
                 FROM campaign_event_data.org_3223.projects) AS h) AS s
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
        VALUES (s.table_name,
                s.dependent_table_name,
                '1900-01-01'::timestamp_ltz,
                s.new_high_watermark_datetime);
*/

SET is_full_refresh = TRUE;
-- (SELECT IFF(stg.udf_get_watermark($target_table, NULL) = '1900-01-01'::TIMESTAMP_LTZ(3), TRUE, FALSE));

/*
SET wm_campaign_event_data_org_3223_users = (SELECT stg.udf_get_watermark($target_table, 'campaign_event_data.org_3223.users'));
SET wm_campaign_event_data_org_3223_message_types = (SELECT stg.udf_get_watermark($target_table, 'CAMPAIGN_EVENT_DATA.ORG_3223.MESSAGE_TYPES'));
SET wm_campaign_event_data_org_3223_projects = (SELECT stg.udf_get_watermark($target_table, 'campaign_event_data.org_3223.projects'));
*/



CREATE OR REPLACE TEMP TABLE _itbl_subs_log__base
(
    user_id NUMBER(38, 0),
    customer_id NUMBER(38,0)
);


INSERT INTO _itbl_subs_log__base
SELECT DISTINCT TRY_TO_NUMBER(REPLACE(user_id, '"', '')) AS user_id,
                CASE
                    WHEN p.name ILIKE '%fabletics%' THEN CONCAT(TRY_TO_NUMBER(REPLACE(u.user_id, '"', '')), 20)
                    WHEN p.name ILIKE '%savage%' THEN CONCAT(TRY_TO_NUMBER(REPLACE(u.user_id, '"', '')), 30)
                    WHEN p.name ILIKE ANY ('%shoedazzle%', '%justfab%', '%fabkids%')
                        THEN CONCAT(TRY_TO_NUMBER(REPLACE(u.user_id, '"', '')), 10)
                    END::NUMERIC(38, 0)                  AS customer_id
FROM campaign_event_data.org_3223.users u
         LEFT JOIN campaign_event_data.org_3223.projects p
             ON u.project_id = p.id
WHERE $is_full_refresh;


/*
INSERT INTO _itbl_subs_log__base
SELECT DISTINCT user_id
FROM (SELECT TRY_TO_NUMBER(REPLACE(ui.user_id, '"', '')) AS user_id
      FROM campaign_event_data.org_3223.users ui
          ,LATERAL FLATTEN(INPUT => CAST(ui.subscribed_message_type_ids AS variant)) u
                JOIN campaign_event_data.org_3223.message_types mt ON mt.id = u.value
               JOIN campaign_event_data.org_3223.projects p ON p.id = mt.project_id
      WHERE p.name ILIKE 'PRD'
      AND (ui.profile_updated_at::TIMESTAMP_LTZ > $wm_campaign_event_data_org_3223_users
          OR mt.created_at::TIMESTAMP_LTZ > $wm_campaign_event_data_org_3223_message_types
          OR p.created_at::TIMESTAMP_LTZ > $wm_campaign_event_data_org_3223_projects)

      UNION ALL

      SELECT TRY_TO_NUMBER(REPLACE(ui.user_id, '"', '')) AS user_id
      FROM campaign_event_data.org_3223.users ui
           ,LATERAL FLATTEN(INPUT => CAST(unsubscribed_message_type_ids AS variant)) u
               ,LATERAL FLATTEN(INPUT => CAST(unsubscribed_channel_ids AS variant)) u3
                JOIN campaign_event_data.org_3223.message_types mt ON mt.id = u.value
               JOIN campaign_event_data.org_3223.projects p ON p.id = mt.project_id
      WHERE p.name ILIKE 'PRD'
      AND (ui.profile_updated_at::TIMESTAMP_LTZ > $wm_campaign_event_data_org_3223_users
          OR mt.created_at::TIMESTAMP_LTZ > $wm_campaign_event_data_org_3223_message_types
          OR p.created_at::TIMESTAMP_LTZ > $wm_campaign_event_data_org_3223_projects)

            UNION ALL

      SELECT TRY_TO_NUMBER(REPLACE(ui.user_id, '"', '')) AS user_id
      FROM campaign_event_data.org_3223.users ui
               ,LATERAL FLATTEN(INPUT => CAST(unsubscribed_channel_ids AS variant)) u
                JOIN campaign_event_data.org_3223.message_types mt ON mt.id = u.value
               JOIN campaign_event_data.org_3223.projects p ON p.id = mt.project_id
      WHERE p.name ILIKE 'PRD'
      AND (ui.profile_updated_at::TIMESTAMP_LTZ > $wm_campaign_event_data_org_3223_users
          OR mt.created_at::TIMESTAMP_LTZ > $wm_campaign_event_data_org_3223_message_types
          OR p.created_at::TIMESTAMP_LTZ > $wm_campaign_event_data_org_3223_projects))
WHERE NOT $is_full_refresh;
*/


CREATE OR REPLACE TEMP TABLE _channel_opted_in AS
SELECT DISTINCT user_id                                                                 AS meta_original_customer_id,
                customer_id,
                IFF(CONTAINS(subbed_message_types, 'SMS Marketing'), TRUE, FALSE)       AS sms_opted_in,
                IFF(CONTAINS(subbed_message_types, 'Email Marketing'), TRUE, FALSE)     AS email_mkt_opted_in,
                IFF(CONTAINS(subbed_message_types, 'Email Transactional'), TRUE, FALSE) AS email_trg_opted_in
FROM (SELECT DISTINCT b.user_id,
                      b.customer_id,
                      LISTAGG(mt.name) AS subbed_message_types
      FROM _itbl_subs_log__base b
               JOIN campaign_event_data.org_3223.users ub ON TRY_TO_NUMBER(b.user_id) = TRY_TO_NUMBER(ub.user_id),
           LATERAL FLATTEN(INPUT => CAST(ub.subscribed_message_type_ids AS variant)) u
               JOIN campaign_event_data.org_3223.message_types mt ON mt.id = u.value
               JOIN campaign_event_data.org_3223.projects p ON p.id = mt.project_id
      WHERE p.name ILIKE 'PRD%'
        AND b.customer_id IS NOT NULL
      GROUP BY ALL);


CREATE OR REPLACE TEMP TABLE _opted_out_message_type AS
SELECT DISTINCT user_id                                                                   AS meta_original_customer_id,
                customer_id,
                IFF(CONTAINS(unsubbed_message_types, 'SMS'), TRUE, FALSE)                 AS sms_opted_out,
                IFF(CONTAINS(unsubbed_message_types, 'Email Marketing'), TRUE, FALSE)     AS email_mkt_opted_out,
                IFF(CONTAINS(unsubbed_message_types, 'Email Transactional'), TRUE, FALSE) AS email_trg_opted_out
FROM (SELECT DISTINCT b.user_id,
                      b.customer_id,
                      LISTAGG(mt.name) AS unsubbed_message_types
      FROM _itbl_subs_log__base b
               JOIN campaign_event_data.org_3223.users ub ON TRY_TO_NUMBER(b.user_id) = TRY_TO_NUMBER(ub.user_id),
           LATERAL FLATTEN(INPUT => CAST(unsubscribed_message_type_ids AS variant)) u
               JOIN campaign_event_data.org_3223.message_types mt ON mt.id = u.value::INT
               JOIN campaign_event_data.org_3223.projects p ON p.id = mt.project_id
      WHERE p.name ILIKE 'PRD%'
        AND b.customer_id IS NOT NULL
      GROUP BY ALL);


CREATE OR REPLACE TEMP TABLE _opted_out_channel_type AS
SELECT DISTINCT user_id                                                                   AS meta_original_customer_id,
                customer_id,
                IFF(CONTAINS(unsubbed_message_types, 'SMS'), TRUE, FALSE)                 AS sms_opted_out,
                IFF(CONTAINS(unsubbed_message_types, 'Email Marketing'), TRUE, FALSE)     AS email_mkt_opted_out,
                IFF(CONTAINS(unsubbed_message_types, 'Email Transactional'), TRUE, FALSE) AS email_trg_opted_out
FROM (SELECT DISTINCT b.user_id,
                      b.customer_id,
                      LISTAGG(c.name) AS unsubbed_message_types
      FROM _itbl_subs_log__base b
               JOIN campaign_event_data.org_3223.users ub ON TRY_TO_NUMBER(b.user_id) = TRY_TO_NUMBER(ub.user_id),
           LATERAL FLATTEN(INPUT => CAST(unsubscribed_channel_ids AS variant)) u
               JOIN campaign_event_data.org_3223.channels c ON c.id = u.value::INT
               JOIN campaign_event_data.org_3223.projects p ON p.id = c.project_id
      WHERE p.name ILIKE 'PRD%'
        AND b.customer_id IS NOT NULL
      GROUP BY ALL);


CREATE OR REPLACE TEMP TABLE _itbl_subs_log__stg_base AS
SELECT DISTINCT b.user_id  AS meta_original_customer_id,
                b.customer_id,
                IFF(ct.sms_opted_out OR mt.sms_opted_out OR NOT (sms.sms_opted_in), TRUE,
                    FALSE) AS sms_opted_out,       --for sms, look for opt outs first, then overlay opted in status(double opt in) only to consider opt out = false.
                IFF(ct.email_mkt_opted_out OR mt.email_mkt_opted_out OR NOT (sms.email_mkt_opted_in), TRUE,
                    FALSE) AS email_mkt_opted_out, -- for email, look for opt outs, otherwise consider opted in
                IFF(ct.email_trg_opted_out OR mt.email_trg_opted_out OR NOT (sms.email_trg_opted_in), TRUE,
                    FALSE) AS email_trg_opted_out
FROM _itbl_subs_log__base b
         LEFT JOIN _opted_out_message_type mt ON mt.customer_id = b.customer_id
         LEFT JOIN _opted_out_channel_type ct ON ct.customer_id = b.customer_id
         LEFT JOIN _channel_opted_in sms ON sms.customer_id = b.customer_id
WHERE b.customer_id IS NOT NULL;


CREATE OR REPLACE TEMP TABLE _itbl_subs_log__stg AS
SELECT customer_id,
       $execution_start_time AS meta_event_datetime,
       sms_opted_out,
       email_mkt_opted_out,
       email_trg_opted_out,
       FALSE                 AS is_deleted,
       $execution_start_time AS meta_create_datetime,
       $execution_start_time AS meta_update_datetime
FROM _itbl_subs_log__stg_base
WHERE customer_id IS NOT NULL;


CREATE OR REPLACE TEMP TABLE _itbl_subs_log_delta AS
SELECT s.*,
    CASE
        WHEN s.is_new_data = TRUE AND first_record_rn = 1 THEN 'new'
        WHEN s.effective_start_datetime <= s.lag_effective_start_datetime THEN 'ignore'
        WHEN s.meta_type_1_hash != s.lag_meta_type_1_hash
            AND s.meta_type_2_hash != s.lag_meta_type_2_hash THEN 'both'
        WHEN s.meta_type_1_hash != s.lag_meta_type_1_hash THEN 'type 1'
        WHEN s.meta_type_2_hash != s.lag_meta_type_2_hash THEN 'type 2'
        ELSE 'ignore'
    END :: VARCHAR(10) AS meta_record_status
FROM (
        SELECT
            s.*,
            nvl(lag(s.meta_type_1_hash) OVER (PARTITION BY s.customer_id ORDER BY s.effective_start_datetime), t.meta_type_1_hash) AS lag_meta_type_1_hash,
            nvl(lag(s.meta_type_2_hash) OVER (PARTITION BY s.customer_id ORDER BY s.effective_start_datetime), t.meta_type_2_hash) AS lag_meta_type_2_hash,
            nvl(lag(s.effective_start_datetime) OVER (PARTITION BY s.customer_id ORDER BY s.effective_start_datetime), t.effective_start_datetime) AS lag_effective_start_datetime,
            nvl2(t.is_current, FALSE, TRUE) AS is_new_data,
            row_number() OVER (PARTITION BY s.customer_id ORDER BY s.effective_start_datetime) AS first_record_rn
        FROM
            (
                SELECT
                    s.customer_id,
                    s.meta_event_datetime AS effective_start_datetime,
                    s.meta_event_datetime,
                    s.sms_opted_out,
                    s.email_mkt_opted_out,
                    s.email_trg_opted_out,
                    s.is_deleted,
                    hash(NULL) AS meta_type_1_hash,
                    hash(s.customer_id,s.sms_opted_out,s.email_mkt_opted_out, s.email_trg_opted_out,s.is_deleted) AS meta_type_2_hash,
                    s.meta_create_datetime,
                    s.meta_update_datetime
                FROM _itbl_subs_log__stg s
            )s
        LEFT JOIN reference.iterable_subscription_log t ON equal_null(t.customer_id, s.customer_id)
            AND t.is_current
        WHERE
            t.is_current IS NULL
            OR t.effective_start_datetime < s.effective_start_datetime
    )s;

CREATE OR REPLACE TEMP TABLE _type_2_delta AS
SELECT
    *,
    dateadd(ms,-1,LEAD(effective_start_datetime) OVER (PARTITION BY customer_id ORDER BY effective_start_datetime)) AS effective_end_datetime,
    row_number() OVER (PARTITION BY customer_id ORDER BY effective_start_datetime) AS event_seq_num
FROM _itbl_subs_log_delta
WHERE meta_record_status IN ('type 2', 'both', 'new');

UPDATE reference.iterable_subscription_log t
SET t.effective_end_datetime = dateadd(ms,-1,s.effective_start_datetime),
    t.is_current = FALSE,
    t.meta_update_datetime = s.meta_update_datetime
FROM _type_2_delta s
WHERE t.is_current
    AND s.event_seq_num = 1
    AND equal_null(t.customer_id, s.customer_id)
    AND s.meta_record_status IN ('both', 'type 2');

INSERT INTO reference.iterable_subscription_log
(
    customer_id,
    sms_opt_out,
    email_mkt_opt_out,
    email_trg_opt_out,
    meta_event_datetime,
    effective_start_datetime,
    effective_end_datetime,
    is_current,
    is_deleted,
    meta_type_1_hash,
    meta_type_2_hash,
    meta_create_datetime,
    meta_update_datetime
)
SELECT
    customer_id,
    sms_opted_out as sms_opt_out,
    email_mkt_opted_out as email_mkt_opt_out,
    email_trg_opted_out as email_trg_opt_out,
    meta_event_datetime,
    CASE
        WHEN s.meta_record_status = 'new'
        THEN to_timestamp_ltz('1900-01-01')
        ELSE s.effective_start_datetime
    END AS effective_start_datetime,
    nvl(s.effective_end_datetime, to_timestamp_ltz('9999-12-31')) AS effective_end_datetime,
    nvl2(s.effective_end_datetime, FALSE, TRUE) AS is_current,
    is_deleted,
    meta_type_1_hash,
    meta_type_2_hash,
    meta_create_datetime,
    meta_update_datetime
FROM _type_2_delta s;

/*
UPDATE stg.meta_table_dependency_watermark
SET
    high_watermark_datetime = IFF(
                                    dependent_table_name IS NOT NULL,
                                    new_high_watermark_datetime,
                                    (
                                        SELECT
                                            MAX(meta_update_datetime)
                                        FROM reference.iterable_subscription_log
                                    )
                                ),
    meta_update_datetime = current_timestamp::timestamp_ltz(3)
WHERE table_name = $target_table;
*/
