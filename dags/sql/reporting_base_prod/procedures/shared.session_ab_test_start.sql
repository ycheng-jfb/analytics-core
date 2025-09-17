SET low_watermark_ltz = %(low_watermark)s:: TIMESTAMP_LTZ;

CREATE OR REPLACE TEMP TABLE _session_base AS
SELECT distinct session_id
FROM (
    SELECT session_id
    FROM lake_consolidated_view.ultra_merchant.session_detail sd
    WHERE sd.meta_update_datetime > $low_watermark_ltz
    UNION ALL
    SELECT session_id
    FROM lake_consolidated_view.ultra_merchant.session s
    WHERE s.meta_update_datetime > $low_watermark_ltz
) AS A
WHERE session_id > 0
ORDER BY session_id ASC;

--getting all distinct test keys
CREATE OR REPLACE TEMPORARY TABLE _keys AS
SELECT DISTINCT
    a.CODE AS test_key,
    lower(a.type) as type,
    max(a.DATETIME_MODIFIED) as min_test_start_datetime_hq -- to get accurate minimum test start datetime
FROM lake_consolidated.ultra_cms_history.test_framework as a --archived test framework table to get archived and current tests
WHERE
   a.code IS NOT NULL
    and ((a.STATUSCODE = 113 and a.TEST_FRAMEWORK_GROUP_ID is null) --active but missing a test split
        or (a.STATUSCODE = 113 and a.test_framework_group_id NOT IN (19,20,24,25,150,151))) --excluding splits 0/100 or 100/0
group by a.code,lower(a.type);

--session-type tests
CREATE OR REPLACE TEMPORARY TABLE _test_keys_session_detail AS
SELECT DISTINCT
    s.customer_id,
    mm.membership_id,
    sd.session_id,
    sd.name AS test_key,
    sd.value AS test_value,
    min(CONVERT_TIMEZONE(COALESCE(ds.store_time_zone,'America/Los_Angeles'),
                    sd.datetime_added))::timestamp_tz AS ab_test_start_local_datetime,
    min(sd.datetime_added) AS test_start_datetime_hq --weird occurrences where there are mult datetimes for a session ID w/ same test value
FROM _session_base sb
JOIN lake_consolidated_view.ultra_merchant.session s
    ON s.session_id = sb.session_id
LEFT JOIN edw_prod.data_model.dim_store ds ON s.store_id = ds.store_id
JOIN lake_consolidated_view.ultra_merchant.session_detail sd
    ON sd.session_id = sb.session_id
JOIN _keys k
    ON k.test_key = sd.name
left join lake_consolidated_view.ultra_merchant.membership mm
    on s.customer_id = mm.customer_id
WHERE K.type = 'session'
AND sd.value NOT IN ('1.')
AND try_cast(sd.value AS INT) BETWEEN 1 AND 100 --filtering out weird values that don't apply
AND s.DATETIME_ADDED >= min_test_start_datetime_hq
and sd.DATETIME_ADDED >= min_test_start_datetime_hq
GROUP BY
    s.customer_id,
    mm.membership_id,
    sd.session_id,
    sd.name,
    sd.value;

--adding membership state to session-type tests
CREATE OR REPLACE TEMPORARY TABLE _test_keys_session AS
SELECT
    s.customer_id,
    s.membership_id,
    s.session_id,
    s.test_key,
    s.test_value,
    s.test_start_datetime_hq,
    s.ab_test_start_local_datetime,
    CASE WHEN f.MEMBERSHIP_STATE IS NULL THEN 'Prospect'
        else f.MEMBERSHIP_STATE end AS membership_state,
    CASE WHEN f.MEMBERSHIP_STATE = 'Lead' then datediff('day',CONVERT_TIMEZONE('America/Los_Angeles', EVENT_START_LOCAL_DATETIME),test_start_datetime_hq::timestamp_tz) + 1
        else null end AS lead_daily_tenure,
    CASE WHEN f.MEMBERSHIP_STATE = 'VIP' then datediff('month',CONVERT_TIMEZONE('America/Los_Angeles', EVENT_START_LOCAL_DATETIME),test_start_datetime_hq::timestamp_tz) + 1
        else null end AS vip_month_tenure
FROM _test_keys_session_detail s
LEFT JOIN edw_prod.DATA_MODEL.FACT_MEMBERSHIP_EVENT f
ON f.customer_id = s.customer_id
    AND CONVERT_TIMEZONE('America/Los_Angeles', f.EVENT_START_LOCAL_DATETIME) <= s.test_start_datetime_hq::timestamp_tz
    AND CONVERT_TIMEZONE('America/Los_Angeles', f.EVENT_END_LOCAL_DATETIME) > s.test_start_datetime_hq::timestamp_tz;

--getting the correct test date range for membership-type tests so that we only look at sessions between those dates
CREATE OR REPLACE TEMPORARY TABLE _max_mem_test_datetime_added AS
SELECT
    m.name AS test_key,
    min_test_start_datetime_hq, --new
    max(datetime_added) AS max_datetime_added
FROM lake_consolidated_view.ultra_merchant.membership_detail m
JOIN _keys k
    ON k.test_key = m.name
WHERE m.datetime_modified > dateadd(d, -30, $low_watermark_ltz)
GROUP BY m.name,min_test_start_datetime_hq;

--getting all customers from membership-type tests
CREATE OR REPLACE TEMPORARY TABLE _test_keys_membership_detail AS
SELECT DISTINCT
    m.membership_id,
    p.customer_id,
    m.name AS test_key,
    m.value AS test_value,
    min(m.datetime_added) AS test_start_datetime_hq
FROM lake_consolidated_view.ultra_merchant.membership_detail AS m
JOIN lake_consolidated_view.ultra_merchant.membership AS p
    ON p.membership_id = m.membership_id
JOIN _keys k
    ON k.test_key = m.name
JOIN _max_mem_test_datetime_added AS mk
    ON mk.test_key = k.test_key
WHERE
    k.type = 'membership'
    AND m.value NOT IN ('1.')
    AND try_cast(m.value AS INT) BETWEEN 1 AND 100 --filtering out weird values that don't apply
    and m.DATETIME_ADDED >= k.min_test_start_datetime_hq
GROUP BY
    m.membership_id,
    p.customer_id,
    m.name,
    m.value;

--getting membership state for membership-type tests
CREATE OR REPLACE TEMPORARY TABLE _test_keys_membership AS
SELECT
    m.membership_id,
    m.customer_id,
    m.test_key,
    m.test_value,
    m.test_start_datetime_hq,
    CASE WHEN f.MEMBERSHIP_STATE IS NULL THEN 'Prospect'
        else f.MEMBERSHIP_STATE end AS membership_state,
    CASE WHEN f.MEMBERSHIP_STATE = 'Lead' then datediff('day',CONVERT_TIMEZONE('America/Los_Angeles', EVENT_START_LOCAL_DATETIME),test_start_datetime_hq::timestamp_tz) + 1
        else null end AS lead_daily_tenure,
    CASE WHEN f.MEMBERSHIP_STATE = 'VIP' then datediff('month',CONVERT_TIMEZONE('America/Los_Angeles', EVENT_START_LOCAL_DATETIME),test_start_datetime_hq::timestamp_tz) + 1
        else null end AS vip_month_tenure
FROM _test_keys_membership_detail m
LEFT JOIN edw_prod.DATA_MODEL.FACT_MEMBERSHIP_EVENT f
ON f.customer_id = m.customer_id
    AND CONVERT_TIMEZONE('America/Los_Angeles', f.EVENT_START_LOCAL_DATETIME) <= m.test_start_datetime_hq::timestamp_tz
    AND CONVERT_TIMEZONE('America/Los_Angeles', f.EVENT_END_LOCAL_DATETIME) > m.test_start_datetime_hq::timestamp_tz;

--getting all sessions for membership-type tests that occurred within min-max date range
CREATE OR REPLACE TEMPORARY TABLE _test_keys_membership_sessions AS
SELECT
    md.membership_id,
    md.customer_id,
    s.session_id,
    md.test_key,
    md.test_value,
    test_start_datetime_hq,
    CONVERT_TIMEZONE(COALESCE(ds.store_time_zone,'America/Los_Angeles'),
                    test_start_datetime_hq)::timestamp_tz AS ab_test_start_local_datetime,
    s.datetime_added as session_hq_datetime,
    md.membership_state,
    md.lead_daily_tenure,
    md.vip_month_tenure,
    mx.max_datetime_added
FROM _test_keys_membership md
JOIN lake_consolidated_view.ultra_merchant.session s
    ON s.customer_id = md.customer_id
LEFT JOIN edw_prod.data_model.dim_store ds ON s.store_id = ds.store_id
JOIN _max_mem_test_datetime_added mx
    ON md.test_key = mx.test_key
where
    ((datediff(hh,(s.datetime_added + interval '3 hour'),test_start_datetime_hq) <= 3
    AND mx.test_key = md.test_key)
    AND (s.datetime_added <= mx.max_datetime_added
    AND mx.test_key = md.test_key));

-- creating one session table for both session and membership-type tests
CREATE OR REPLACE TEMP table _session_ab_test_start_stg AS
    SELECT
    'session' as ab_test_type
    ,session_id
    ,test_key AS ab_test_key
    ,test_value AS ab_test_segment
    ,ab_test_start_local_datetime::timestamp_tz AS ab_test_start_local_datetime
    ,membership_state AS test_start_membership_state
    ,lead_daily_tenure as test_start_lead_daily_tenure
    ,vip_month_tenure as test_start_vip_month_tenure
    ,HASH(ab_test_type, session_id, ab_test_key, ab_test_segment, ab_test_start_local_datetime, test_start_membership_state,test_start_lead_daily_tenure,test_start_vip_month_tenure,customer_id,membership_id) AS meta_row_hash
    ,customer_id
    ,membership_id
FROM _test_keys_session

UNION

SELECT
    'membership' AS ab_test_type
    ,session_id
    ,test_key AS ab_test_key
    ,test_value AS ab_test_segment
    ,ab_test_start_local_datetime::timestamp_tz AS ab_test_start_local_datetime
    ,membership_state AS test_start_membership_state
    ,lead_daily_tenure as test_start_lead_daily_tenure
    ,vip_month_tenure as test_start_vip_month_tenure
    ,HASH(ab_test_type, session_id, ab_test_key, ab_test_segment, ab_test_start_local_datetime, test_start_membership_state,test_start_lead_daily_tenure,test_start_vip_month_tenure,customer_id,membership_id) AS meta_row_hash
    ,customer_id
    ,membership_id
FROM _test_keys_membership_sessions;

MERGE INTO shared.session_ab_test_start AS tgt
    USING (
            SELECT *
            FROM (
                     SELECT *,
                            edw_prod.stg.udf_unconcat_brand(session_id) AS meta_original_session_id,
                            row_number() over (partition by session_id, ab_test_key, ab_test_segment, ab_test_start_local_datetime order by ab_test_start_local_datetime DESC) AS rn
                     FROM _session_ab_test_start_stg
            ) AS A
            where rn = 1
        ) AS src
        ON src.session_id = tgt.session_id
        AND src.ab_test_key = tgt.ab_test_key
        AND src.ab_test_segment = tgt.ab_test_segment
        AND src.ab_test_start_local_datetime = tgt.ab_test_start_local_datetime
    WHEN NOT MATCHED THEN
        INSERT(
            session_id,
            ab_test_type,
            test_start_membership_state,
            ab_test_key,
            ab_test_segment,
            ab_test_start_local_datetime,
            test_start_lead_daily_tenure,
            test_start_vip_month_tenure,
            meta_row_hash,
            customer_id,
            membership_id,
            meta_original_session_id
            )
        VALUES(
            src.session_id,
            src.ab_test_type,
            src.test_start_membership_state,
            src.ab_test_key,
            src.ab_test_segment,
            src.ab_test_start_local_datetime,
            src.test_start_lead_daily_tenure,
            src.test_start_vip_month_tenure,
            src.meta_row_hash,
            src.customer_id,
            src.membership_id,
            src.meta_original_session_id
            )
    WHEN MATCHED AND src.meta_row_hash != tgt.meta_row_hash THEN
        UPDATE
        SET /* No need to update the MERGE KEYs, they must match to update in the first place. */
            --tgt.session_id = src.session_id,
            tgt.ab_test_type = src.ab_test_type,
            tgt.test_start_membership_state =src.test_start_membership_state,
            --tgt.ab_test_key = src.ab_test_key,
            --tgt.ab_test_segment = src.ab_test_segment,
            --tgt.ab_test_start_local_datetime = src.ab_test_start_local_datetime,
            tgt.test_start_lead_daily_tenure = src.test_start_lead_daily_tenure,
            tgt.test_start_vip_month_tenure = src.test_start_vip_month_tenure,
            tgt.meta_row_hash = src.meta_row_hash,
            tgt.customer_id = src.customer_id,
            tgt.membership_id = src.membership_id,
            tgt.meta_original_session_id = src.meta_original_session_id,
            tgt.meta_update_datetime = CURRENT_TIMESTAMP;
