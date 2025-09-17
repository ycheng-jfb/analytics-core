SET low_watermark_ltz = %(low_watermark)s :: TIMESTAMP_LTZ;

CREATE OR REPLACE TEMP TABLE _session_base (session_id number) CLUSTER BY (TRUNC(session_id, -4));

INSERT INTO _session_base (session_id)
SELECT DISTINCT s.session_id
FROM lake_consolidated_view.ultra_merchant.session s
WHERE meta_update_datetime > $low_watermark_ltz
ORDER BY s.session_id;

CREATE OR REPLACE TEMPORARY TABLE _membership_state_base AS
SELECT
    sb.session_id,
    s.datetime_added AS session_datetime,
    COALESCE(f.MEMBERSHIP_STATE, 'Prospect') AS membership_state,
    CASE
        WHEN membership_state =  'Lead'
        THEN datediff(DAY, CONVERT_TIMEZONE('America/Los_Angeles', f.event_start_local_datetime) , session_datetime) + 1
    END AS daily_lead_tenure,
    CASE
        WHEN membership_state = 'VIP'
        THEN datediff(MONTH, CONVERT_TIMEZONE('America/Los_Angeles', f.event_start_local_datetime), session_datetime) + 1
    END AS monthly_vip_tenure
FROM _session_base sb
JOIN lake_consolidated_view.ultra_merchant.session AS s
    ON sb.session_id = s.session_id
LEFT JOIN edw_prod.data_model.fact_membership_event AS f
    ON f.customer_id = s.customer_id
    AND CONVERT_TIMEZONE('America/Los_Angeles', f.event_start_local_datetime) <= s.datetime_added
    AND CONVERT_TIMEZONE('America/Los_Angeles', f.event_end_local_datetime) > s.datetime_added
ORDER BY sb.session_id;

MERGE INTO staging.membership_state t USING (
     SELECT
        session_id,
        membership_state,
        daily_lead_tenure,
        monthly_vip_tenure,
        edw_prod.stg.udf_unconcat_brand(session_id) AS meta_original_session_id,
        HASH(session_id, membership_state, daily_lead_tenure, monthly_vip_tenure) AS meta_row_hash
    FROM _membership_state_base
    ORDER BY session_id
) AS src ON src.session_id = t.session_id
    WHEN NOT MATCHED THEN
        INSERT (
            session_id,
            membership_state,
            daily_lead_tenure,
            monthly_vip_tenure,
            meta_original_session_id,
            meta_row_hash
            )
        VALUES (
            src.session_id,
            src.membership_state,
            src.daily_lead_tenure,
            src.monthly_vip_tenure,
            src.meta_original_session_id,
            src.meta_row_hash
            )
    WHEN MATCHED AND src.meta_row_hash != t.meta_row_hash
        THEN UPDATE SET
        t.membership_state = src.membership_state,
        t.daily_lead_tenure = src.daily_lead_tenure,
        t.monthly_vip_tenure = src.monthly_vip_tenure,
        t.meta_row_hash = src.meta_row_hash,
        t.meta_original_session_id = src.meta_original_session_id,
        t.meta_update_datetime = current_timestamp;
