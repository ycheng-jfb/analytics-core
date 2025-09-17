CREATE OR REPLACE TRANSIENT TABLE SHARED.TFG016_TRAFFIC_ESTIMATION AS
SELECT
    session_hq_datetime::DATE as date,
    DATE_TRUNC('month', session_hq_datetime)::DATE as month_year,
    EXTRACT(YEAR FROM session_hq_datetime) as year,
    EXTRACT(MONTH FROM session_hq_datetime) as month,
    EXTRACT(DAY FROM session_hq_datetime) as day,
    EXTRACT(HOUR FROM session_hq_datetime) as hour,
    store_brand,
    store_region,
    membership_state,
    platform,
    is_migrated_session,
    is_spawned_session,
    is_session_without_visitor_id,
    is_test_customer_account,
    is_bot,
    is_in_segment,
/*
    is_session_with_order,
    is_bot_session_with_successful_order,
 */
    traffic_type,
    traffic_issue_type,
    count(distinct session_id) as sessions,
    sum(case when is_lead_registration_action = true and membership_state = 'Prospect' then 1 else 0 end) as leads,
    sum(orders) as orders
FROM reporting_base_prod.shared.session_indicator
Where
     CONVERT_TIMEZONE('America/Los_Angeles', session_local_datetime) between
      date_trunc('month',current_date) - interval '13 month' and current_date + interval '1 day'
    /* Yitty cleanup */
    AND NOT (
        store_brand = 'Yitty'
        AND session_local_datetime <= '2022-04-02')
GROUP BY
    session_hq_datetime::DATE,
    DATE_TRUNC('month', session_hq_datetime)::DATE,
    EXTRACT(YEAR FROM session_hq_datetime),
    EXTRACT(MONTH FROM session_hq_datetime),
    EXTRACT(DAY FROM session_hq_datetime),
    EXTRACT(HOUR FROM session_hq_datetime),
    store_brand,
    store_region,
    membership_state,
    platform,
    is_migrated_session,
    is_spawned_session,
    is_session_without_visitor_id,
    is_test_customer_account,
    is_bot,
    is_in_segment,
/*
    is_session_with_order,
    is_bot_session_with_successful_order,
 */
    traffic_type,
    traffic_issue_type
;
