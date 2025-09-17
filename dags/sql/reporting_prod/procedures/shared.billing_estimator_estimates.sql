--- Define Start and End date
SET start_date = DATE_TRUNC('MONTH', CURRENT_DATE); --- 1st day of current month;
SET end_date = CURRENT_DATE; ---Current Day
SET snapshot_datetime = CURRENT_TIMESTAMP()::TIMESTAMP_LTZ(3);

INSERT INTO shared.billing_estimator_estimates
SELECT sub.*,
       NULLIF(ber.credited_first_time_5day_sum, 0) /  NULLIF(ber.total_attempts_5day_sum, 0) AS first_time_success_rate_5dayavg,
       NULLIF(ber.retry_success_5day_sum, 0) / NULLIF(ber.retry_attempts_5day_sum, 0) AS retry_success_rate_5dayavg
FROM (
    SELECT date(p.date_period_start) AS billing_period,
        p.period_id,
        CURRENT_TIMESTAMP AS estimate_creation_datetime,
        s.store_id,
        s.label,
        COUNT(DISTINCT CASE WHEN mb.statuscode = 5155 AND m.statuscode = 3930 THEN mb.membership_id END) AS marked_for_credit,
        COUNT(DISTINCT CASE WHEN mb.statuscode = 5150 AND m.statuscode = 3930 THEN mb.membership_id END) AS not_yet_due,
        COUNT(DISTINCT CASE WHEN mb.statuscode IN (5160, 5161) AND m.statuscode = 3930 THEN mb.membership_id END) AS total_members_in_retry,
        COUNT(DISTINCT CASE WHEN mb.statuscode IN (5170, 5171) AND date(o.datetime_added) = CURRENT_DATE THEN o.order_id END) AS credited_today_count,
        COUNT(DISTINCT CASE WHEN TO_DATE(o.datetime_added) = CURRENT_DATE THEN o.order_id END) AS today_attempted,
        COUNT(DISTINCT CASE WHEN q.statuscode = 4205 AND date(q.datetime_last_retry) = CURRENT_DATE THEN q.order_id END) AS total_retry_successes_today
    FROM lake_consolidated_view.ultra_merchant.membership_billing mb
    LEFT OUTER JOIN lake_consolidated_view.ultra_merchant."ORDER" o ON o.order_id = mb.order_id
    LEFT OUTER JOIN lake_consolidated_view.ultra_merchant.billing_retry_schedule_queue q ON q.order_id = o.order_id
    JOIN lake_consolidated_view.ultra_merchant.membership m ON mb.membership_id = m.membership_id
    JOIN lake_consolidated_view.ultra_merchant.store s ON s.store_id = m.store_id
    LEFT OUTER JOIN lake_consolidated_view.ultra_merchant.period p ON p.period_id = mb.period_id
    WHERE date(p.date_period_start) = $start_date
        AND mb.membership_type_id = 3
    GROUP BY p.date_period_start,
        p.period_id,
        CURRENT_TIMESTAMP,
        s.store_id,
        s.label

    UNION
    SELECT date(p.date_period_start) AS billing_period,
        p.period_id,
        CURRENT_TIMESTAMP AS estimate_creation_datetime,
        s.store_id,
        s.label,
        COUNT(DISTINCT CASE WHEN mp.statuscode = 3953 AND m.statuscode = 3930 THEN mp.membership_id END) AS marked_for_credit,
        COUNT(DISTINCT CASE WHEN mp.statuscode = 3950 AND m.statuscode = 3930 THEN mp.membership_id END) AS not_yet_due,
        COUNT(DISTINCT CASE WHEN mp.statuscode = 3956 AND m.statuscode = 3930 THEN mp.membership_id END) AS total_members_in_retry,
        COUNT(DISTINCT CASE WHEN mp.statuscode = 3957 AND date(o.datetime_added) = CURRENT_DATE THEN o.order_id END) AS credited_today_count,
        COUNT(DISTINCT CASE WHEN TO_DATE(o.datetime_added) = CURRENT_DATE THEN o.order_id END) AS today_attempted,
        COUNT(DISTINCT CASE WHEN q.statuscode = 4205 AND date(q.datetime_last_retry) = CURRENT_DATE THEN q.order_id END) AS total_retry_successes_today
    FROM lake_consolidated_view.ultra_merchant.membership_period mp
    LEFT OUTER JOIN lake_consolidated_view.ultra_merchant."ORDER" o ON o.order_id = mp.credit_order_id
    LEFT OUTER JOIN lake_consolidated_view.ultra_merchant.billing_retry_schedule_queue q ON q.order_id = o.order_id
    JOIN lake_consolidated_view.ultra_merchant.membership m ON mp.membership_id = m.membership_id
    JOIN lake_consolidated_view.ultra_merchant.store s ON s.store_id = m.store_id
    JOIN lake_consolidated_view.ultra_merchant.period p ON p.period_id = mp.period_id
    WHERE date(p.date_period_start) = $start_date
        AND m.store_id IN (46, 79, 26, 41, 121, 55, 36, 38, 48, 50, 57, 59, 61, 63, 65, 67, 69, 71, 73, 75, 77, 125,
                           127, 129, 131, 133, 135, 137, 139)
    GROUP BY p.date_period_start,
        p.period_id,
        CURRENT_TIMESTAMP,
        s.store_id,
        s.label
) sub
LEFT OUTER JOIN shared.billing_estimator_rates ber ON ber.label = sub.label
    AND ber.order_date_added = date(sub.estimate_creation_datetime);

-- Temorarly adding this for analysis DA-25863
INSERT INTO shared.billing_estimator_estimates_detail
SELECT sub.*,
       NULLIF(ber.credited_first_time_5day_sum, 0) /  NULLIF(ber.total_attempts_5day_sum, 0) AS first_time_success_rate_5dayavg,
       NULLIF(ber.retry_success_5day_sum, 0) / NULLIF(ber.retry_attempts_5day_sum, 0) AS retry_success_rate_5dayavg,
       $snapshot_datetime AS snapshot_datetime
FROM (
    SELECT date(p.date_period_start) AS billing_period,
        o.order_id,
        p.period_id,
        CURRENT_TIMESTAMP AS estimate_creation_datetime,
        s.store_id,
        s.label,
        COUNT(DISTINCT CASE WHEN mb.statuscode = 5155 AND m.statuscode = 3930 THEN mb.membership_id END) AS marked_for_credit,
        COUNT(DISTINCT CASE WHEN mb.statuscode = 5150 AND m.statuscode = 3930 THEN mb.membership_id END) AS not_yet_due,
        COUNT(DISTINCT CASE WHEN mb.statuscode IN (5160, 5161) AND m.statuscode = 3930 THEN mb.membership_id END) AS total_members_in_retry,
        COUNT(DISTINCT CASE WHEN mb.statuscode IN (5170, 5171) AND date(o.datetime_added) = CURRENT_DATE THEN o.order_id END) AS credited_today_count,
        COUNT(DISTINCT CASE WHEN TO_DATE(o.datetime_added) = CURRENT_DATE THEN o.order_id END) AS today_attempted,
        COUNT(DISTINCT CASE WHEN q.statuscode = 4205 AND date(q.datetime_last_retry) = CURRENT_DATE THEN q.order_id END) AS total_retry_successes_today
    FROM lake_consolidated_view.ultra_merchant.membership_billing mb
    LEFT OUTER JOIN lake_consolidated_view.ultra_merchant."ORDER" o ON o.order_id = mb.order_id
    LEFT OUTER JOIN lake_consolidated_view.ultra_merchant.billing_retry_schedule_queue q ON q.order_id = o.order_id
    JOIN lake_consolidated_view.ultra_merchant.membership m ON mb.membership_id = m.membership_id
    JOIN lake_consolidated_view.ultra_merchant.store s ON s.store_id = m.store_id
    LEFT OUTER JOIN lake_consolidated_view.ultra_merchant.period p ON p.period_id = mb.period_id
    WHERE date(p.date_period_start) = $start_date
        AND mb.membership_type_id = 3
    GROUP BY p.date_period_start,
        o.order_id,
        p.period_id,
        CURRENT_TIMESTAMP,
        s.store_id,
        s.label

    UNION
    SELECT date(p.date_period_start) AS billing_period,
        o.order_id,
        p.period_id,
        CURRENT_TIMESTAMP AS estimate_creation_datetime,
        s.store_id,
        s.label,
        COUNT(DISTINCT CASE WHEN mp.statuscode = 3953 AND m.statuscode = 3930 THEN mp.membership_id END) AS marked_for_credit,
        COUNT(DISTINCT CASE WHEN mp.statuscode = 3950 AND m.statuscode = 3930 THEN mp.membership_id END) AS not_yet_due,
        COUNT(DISTINCT CASE WHEN mp.statuscode = 3956 AND m.statuscode = 3930 THEN mp.membership_id END) AS total_members_in_retry,
        COUNT(DISTINCT CASE WHEN mp.statuscode = 3957 AND date(o.datetime_added) = CURRENT_DATE THEN o.order_id END) AS credited_today_count,
        COUNT(DISTINCT CASE WHEN TO_DATE(o.datetime_added) = CURRENT_DATE THEN o.order_id END) AS today_attempted,
        COUNT(DISTINCT CASE WHEN q.statuscode = 4205 AND date(q.datetime_last_retry) = CURRENT_DATE THEN q.order_id END) AS total_retry_successes_today
    FROM lake_consolidated_view.ultra_merchant.membership_period mp
    LEFT OUTER JOIN lake_consolidated_view.ultra_merchant."ORDER" o ON o.order_id = mp.credit_order_id
    LEFT OUTER JOIN lake_consolidated_view.ultra_merchant.billing_retry_schedule_queue q ON q.order_id = o.order_id
    JOIN lake_consolidated_view.ultra_merchant.membership m ON mp.membership_id = m.membership_id
    JOIN lake_consolidated_view.ultra_merchant.store s ON s.store_id = m.store_id
    JOIN lake_consolidated_view.ultra_merchant.period p ON p.period_id = mp.period_id
    WHERE date(p.date_period_start) = $start_date
        AND m.store_id IN (46, 79, 26, 41, 121, 55, 36, 38, 48, 50, 57, 59, 61, 63, 65, 67, 69, 71, 73, 75, 77, 125,
                           127, 129, 131, 133, 135, 137, 139)
    GROUP BY p.date_period_start,
        o.order_id,
        p.period_id,
        CURRENT_TIMESTAMP,
        s.store_id,
        s.label
) sub
LEFT OUTER JOIN shared.billing_estimator_rates ber ON ber.label = sub.label
    AND ber.order_date_added = date(sub.estimate_creation_datetime);


INSERT INTO shared.billing_estimator_estimates_archive
SELECT billing_period,
    period_id,
    estimate_creation_datetime,
    store_id,
    label,
    marked_for_credit,
    not_yet_due,
    total_members_in_retry,
    credited_today_count,
    today_attempted,
    total_retry_successes_today,
    first_time_success_rate_5dayavg,
    retry_success_rate_5dayavg,
    $snapshot_datetime AS snapshot_datetime
FROM shared.billing_estimator_estimates;
