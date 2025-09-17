--- Define Start and End date
SET start_date = DATE_TRUNC('MONTH', CURRENT_DATE); --- 1st day of current month;
SET end_date = CURRENT_DATE(); ---Current Day
SET snapshot_datetime = CURRENT_TIMESTAMP()::TIMESTAMP_LTZ(3);

INSERT INTO shared.billing_estimator_output
SELECT billing_month,
       label,
       final_label,
       estimate_creation_datetime,
       SUM(first_attempt)                             actuals_first_attempt_credits_billed_count,
       SUM(retry_billing)                             actuals_retry_credits_billed_count,
       SUM(total_credit_billing_orders)               actuals_total_credits_billed_count,
       SUM(credit_billing_charged_amount_usd)         actuals_total_credits_billed_amount,
       SUM(membership_credit_charged_amount_budget)   finance_budget_total_credits_charged_amount,
       SUM(membership_credit_charged_amount_forecast) finance_forecast_total_credits_charged_amount,
       SUM(marked_for_credit)                         estimates_marked_for_credit,
       SUM(not_yet_due)                               estimates_not_yet_due,
       SUM(total_members_in_retry)                    estimates_total_members_in_retry,
       SUM(credited_today_count)                      estimates_already_billed_today_count,
       SUM(today_attempted)                           estimates_today_attempted,
       SUM(total_retry_successes_today)               estimates_already_billed_retry_today_count,
       SUM(first_time_success_rate_5dayavg),
       SUM(retry_success_rate_5dayavg)
FROM (
-- MTD BILLING ACTUALS
    SELECT DATE_TRUNC('MONTH', date) AS billing_month,
        CASE report_mapping WHEN 'FK-TREV-NA' THEN 'FabKids'
            WHEN 'FL-TREV-FR' THEN 'Fabletics FR'
            WHEN 'FL-TREV-UK' THEN 'Fabletics UK'
            WHEN 'FL-TREV-DE' THEN 'Fabletics DE'
            WHEN 'FL-TREV-DK' THEN 'Fabletics DK'
            WHEN 'FL-TREV-ES' THEN 'Fabletics ES'
            WHEN 'FL-TREV-SE' THEN 'Fabletics SE'
            WHEN 'FL-TREV-NL' THEN 'Fabletics NL'
            WHEN 'FL+SC-TREV-US' THEN 'Fabletics'
            WHEN 'FL+SC-TREV-CA' THEN 'Fabletics CA'
            WHEN 'JF-TREV-UK' THEN 'JustFab UK'
            WHEN 'JF-TREV-NL' THEN 'JustFab NL'
            WHEN 'JF-TREV-DE' THEN 'JustFab DE'
            WHEN 'JF-TREV-SE' THEN 'JustFab SE'
            WHEN 'JF-TREV-ES' THEN 'JustFab ES'
            WHEN 'JF-TREV-FR' THEN 'JustFab FR'
            WHEN 'JF-TREV-DK' THEN 'JustFab DK'
            WHEN 'JF-TREV-CA' THEN 'JustFab CA'
            WHEN 'JF-TREV-US' THEN 'JustFab'
            WHEN 'SD-TREV-NA' THEN 'ShoeDazzle'
            WHEN 'SX-TREV-DE' THEN 'Savage X DE'
            WHEN 'SX-TREV-FR' THEN 'Savage X FR'
            WHEN 'SX-TREV-ES' THEN 'Savage X ES'
            WHEN 'SX-TREV-EUREM' THEN 'Savage X EUREM'
            WHEN 'SX-TREV-UK' THEN 'Savage X UK'
            WHEN 'SX-TREV-NA' THEN 'Savage X'
            WHEN 'YT-OREV-NA' THEN 'Yitty NA' END AS label,
        CASE report_mapping WHEN 'FK-TREV-NA' THEN 'FK'
            WHEN 'FL-TREV-FR' THEN 'FL EU'
            WHEN 'FL-TREV-UK' THEN 'FL EU'
            WHEN 'FL-TREV-DE' THEN 'FL EU'
            WHEN 'FL-TREV-DK' THEN 'FL EU'
            WHEN 'FL-TREV-ES' THEN 'FL EU'
            WHEN 'FL-TREV-SE' THEN 'FL EU'
            WHEN 'FL-TREV-NL' THEN 'FL EU'
            WHEN 'FL+SC-TREV-US' THEN 'FL NA'
            WHEN 'FL+SC-TREV-CA' THEN 'FL NA'
            WHEN 'JF-TREV-UK' THEN 'JF EU'
            WHEN 'JF-TREV-NL' THEN 'JF EU'
            WHEN 'JF-TREV-DE' THEN 'JF EU'
            WHEN 'JF-TREV-SE' THEN 'JF EU'
            WHEN 'JF-TREV-ES' THEN 'JF EU'
            WHEN 'JF-TREV-FR' THEN 'JF EU'
            WHEN 'JF-TREV-DK' THEN 'JF EU'
            WHEN 'JF-TREV-CA' THEN 'JF NA'
            WHEN 'JF-TREV-US' THEN 'JF NA'
            WHEN 'SD-TREV-NA' THEN 'SD'
            WHEN 'SX-TREV-DE' THEN 'SX EU'
            WHEN 'SX-TREV-FR' THEN 'SX EU'
            WHEN 'SX-TREV-ES' THEN 'SX EU'
            WHEN 'SX-TREV-EUREM' THEN 'SX EU'
            WHEN 'SX-TREV-UK' THEN 'SX EU'
            WHEN 'SX-TREV-NA' THEN 'SX NA'
            WHEN 'YT-OREV-NA' THEN 'YT NA' END AS final_label,
        $end_date AS estimate_creation_datetime,
        SUM(billed_credit_cash_transaction_count) - SUM(billed_credits_successful_on_retry) AS first_attempt,
        SUM(billed_credits_successful_on_retry) AS retry_billing,
        SUM(billed_credit_cash_transaction_count) AS total_credit_billing_orders,
        SUM(billed_credit_cash_transaction_amount) AS credit_billing_charged_amount_usd,
        0 AS membership_credit_charged_amount_budget,
        0 AS membership_credit_charged_amount_forecast,
        0 AS marked_for_credit,
        0 AS not_yet_due,
        0 AS total_members_in_retry,
        0 AS credited_today_count,
        0 AS today_attempted,
        0 AS total_retry_successes_today,
        0 AS first_time_success_rate_5dayavg,
        0 AS retry_success_rate_5dayavg
    FROM edw_prod.reporting.daily_cash_final_output
    WHERE date >= $start_date
        AND date < $end_date
        AND date_object = 'placed'
        AND currency_object = 'usd'
        AND currency_type = 'USD'
        AND report_mapping IN ('FK-TREV-NA', 'FL-TREV-FR', 'FL-TREV-UK', 'FL-TREV-DE', 'FL-TREV-DK', 'FL-TREV-ES',
                               'FL-TREV-SE', 'FL-TREV-NL', 'FL+SC-TREV-US', 'FL+SC-TREV-CA', 'JF-TREV-UK', 'JF-TREV-NL',
                               'JF-TREV-DE', 'JF-TREV-SE', 'JF-TREV-ES', 'JF-TREV-FR', 'JF-TREV-DK', 'JF-TREV-CA',
                               'JF-TREV-US', 'SD-TREV-NA', 'SX-TREV-DE', 'SX-TREV-FR', 'SX-TREV-ES', 'SX-TREV-EUREM',
                               'SX-TREV-UK', 'SX-TREV-NA', 'YT-OREV-NA')
    GROUP BY DATE_TRUNC('MONTH', date),
        label,
        final_label

    UNION

    SELECT DATE_TRUNC('MONTH', date) AS billing_month,
        '' AS label,
        CASE report_mapping WHEN 'SD-TREV-NA' THEN 'SD'
        WHEN 'SX-TREV-NA' THEN 'SX NA'
        WHEN 'FK-TREV-NA' THEN 'FK'
        WHEN 'FL+SC-TREV-NA' THEN 'FL NA'
        WHEN 'JF-TREV-NA' THEN 'JF NA'
        WHEN 'JF-TREV-EU' THEN 'JF EU'
        WHEN 'FL-TREV-EU' THEN 'FL EU'
        WHEN 'SX-TREV-EU' THEN 'SX EU'
        WHEN 'YT-OREV-NA' THEN 'YT NA' END AS final_label,
        $end_date AS estimate_creation_datetime,
        0 AS first_attempt,
        0 AS retry_billing,
        0 AS total_credit_billing_orders,
        0 AS credit_billing_charged_amount_usd,
        MAX(billed_credit_cash_transaction_amount_budget) membership_credit_charged_amount_budget,
        MAX(billed_credit_cash_transaction_amount_forecast) membership_credit_charged_amount_forecast,
        0 AS marked_for_credit,
        0 AS not_yet_due,
        0 AS total_members_in_retry,
        0 AS credited_today_count,
        0 AS today_attempted,
        0 AS total_retry_successes_today,
        0 AS first_time_success_rate_5dayavg,
        0 AS retry_success_rate_5dayavg
    FROM edw_prod.reporting.daily_cash_final_output
    WHERE date = $start_date
        AND date_object = 'placed'
        AND currency_object = 'usd'
        AND currency_type = 'USD'
        AND report_mapping IN ('SD-TREV-NA', 'SX-TREV-NA', 'FK-TREV-NA', 'FL+SC-TREV-NA', 'JF-TREV-NA',
                   'JF-TREV-EU', 'FL-TREV-EU', 'SX-TREV-EU', 'YT-OREV-NA')
    GROUP BY DATE_TRUNC('MONTH', date),
        final_label

    UNION

----FINAL OUTPUT
    SELECT date(be.billing_period) billing_period,
        IFF(be.label = 'Yitty', 'Yitty NA', be.label) AS label,
        IFF(besl.final_label = 'YTY NA', 'YT NA', besl.final_label) AS final_label,
        be.estimate_creation_datetime,
        0 AS first_attempt,
        0 AS retry_billing,
        0 AS total_credit_billing_orders,
        0 AS credit_billing_charged_amount_usd,
        0 AS membership_credit_charged_amount_budget,
        0 AS membership_credit_charged_amount_forecast,
        be.marked_for_credit,
        be.not_yet_due,
        be.total_members_in_retry,
        be.credited_today_count,
        be.today_attempted,
        be.total_retry_successes_today,
        be.first_time_success_rate_5dayavg,
        be.retry_success_rate_5dayavg
    FROM shared.billing_estimator_estimates be
    JOIN shared.billing_estimator_store_label besl ON be.store_id = besl.store_id
    WHERE date(estimate_creation_datetime) = $end_date
) test
GROUP BY billing_month,
    label,
    final_label,
    estimate_creation_datetime;

INSERT INTO shared.billing_estimator_output_archive
SELECT billing_month,
    label,
    final_label,
    estimate_creation_datetime,
    actuals_first_attempt_credits_billed_count,
    actuals_retry_credits_billed_count,
    actuals_total_credits_billed_count,
    actuals_total_credits_billed_amount,
    finance_budget_total_credits_charged_amount,
    finance_forecast_total_credits_charged_amount,
    estimates_marked_for_credit,
    estimates_not_yet_due,
    estimates_total_members_in_retry,
    estimates_already_billed_today_count,
    estimates_today_attempted,
    estimates_already_billed_retry_today_count,
    first_time_success_rate_5dayavg,
    retry_success_rate_5dayavg,
    $snapshot_datetime AS snapshot_datetime
FROM shared.billing_estimator_output;
