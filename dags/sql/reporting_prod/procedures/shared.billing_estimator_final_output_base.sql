
SET snapshot_datetime = CURRENT_TIMESTAMP()::TIMESTAMP_LTZ(3);

CREATE OR REPLACE TEMPORARY TABLE _billing_estimator_output AS
SELECT beo.billing_month,
    IFF(beo.label = 'Yitty', 'Yitty NA', beo.label) AS label,
    beo.final_label,
    date(beo.estimate_creation_datetime) AS estimate_creation_datetime,
    SUM(beo.actuals_first_attempt_credits_billed_count) AS actuals_first_attempt_credits_billed_count,
    SUM(beo.actuals_retry_credits_billed_count) AS actuals_retry_credits_billed_count,
    SUM(beo.actuals_total_credits_billed_count) AS actuals_total_credits_billed_count,
    SUM(beo.actuals_total_credits_billed_amount) AS actuals_total_credits_billed_amount,
    SUM(beo.finance_budget_total_credits_charged_amount) AS finance_budget_total_credits_charged_amount,
    SUM(beo.finance_forecast_total_credits_charged_amount) AS finance_forecast_total_credits_charged_amount,
    SUM(beo.estimates_marked_for_credit) AS estimates_marked_for_credit,
    SUM(beo.estimates_not_yet_due) AS estimates_not_yet_due,
    SUM(beo.estimates_total_members_in_retry) AS estimates_total_members_in_retry,
    SUM(beo.estimates_already_billed_today_count) AS estimates_already_billed_today_count,
    SUM(beo.estimates_today_attempted) AS estimates_today_attempted,
    SUM(beo.estimates_already_billed_retry_today_count) AS estimates_already_billed_retry_today_count,
    SUM(beo.first_time_success_rate_5dayavg) AS first_time_success_rate_5dayavg,
    SUM(beo.retry_success_rate_5dayavg) AS retry_success_rate_5dayavg,
    SUM(NULLIF(ber.credited_first_time, 0)) / NULLIF(SUM(ber.total_attempts), 0) AS first_time_success_rate_daily,
    SUM(ber.credited_first_time_prior_month) / NULLIF(SUM(ber.total_attempts_prior_month), 0) AS first_time_success_rate_prior_month,
    SUM(ber.retry_success_prior_month) / NULLIF(SUM(ber.total_attempts_less_credited_first_time_prior_month), 0) AS retry_success_rate_prior_month,
    SUM(ber.retry_success_6months) / NULLIF(SUM(ber.retry_attempts_6months), 0) AS retry_success_rate_6months
FROM shared.billing_estimator_output beo
LEFT OUTER JOIN shared.billing_estimator_rates ber ON IFF(beo.label = 'Yitty', 'Yitty NA', beo.label) = ber.label
    AND ber.order_date_added = date(beo.estimate_creation_datetime)
GROUP BY beo.billing_month,
    IFF(beo.label = 'Yitty', 'Yitty NA', beo.label),
    beo.final_label,
    date(beo.estimate_creation_datetime);

CREATE OR REPLACE TEMPORARY TABLE _daily_cash_final_output AS
SELECT DATE_TRUNC('MONTH', date) AS billing_month,
    ADD_MONTHS(billing_month, 1) AS next_month,
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
        WHEN 'SX-TREV-SX' THEN 'Savage X ES'
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
    SUM(billed_credit_cash_transaction_count) AS actuals_count,
    SUM(billed_credit_cash_transaction_amount) AS actuals_amount
FROM edw_prod.reporting.daily_cash_final_output
WHERE date_object = 'placed'
    AND currency_object = 'usd'
    AND currency_type = 'USD'
    AND report_mapping IN (
        'FK-TREV-NA', 'FL-TREV-FR', 'FL-TREV-UK', 'FL-TREV-DE', 'FL-TREV-DK', 'FL-TREV-ES', 'FL-TREV-SE', 'FL-TREV-NL',
        'FL+SC-TREV-US', 'FL+SC-TREV-CA', 'JF-TREV-UK', 'JF-TREV-NL', 'JF-TREV-DE', 'JF-TREV-SE', 'JF-TREV-ES', 'JF-TREV-FR',
        'JF-TREV-DK', 'JF-TREV-CA', 'JF-TREV-US', 'SD-TREV-NA', 'SX-TREV-DE', 'SX-TREV-FR', 'SX-TREV-ES', 'SX-TREV-EUREM',
        'SX-TREV-UK', 'SX-TREV-NA', 'YT-OREV-NA'
    )
GROUP BY billing_month,
    label,
    final_label;

CREATE OR REPLACE TEMPORARY TABLE _prior_month_actuals AS
SELECT billing_month,
    label,
    final_label,
    actuals_count,
    actuals_amount,
    0 AS actuals_count_prior_month,
    0 AS actuals_amount_prior_month
FROM _daily_cash_final_output
UNION
SELECT next_month,
    label,
    final_label,
    0 AS actuals_count,
    0 AS actuals_amount,
    actuals_count AS actuals_count_prior_month,
    actuals_amount AS actuals_amount_prior_month
FROM _daily_cash_final_output;

CREATE OR REPLACE TEMPORARY TABLE _actuals AS
SELECT billing_month,
    label,
    final_label,
    SUM(CASE WHEN CURRENT_DATE > LAST_DAY(billing_month) THEN actuals_count ELSE 0 END) AS actuals_count_current_month,
    SUM(CASE WHEN CURRENT_DATE > LAST_DAY(billing_month) THEN actuals_amount ELSE 0 END) AS actuals_amount_current_month,
    SUM(actuals_count_prior_month) AS actuals_count_prior_month,
    SUM(actuals_amount_prior_month) AS actuals_amount_prior_month
FROM _prior_month_actuals
GROUP BY billing_month,
    label,
    final_label;

CREATE OR REPLACE TEMPORARY TABLE _weighted_membership_price AS
SELECT label,
    IFF(final_label = 'YTY NA', 'YT NA', final_label) AS final_label,
    weighted_membership_price
FROM shared.weighted_membership_price wmp
JOIN shared.billing_estimator_store_label besl ON besl.store_id = wmp.store_id
WHERE final_label IN ('FK', 'FL NA', 'JF NA', 'SD', 'SX NA', 'YTY NA');

BEGIN;

TRUNCATE TABLE shared.billing_estimator_final_output_base;

INSERT INTO shared.billing_estimator_final_output_base (
    billing_month,
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
    membership_fee,
    label_rank,
    first_time_success_rate_daily_lag1,
    first_time_success_rate_intraday,
    first_time_success_rate_daily,
    first_time_success_rate_5dayavg,
    retry_success_rate_5dayavg,
    first_time_success_rate_prior_month,
    retry_success_rate_prior_month,
    retry_success_rate_6months,
    actuals_fullmonth,
    actuals_count_fullmonth,
    actuals_priormonth,
    actuals_count_priormonth
)
SELECT beo.billing_month,
    beo.label,
    beo.final_label,
    beo.estimate_creation_datetime,
    beo.actuals_first_attempt_credits_billed_count,
    beo.actuals_retry_credits_billed_count,
    beo.actuals_total_credits_billed_count,
    beo.actuals_total_credits_billed_amount,
    beo.finance_budget_total_credits_charged_amount,
    beo.finance_forecast_total_credits_charged_amount,
    beo.estimates_marked_for_credit,
    beo.estimates_not_yet_due,
    beo.estimates_total_members_in_retry,
    beo.estimates_already_billed_today_count,
    beo.estimates_today_attempted,
    beo.estimates_already_billed_retry_today_count,
    CASE WHEN (beo.billing_month = DATE_TRUNC('MONTH', CURRENT_DATE()) AND beo.final_label IN ('FK', 'FL NA', 'JF NA', 'SD', 'SX NA', 'YT NA'))
            THEN wmp.weighted_membership_price
        WHEN (beo.billing_month = '2022-05-01' and beo.label = 'Fabletics') THEN '54.95'
        ELSE NULLIF(a.actuals_amount_prior_month, 0) / NULLIF(a.actuals_count_prior_month, 0) END AS membership_fee,
    RANK() OVER (PARTITION BY beo.label, beo.billing_month ORDER BY beo.estimate_creation_datetime) AS label_rank,
    LAG(NULLIF(beo.first_time_success_rate_daily, 0)) OVER (PARTITION BY beo.label ORDER BY beo.estimate_creation_datetime) AS first_time_success_rate_daily_lag1,
    NULLIF(beo.estimates_already_billed_today_count, 0) / NULLIF(beo.estimates_today_attempted, 0) AS first_time_success_rate_intraday,
    NULLIF(beo.first_time_success_rate_daily, 0) AS first_time_success_rate_daily,
    NULLIF(beo.first_time_success_rate_5dayavg, 0) AS first_time_success_rate_5dayavg,
    NULLIF(beo.retry_success_rate_5dayavg, 0) AS retry_success_rate_5dayavg,
    beo.first_time_success_rate_prior_month,
    beo.retry_success_rate_prior_month,
    beo.retry_success_rate_6months,
    ZEROIFNULL(a.actuals_amount_current_month) AS actuals_fullmonth,
    ZEROIFNULL(a.actuals_count_current_month) AS actuals_count_fullmonth,
    ZEROIFNULL(a.actuals_amount_prior_month) AS actuals_priormonth,
    ZEROIFNULL(a.actuals_count_prior_month) AS actuals_count_priormonth
FROM _billing_estimator_output beo
LEFT JOIN _actuals a ON beo.billing_month = a.billing_month
    AND beo.label = a.label
    AND beo.final_label = a.final_label
LEFT JOIN _weighted_membership_price wmp ON wmp.label = beo.label
    AND wmp.final_label = beo.final_label;

INSERT INTO shared.billing_estimator_final_output_base_archive (
    billing_month,
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
    membership_fee,
    label_rank,
    first_time_success_rate_daily_lag1,
    first_time_success_rate_intraday,
    first_time_success_rate_daily,
    first_time_success_rate_5dayavg,
    retry_success_rate_5dayavg,
    first_time_success_rate_prior_month,
    retry_success_rate_prior_month,
    retry_success_rate_6months,
    actuals_fullmonth,
    actuals_count_fullmonth,
    actuals_priormonth,
    actuals_count_priormonth,
    snapshot_datetime
)
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
    membership_fee,
    label_rank,
    first_time_success_rate_daily_lag1,
    first_time_success_rate_intraday,
    first_time_success_rate_daily,
    first_time_success_rate_5dayavg,
    retry_success_rate_5dayavg,
    first_time_success_rate_prior_month,
    retry_success_rate_prior_month,
    retry_success_rate_6months,
    actuals_fullmonth,
    actuals_count_fullmonth,
    actuals_priormonth,
    actuals_count_priormonth,
    $snapshot_datetime AS snapshot_datetime
FROM shared.billing_estimator_final_output_base;

COMMIT;
