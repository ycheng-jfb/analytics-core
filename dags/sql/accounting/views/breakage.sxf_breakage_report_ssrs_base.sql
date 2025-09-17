CREATE OR REPLACE VIEW breakage.sxf_breakage_report_ssrs_base AS
with _nmp_transition as
(select original_issued_month,
       'Recognize' as deferred_recognition_label_credit,
       sum(breakage_recorded) as breakage_recorded
FROM breakage.savagex_us_breakage_report
WHERE activity_month = '2024-06-01'
group by 1,2)

/*NMP 2024 - Inclusive of Converted Tokens & Newly Issued Membership Credits*/
SELECT CONCAT(brand, ' ', country)                               business_unit,
       'Local Gross VAT'                                         currency,
       b.deferred_recognition_label_credit                          deferred_recognition_label,
       original_credit_reason                                    AS store_credit_reason,
       '24x12'                                                   assumption_months_baseline,
       b.original_issued_month                                     issued_month,
       activity_month::DATE                                      recognition_booking_month,
       credit_tenure                                             months_of_activity_from_issuance,
       DATEADD(MONTH, -23, activity_month)::DATE                 credit_cohort_start_blend,
       activity_month::DATE                                      credit_cohort_end_blend,
       24                                                        blended_credit_cohorts,
       'Clean Blended Cohorts'                                   blended_credit_cohorts_type,
       issued_to_date                                            issued_amount,
       redeemed_to_date                                          redeemed_to_date,
       cancelled_to_date                                         cancelled_to_date,
       expired_to_date                                           expired_to_date,
       unredeemed_to_date                                        unredeemed_to_date,
       0                                                         unredeemed_cant_recognize,

       redeemed_rate_to_date                                     redeemed_to_date_pct,
       cancelled_to_date / NULLIF(issued_amount, 0)              cancelled_to_date_pct,
       unredeemed_to_date / NULLIF(issued_amount, 0)             unredeemed_to_date_pct,

       expected_redeemed_rate                                    assumption_cumulative_redeemed_pct,
       expected_redeemed_rate_max                                assumption_cumulative_redeemed_m19or24_pct,
       1 - expected_redeemed_rate_max - expected_unredeemed_rate assumption_cumulative_cancelled_pct,
       expected_unredeemed_rate                                  assumption_cumulative_unredeemed_pct,

       issued_amount * expected_redeemed_rate                    expected_redeemed,
       expected_additional_redeemed                              expected_additional_redeemed,
       issued_amount * assumption_cumulative_cancelled_pct       expected_cancelled,
       expected_additional_cancelled                             expected_additional_cancelled,
       expected_additional_activity_amount_m14to19               expected_additional_activity_amount_m14to19,
       issued_amount * assumption_cumulative_unredeemed_pct      expected_unredeemed,

       NULL                                                      redeemed_to_date_vs_expected_redeemed_m10,
       expected_breakage_to_record                               expected_breakage,
       b.expected_cannot_recognize                               expected_cannot_recognize,
       b.breakage_recorded                                       breakage_to_record,
       IFF(RANK() OVER (PARTITION BY brand,region,country,original_credit_tender,IFF(original_credit_reason = 'Token Billing with Refund', 'Token Billing', original_credit_reason),
           original_credit_type,b.original_issued_month, b.deferred_recognition_label_credit
           ORDER BY activity_month) = 1, breakage_to_record,
           (breakage_to_record - LAG(breakage_to_record) OVER (PARTITION BY brand,region,country,original_credit_tender,IFF(original_credit_reason = 'Token Billing with Refund', 'Token Billing', original_credit_reason),original_credit_type,
               b.original_issued_month, b.deferred_recognition_label_credit
               ORDER BY activity_month)))-nvl(nt.breakage_recorded,0)                       change_in_breakage_to_record,
       b.expected_cannot_recognize                               reporting_cannot_recognize,
       breakage_to_record                                        reporting_expected_breakage,

       CONCAT('Savage X ', region)                               region,
       CONCAT('SX', country, '-', 'F')                           display_store,
       CONCAT('SX', country, '-', original_credit_type)          store_credit_type
FROM breakage.savagex_us_breakage_report b
LEFT JOIN _nmp_transition nt on nt.original_issued_month = b.original_issued_month
                                and nt.deferred_recognition_label_credit = b.deferred_recognition_label_credit
                                and b.activity_month = '2024-07-01'
WHERE activity_month >= '2024-07-01'

UNION ALL
/*TOKEN*/
SELECT CONCAT(brand, ' ', country)                               business_unit,
       'Local Gross VAT'                                         currency,
       'Recognize'                                               deferred_recognition_label,
       IFF(original_credit_reason = 'Token Billing with Refund', 'Token Billing', original_credit_reason) AS store_credit_reason,
       CASE
           WHEN activity_month::DATE <= '2023-06-01' THEN '19x12'
           ELSE '13x12' END                                      assumption_months_baseline,
       original_issued_month                                     issued_month,
       activity_month::DATE                                      recognition_booking_month,
       credit_tenure                                             months_of_activity_from_issuance,
       DATEADD(MONTH, -23, activity_month)::DATE                 credit_cohort_start_blend,
       activity_month::DATE                                      credit_cohort_end_blend,
       24                                                        blended_credit_cohorts,
       'Clean Blended Cohorts'                                   blended_credit_cohorts_type,
       issued_to_date                                            issued_amount,
       redeemed_to_date                                          redeemed_to_date,
       cancelled_to_date                                         cancelled_to_date,
       expired_to_date                                           expired_to_date,
       unredeemed_to_date                                        unredeemed_to_date,
       0                                                         unredeemed_cant_recognize,

       redeemed_rate_to_date                                     redeemed_to_date_pct,
       cancelled_to_date / NULLIF(issued_amount, 0)              cancelled_to_date_pct,
       unredeemed_to_date / NULLIF(issued_amount, 0)             unredeemed_to_date_pct,

       expected_redeemed_rate                                    assumption_cumulative_redeemed_pct,
       expected_redeemed_rate_max                                assumption_cumulative_redeemed_m19or24_pct,
       1 - expected_redeemed_rate_max - expected_unredeemed_rate assumption_cumulative_cancelled_pct,
       expected_unredeemed_rate                                  assumption_cumulative_unredeemed_pct,

       issued_amount * expected_redeemed_rate                    expected_redeemed,
       expected_additional_redeemed                              expected_additional_redeemed,
       issued_amount * assumption_cumulative_cancelled_pct       expected_cancelled,
       expected_additional_cancelled                             expected_additional_cancelled,
       expected_additional_activity_amount_m14to19               expected_additional_activity_amount_m14to19,
       issued_amount * assumption_cumulative_unredeemed_pct      expected_unredeemed,

       NULL                                                      redeemed_to_date_vs_expected_redeemed_m10,
       expected_breakage_to_record                               expected_breakage,
       0                                                         expected_cannot_recognize,
       breakage_recorded                                         breakage_to_record,
       IFF(RANK() OVER (PARTITION BY brand,region,country,original_credit_tender,IFF(original_credit_reason = 'Token Billing with Refund', 'Token Billing', original_credit_reason),
           original_credit_type,original_issued_month
           ORDER BY activity_month) = 1, breakage_to_record,
           (breakage_to_record - LAG(breakage_to_record) OVER (PARTITION BY brand,region,country,original_credit_tender,IFF(original_credit_reason = 'Token Billing with Refund', 'Token Billing', original_credit_reason),original_credit_type,
               original_issued_month
               ORDER BY activity_month)))                        change_in_breakage_to_record,
       0                                                         reporting_cannot_recognize,
       breakage_to_record                                        reporting_expected_breakage,

       CONCAT('Savage X ', region)                               region,
       CONCAT('SX', country, '-', 'F')                           display_store,
       CONCAT('SX', country, '-', original_credit_type)          store_credit_type
FROM breakage.savagex_us_breakage_report
WHERE original_credit_reason in ('Token Billing','Token Billing with Refund')
and activity_month < '2024-07-01'

UNION ALL
/*CONVERTED CREDITS*/
SELECT CONCAT(brand, ' ', country)                          business_unit,
       'Local Gross VAT'                                    currency,
       'Recognize'                                          deferred_recognition_label,
       original_credit_reason                               store_credit_reason,
       '24x12'                                              assumption_months_baseline,
       original_issued_month                                issued_month,
       activity_month::DATE                                 recognition_booking_month,
       credit_tenure                                        months_of_activity_from_issuance,
       '2019-02-01'::DATE                                   credit_cohort_start_blend,
       '2021-12-01'::DATE                                   credit_cohort_end_blend,
       24                                                   blended_credit_cohorts,
       'Actual Rates With Regression'                       blended_credit_cohorts_type,
       issued_to_date                                       issued_amount,
       redeemed_to_date                                     redeemed_to_date,
       cancelled_to_date                                    cancelled_to_date,
       expired_to_date                                      expired_to_date,
       unredeemed_to_date                                   unredeemed_to_date,
       0                                                    unredeemed_cant_recognize,

       redeemed_to_date / NULLIF(issued_amount, 0)          redeemed_to_date_pct,
       cancelled_to_date / NULLIF(issued_amount, 0)         cancelled_to_date_pct,
       unredeemed_to_date / NULLIF(issued_amount, 0)        unredeemed_to_date_pct,

       expected_redeemed_rate                               assumption_cumulative_redeemed_pct,
       expected_redeemed_rate_max                           assumption_cumulative_redeemed_m19or24_pct,
       1 - expected_redeemed_rate_max -
       expected_unredeemed_rate                             assumption_cumulative_cancelled_pct,
       expected_unredeemed_rate                             assumption_cumulative_unredeemed_pct,

       issued_amount * expected_redeemed_rate               expected_redeemed,
       expected_additional_redeemed                         expected_additional_redeemed,
       issued_amount * assumption_cumulative_cancelled_pct  expected_cancelled,
       expected_additional_cancelled                        expected_additional_cancelled,
       0                                                    expected_additional_activity_amount_m14to19,
       issued_amount * assumption_cumulative_unredeemed_pct expected_unredeemed,

       NULL                                                 redeemed_to_date_vs_expected_redeemed_m10,
       expected_breakage_to_record                          expected_breakage,
       0                                                    expected_cannot_recognize,
       breakage_recorded                                    breakage_to_record,
       IFF(RANK() OVER (PARTITION BY brand,region,country,original_credit_tender,original_credit_reason,
           original_credit_type,original_issued_month
           ORDER BY activity_month) = 1, breakage_to_record,
           (breakage_to_record - LAG(breakage_to_record) OVER (PARTITION BY brand,region,country,original_credit_tender,original_credit_reason,original_credit_type,
               original_issued_month
               ORDER BY activity_month)))                   change_in_breakage_to_record,
       0                                                    reporting_cannot_recognize,
       breakage_to_record                                   reporting_expected_breakage,
       CONCAT('Savage X ', region)                          region,
       CONCAT('SX', country, '-', 'F')                      display_store,
       CONCAT('SX', country, '-', original_credit_type)     store_credit_type
FROM breakage.savagex_us_breakage_report
WHERE original_credit_reason = 'Membership Credit'
and activity_month < '2024-07-01'

UNION ALL
/*NON-EMP*/
SELECT business_unit,
       currency,
       deferred_recognition_label,
       IFF(store_credit_reason = 'Membership Credit with Refund', 'Membership Credit', store_credit_reason) AS store_credit_reason,
       '24X10'                                                              assumption_months_baseline,
       issued_month,
       DATE_TRUNC(MONTH, recognition_booking_month)::DATE                   recognition_booking_month,
       credit_tenure                                                        months_of_activity_from_issuance,
       DATEADD(MONTH, -23, recognition_booking_month)::DATE                 credit_cohort_start_blend,
       recognition_booking_month::DATE                                      credit_cohort_end_blend,
       24                                                                   blended_credit_cohorts,
       IFF(credit_tenure <= 24, 'Clean Blended Cohorts', 'Actual Rates')    blended_credit_cohorts_type,
       issued_to_date                                                       issued_amount,
       redeemed_to_date,
       cancelled_to_date,
       0                                                                    expired_to_date,
       unredeemed_to_date,
       unredeemed_cant_recognize,

       redeemed_rate_to_date                                                redeemed_to_date_pct,
       cancelled_to_date_pct,
       unredeemed_to_date_pct,

       expected_redeemed_rate                                               assumption_cumulative_redeemed_pct,
       expected_redeemed_rate_max                                           assumption_cumulative_redeemed_m19or24_pct,
       assumption_cumulative_cancelled_pct,
       expected_unredeemed_rate                                             assumption_cumulative_unredeemed_pct,

       expected_redeemed,
       expected_additional_redeemed,
       expected_cancelled,
       expected_additional_cancelled,
       0                                                                    expected_additional_activity_amount_m14to19,
       expected_unredeemed,

       actual_redeemed_rate_vs_expected                                     redeemed_to_date_vs_expected_redeemed_m10,
       expected_breakage                                                    reporting_expected_breakage,
       expected_cannot_recognize,
       breakage_to_record,
       IFF(RANK() OVER (PARTITION BY business_unit,currency, deferred_recognition_label, IFF(store_credit_reason = 'Membership Credit with Refund', 'Membership Credit', store_credit_reason),issued_month
           ORDER BY recognition_booking_month) = 1, breakage_to_record,
           (breakage_to_record - LAG(breakage_to_record) OVER (PARTITION BY business_unit,currency, deferred_recognition_label, IFF(store_credit_reason = 'Membership Credit with Refund', 'Membership Credit', store_credit_reason),issued_month
                                                                   ORDER BY recognition_booking_month)))   as change_in_breakage_to_record,
       IFF(blended_credit_cohorts_type = 'Actual Rates', unredeemed_cant_recognize,
           expected_cannot_recognize)                                       reporting_cannot_recognize,
       expected_breakage                                                    reporting_expected_breakage,
       CONCAT('Savage X ',
              IFF(SUBSTR(business_unit, -2, 2) = 'CA' OR SUBSTR(business_unit, -2, 2) = 'US', 'NA',
                  'EU'))                                                    region,
       CASE
           WHEN business_unit = 'Savage X ES' THEN 'SXES'
           WHEN business_unit = 'Savage X DE' THEN 'SXDE'
           WHEN business_unit = 'Savage X EUREM' THEN 'SXNL'
           WHEN business_unit = 'Savage X FR' THEN 'SXFR'
           WHEN business_unit = 'Savage X UK' THEN 'SXUK'
           ELSE business_unit END                         display_store,
       CONCAT('SX', IFF(business_unit = 'Savage X EUREM', 'NL', SUBSTR(business_unit, -2, 2)), '-',
              'Fixed Credit')                                               store_credit_type
FROM breakage.jfb_sxf_membership_credit_breakage_report
WHERE business_unit IN ('Savage X DE',
                        'Savage X ES',
                        'Savage X EUREM',
                        'Savage X FR',
                        'Savage X UK')
  AND store_credit_reason in ('Membership Credit','Membership Credit with Refund')
  AND NOT (currency = 'Local Net VAT' AND region = 'JFBNA')
  AND CONTAINS(currency, 'Local');
