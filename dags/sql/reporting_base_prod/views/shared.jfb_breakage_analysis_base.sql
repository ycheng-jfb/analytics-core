CREATE OR REPLACE VIEW shared.jfb_breakage_analysis_base
(
 business_unit,
 currency,
 deferred_recognition_label,
 store_credit_reason,
 assumption_months_baseline,
 issued_month,
 recognition_booking_month,
 months_of_activity_from_issuance,
 credit_cohort_start_blend,
 credit_cohort_end_blend,
 blended_credit_cohorts,
 blended_credit_cohorts_type,
 issued_amount,
 redeemed_m10,
 cancelled_m10,
 unredeemed_m10,
 redeemed_to_date,
 cancelled_to_date,
 expired_to_date,
 unredeemed_to_date,
 redeemed_m11plus,
 cancelled_m11plus,
 unredeemed_cant_recognize,
 redeemed_to_date_pct,
 cancelled_to_date_pct,
 unredeemed_to_date_pct,
 assumption_cumulative_redeemed_pct,
 assumption_cumulative_redeemed_m10_pct,
 assumption_cumulative_redeemed_m11plus_pct,
 assumption_cumulative_redeemed_m19or24_pct,
 assumption_cumulative_cancelled_pct,
 assumption_cumulative_cancelled_m10_pct,
 assumption_cumulative_cancelled_m11plus_pct,
 assumption_cumulative_unredeemed_m10_pct,
 assumption_cumulative_unredeemed_pct,
 expected_redeemed,
 expected_redeemed_m10,
 expected_redeemed_m11plus,
 expected_additional_redeemed,
 expected_cancelled,
 expected_cancelled_m10,
 expected_cancelled_m11plus,
 expected_additional_cancelled,
 expected_unredeemed_m10,
 expected_unredeemed,
 redeemed_to_date_vs_expected_redeemed_m10,
 expected_breakage,
 expected_cannot_recognize,
 breakage_to_record,
 change_in_breakage_to_record,
 monthly_redemption,
 monthly_cancellation,
 monthly_expiration,
 reporting_cannot_recognize,
 reporting_expected_breakage,
 region,
 display_store,
 store_credit_type,
 breakage_type
)
AS
WITH nonemp AS
         (SELECT business_unit             target_business_unit,
                 recognition_booking_month last_nonemp_month,
                 issued_month,
                 SUM(breakage_to_record)   breakage_to_record
          FROM reporting_base_prod.shared.jfb_sxf_membership_credit_breakage_report last_nonemp_details
          WHERE recognition_booking_month = (SELECT MAX(recognition_booking_month)
                                             FROM reporting_base_prod.shared.jfb_sxf_membership_credit_breakage_report last_nonemp_month
                                             WHERE last_nonemp_month.business_unit = last_nonemp_details.business_unit
                                               AND issued_month >= '2018-12-01')
            AND business_unit IN ('FabKids US', 'JustFab US', 'ShoeDazzle US')
            AND issued_month >= '2018-12-01'
            AND currency = 'Local Gross VAT'
          GROUP BY business_unit, recognition_booking_month, issued_month),

     giftco AS
         (SELECT business_unit,
                 brand || ' ' || country        target_business_unit,
                 recognition_booking_month,
                 issued_month                   giftco_issued_month,
                 MIN(recognition_booking_month) first_token_month,
                 SUM(redeemed_to_date)          first_token_redeemed,
                 SUM(cancelled_to_date)         first_token_cancelled,
                 SUM(breakage_to_record)        first_token_breakage
          FROM reporting_base_prod.shared.jfb_converted_breakage_report
          WHERE business_unit LIKE '%Token to Giftco'
            AND recognition_booking_month = (SELECT MIN(recognition_booking_month)
                                             FROM reporting_base_prod.shared.jfb_converted_breakage_report
                                             WHERE business_unit LIKE '%Token to Giftco')
          GROUP BY business_unit,
                   brand || ' ' || country,
                   recognition_booking_month,
                   issued_month)
/*CONVERTED CREDITS*/
SELECT cbr.business_unit                                                          business_unit,
       'Local Gross VAT'                                                          currency,
       'Recognize'                                                                deferred_recognition_label,
       cbr.store_credit_reason                                                    store_credit_reason,
       '36x12 + 24x12'                                                            assumption_months_baseline,
       cbr.issued_month                                                           issued_month,
       (cbr.recognition_booking_month::DATE)                                      recognition_booking_month,
       credit_tenure                                                              months_of_activity_from_issuance,
       (DATEADD(MONTH, -24, cbr.recognition_booking_month)::DATE)                 credit_cohort_start_blend,
       (cbr.recognition_booking_month::DATE)                                      credit_cohort_end_blend,
       24                                                                         blended_credit_cohorts,
       IFF(credit_tenure <= 24, 'Clean Blended Cohorts', 'Actual Rates')          blended_credit_cohorts_type,
       issued_to_date                                                             issued_amount,
       NULL                                                                       redeemed_m10,
       NULL                                                                       cancelled_m10,
       NULL                                                                       unredeemed_m10,
       redeemed_to_date                                                           redeemed_to_date,
       cancelled_to_date                                                          cancelled_to_date,
       expired_to_date                                                            expired_to_date,
       unredeemed_to_date                                                         unredeemed_to_date,
       NULL                                                                       redeemed_m11plus,
       NULL                                                                       cancelled_m11plus,
       0                                                                          unredeemed_cant_recognize,
       redeemed_to_date / NULLIF(issued_amount, 0)                                redeemed_to_date_pct,
       cancelled_to_date / NULLIF(issued_amount, 0)                               cancelled_to_date_pct,
       unredeemed_to_date / NULLIF(issued_amount, 0)                              unredeemed_to_date_pct,

       expected_redeemed_rate                                                     assumption_cumulative_redeemed_pct,
       NULL                                                                       assumption_cumulative_redeemed_m10_pct,
       NULL                                                                       assumption_cumulative_redeemed_m11plus_pct,

       expected_redeemed_rate                                                     assumption_cumulative_redeemed_m19or24_pct,

       1 - expected_redeemed_rate -
       expected_unredeemed_rate                                                   assumption_cumulative_cancelled_pct,
       NULL                                                                       assumption_cumulative_cancelled_m10_pct,
       NULL                                                                       assumption_cumulative_cancelled_m11plus_pct,
       NULL                                                                       assumption_cumulative_unredeemed_m10_pct,
       expected_unredeemed_rate                                                   assumption_cumulative_unredeemed_pct,

       issued_amount * expected_unredeemed_rate                                   expected_redeemed,
       NULL                                                                       expected_redeemed_m10,
       NULL                                                                       expected_redeemed_m11plus,
       expected_additional_redeemed                                               expected_additional_redeemed,
       issued_amount * assumption_cumulative_cancelled_pct                        expected_cancelled,
       NULL                                                                       expected_cancelled_m10,
       NULL                                                                       expected_cancelled_m11plus,

       expected_additional_cancelled                                              expected_additional_cancelled,
       NULL                                                                       expected_unredeemed_m10,
       issued_amount * assumption_cumulative_unredeemed_pct                       expected_unredeemed,
       NULL                                                                       redeemed_to_date_vs_expected_redeemed_m10,
       expected_breakage                                                          expected_breakage,
       0                                                                          expected_cannot_recognize,
       cbr.breakage_to_record                                                     breakage_to_record,
       CASE
           WHEN cbr.business_unit LIKE '%Token to Giftco%'
               THEN IFF(RANK() OVER (PARTITION BY cbr.business_unit,cbr.store_credit_reason,cbr.issued_month
               ORDER BY cbr.recognition_booking_month) = 1, -1 * (cbr.redeemed_to_date + cbr.cancelled_to_date),
                        (cbr.breakage_to_record - LAG(cbr.breakage_to_record) OVER
                            (PARTITION BY cbr.business_unit,cbr.store_credit_reason,cbr.issued_month
                            ORDER BY cbr.recognition_booking_month)))

           WHEN cbr.recognition_booking_month = first_token_month
               THEN IFF(RANK() OVER (PARTITION BY cbr.business_unit,cbr.store_credit_reason,cbr.issued_month
               ORDER BY cbr.recognition_booking_month) = 1,
                        cbr.breakage_to_record - IFNULL(nonemp.breakage_to_record, 0),
                        (cbr.breakage_to_record - LAG(cbr.breakage_to_record) OVER
                            (PARTITION BY cbr.business_unit,cbr.store_credit_reason,cbr.issued_month
                            ORDER BY cbr.recognition_booking_month)))
               + giftco.first_token_breakage + giftco.first_token_redeemed + giftco.first_token_cancelled

           ELSE IFF(RANK() OVER (PARTITION BY cbr.business_unit,cbr.store_credit_reason,cbr.issued_month
               ORDER BY cbr.recognition_booking_month) = 1,
                    cbr.breakage_to_record - IFNULL(nonemp.breakage_to_record, 0),
                    (cbr.breakage_to_record - LAG(cbr.breakage_to_record) OVER
                        (PARTITION BY cbr.business_unit,cbr.store_credit_reason,cbr.issued_month
                        ORDER BY cbr.recognition_booking_month)))
           END                                                                    change_in_breakage_to_record,

       IFF(RANK() OVER (PARTITION BY cbr.business_unit,region,country,original_credit_tender,cbr.store_credit_reason,
           original_credit_type,cbr.issued_month
           ORDER BY cbr.recognition_booking_month) = 1, redeemed_to_date,
           (redeemed_to_date -
            LAG(redeemed_to_date) OVER (PARTITION BY cbr.business_unit,region,country,original_credit_tender,cbr.store_credit_reason,original_credit_type,
                cbr.issued_month
                ORDER BY cbr.recognition_booking_month)))                         monthly_redemption,


       IFF(RANK() OVER (PARTITION BY cbr.business_unit,region,country,original_credit_tender,cbr.store_credit_reason,
           original_credit_type,cbr.issued_month
           ORDER BY cbr.recognition_booking_month) = 1, cancelled_to_date,
           (cancelled_to_date - LAG(cancelled_to_date)
                                    OVER (PARTITION BY cbr.business_unit,region,country,original_credit_tender,cbr.store_credit_reason,original_credit_type,
                                        cbr.issued_month
                                        ORDER BY cbr.recognition_booking_month))) monthly_cancellation,

       IFF(RANK() OVER (PARTITION BY cbr.business_unit,region,country,original_credit_tender,cbr.store_credit_reason,
           original_credit_type,cbr.issued_month
           ORDER BY cbr.recognition_booking_month) = 1, expired_to_date,
           (expired_to_date -
            LAG(expired_to_date) OVER (PARTITION BY cbr.business_unit,region,country,original_credit_tender,cbr.store_credit_reason,original_credit_type,
                cbr.issued_month
                ORDER BY cbr.recognition_booking_month)))                         monthly_expiration,
       0                                                                          reporting_cannot_recognize,
       cbr.breakage_to_record                                                     reporting_expected_breakage,
       'JFB' || region                                                            region,
       CASE
           WHEN cbr.business_unit = 'FabKids US' THEN 'FKUS'
           WHEN cbr.business_unit = 'ShoeDazzle US' THEN 'SDUS'
           WHEN cbr.business_unit = 'JustFab US' THEN 'JFUS'
           ELSE cbr.business_unit END                                             display_store,
       CONCAT('JFB', SUBSTR(cbr.business_unit, -2, 2), '-',
              'F', '-',
              cbr.store_credit_reason)                                            store_credit_type,
       breakage_type
FROM reporting_base_prod.shared.jfb_converted_breakage_report cbr
         LEFT JOIN nonemp
                   ON cbr.business_unit = nonemp.target_business_unit
                       AND cbr.issued_month = nonemp.issued_month
                       AND cbr.recognition_booking_month = DATEADD(MONTH, 1, nonemp.last_nonemp_month)
         LEFT JOIN giftco
                   ON cbr.business_unit = giftco.target_business_unit
                       AND cbr.recognition_booking_month = giftco.first_token_month
                       AND cbr.issued_month = giftco.giftco_issued_month
WHERE cbr.business_unit IN ('JustFab US', 'FabKids US', 'ShoeDazzle US',
                            'JustFab US Token to Giftco', 'FabKids US Token to Giftco', 'ShoeDazzle US Token to Giftco')
  AND NOT (currency = 'Local Net VAT' AND region = 'NA')
  AND CONTAINS(currency, 'Local')

UNION ALL

/*TOKEN*/
SELECT business_unit                                                     business_unit,
       'Local Gross VAT'                                                 currency,
       'Recognize'                                                       deferred_recognition_label,
       store_credit_reason                                               store_credit_reason,
       '13x12'                                                           assumption_months_baseline,
       issued_month                                                      issued_month,
       (recognition_booking_month::DATE)                                 recognition_booking_month,
       credit_tenure                                                     months_of_activity_from_issuance,
       (DATEADD(MONTH, -23, recognition_booking_month)::DATE)            credit_cohort_start_blend,
       (recognition_booking_month::DATE)                                 credit_cohort_end_blend,
       24                                                                blended_credit_cohorts,
       IFF(credit_tenure <= 24, 'Clean Blended Cohorts', 'Actual Rates') blended_credit_cohorts_type,
       issued_to_date                                                    issued_amount,
       NULL                                                              redeemed_m10,
       NULL                                                              cancelled_m10,
       NULL                                                              unredeemed_m10,
       redeemed_to_date                                                  redeemed_to_date,
       cancelled_to_date                                                 cancelled_to_date,
       expired_to_date                                                   expired_to_date,
       unredeemed_to_date                                                unredeemed_to_date,
       NULL                                                              redeemed_m11plus,
       NULL                                                              cancelled_m11plus,
       0                                                                 unredeemed_cant_recognize,
       redeemed_rate_to_date                                             redeemed_to_date_pct,
       cancelled_to_date / NULLIF(issued_amount, 0)                      cancelled_to_date_pct,
       unredeemed_to_date / NULLIF(issued_amount, 0)                     unredeemed_to_date_pct,
       expected_redeemed_rate                                            assumption_cumulative_redeemed_pct,
       NULL                                                              assumption_cumulative_redeemed_m10_pct,
       NULL                                                              assumption_cumulative_redeemed_m11plus_pct,
       expected_redeemed_rate_max                                        assumption_cumulative_redeemed_m19or24_pct,
       1 - expected_redeemed_rate_max - expected_unredeemed_rate         assumption_cumulative_cancelled_pct,
       NULL                                                              assumption_cumulative_cancelled_m10_pct,
       NULL                                                              assumption_cumulative_cancelled_m11plus_pct,
       NULL                                                              assumption_cumulative_unredeemed_m10_pct,
       expected_unredeemed_rate                                          assumption_cumulative_unredeemed_pct,
       issued_amount * expected_redeemed_rate                            expected_redeemed,
       NULL                                                              expected_redeemed_m10,
       NULL                                                              expected_redeemed_m11plus,
       expected_additional_redeemed                                      expected_additional_redeemed,
       issued_amount * assumption_cumulative_cancelled_pct               expected_cancelled,
       NULL                                                              expected_cancelled_m10,
       NULL                                                              expected_cancelled_m11plus,
       expected_additional_cancelled                                     expected_additional_cancelled,
       NULL                                                              expected_unredeemed_m10,
       issued_amount * assumption_cumulative_unredeemed_pct              expected_unredeemed,
       NULL                                                              redeemed_to_date_vs_expected_redeemed_m10,
       expected_breakage                                                 expected_breakage,
       0                                                                 expected_cannot_recognize,
       breakage_to_record                                                breakage_to_record,
       IFF(RANK() OVER (PARTITION BY business_unit,region,country,original_credit_tender,store_credit_reason,
           original_credit_type,issued_month
           ORDER BY recognition_booking_month) = 1, breakage_to_record,
           (breakage_to_record - LAG(breakage_to_record) OVER (PARTITION BY business_unit,region,country,original_credit_tender,store_credit_reason,original_credit_type,
               issued_month
               ORDER BY recognition_booking_month)))                     change_in_breakage_to_record,

       IFF(RANK() OVER (PARTITION BY business_unit,region,country,original_credit_tender,store_credit_reason,
           original_credit_type,issued_month
           ORDER BY recognition_booking_month) = 1, redeemed_to_date,
           (redeemed_to_date - LAG(redeemed_to_date) OVER (PARTITION BY business_unit,region,country,original_credit_tender,store_credit_reason,original_credit_type,
               issued_month
               ORDER BY recognition_booking_month)))                     monthly_redemption,


       IFF(RANK() OVER (PARTITION BY business_unit,region,country,original_credit_tender,store_credit_reason,
           original_credit_type,issued_month
           ORDER BY recognition_booking_month) = 1, cancelled_to_date,
           (cancelled_to_date - LAG(cancelled_to_date) OVER (PARTITION BY business_unit,region,country,original_credit_tender,store_credit_reason,original_credit_type,
               issued_month
               ORDER BY recognition_booking_month)))                     monthly_cancellation,

       IFF(RANK() OVER (PARTITION BY business_unit,region,country,original_credit_tender,store_credit_reason,
           original_credit_type,issued_month
           ORDER BY recognition_booking_month) = 1, expired_to_date,
           (expired_to_date - LAG(expired_to_date) OVER (PARTITION BY business_unit,region,country,original_credit_tender,store_credit_reason,original_credit_type,
               issued_month
               ORDER BY recognition_booking_month)))                     monthly_expiration,


       0                                                                 reporting_cannot_recognize,
       breakage_to_record                                                reporting_expected_breakage,
       'JFB' || region                                                   region,
       CASE
           WHEN business_unit = 'FabKids US' THEN 'FKUS'
           WHEN business_unit = 'ShoeDazzle US' THEN 'SDUS'
           WHEN business_unit = 'JustFab US' THEN 'JFUS'
           ELSE business_unit END                                        display_store,
       CONCAT('JFB', SUBSTR(business_unit, -2, 2), '-',
              'F', '-',
              store_credit_reason)                                       store_credit_type,
       breakage_type
FROM reporting_base_prod.shared.jfb_token_breakage_report
WHERE business_unit IN ('JustFab US', 'FabKids US', 'ShoeDazzle US')
  AND NOT (currency = 'Local Net VAT' AND region = 'NA')
  AND CONTAINS(currency, 'Local')

UNION ALL

/*NON-EMP*/
SELECT CASE
           WHEN mcr.business_unit = 'FabKids US' AND issued_month < '2018-12-01' THEN 'FabKids US-Giftco'
           WHEN mcr.business_unit = 'FabKids US' THEN 'FabKids US'
           WHEN mcr.business_unit = 'ShoeDazzle US' AND issued_month < '2018-12-01' THEN 'ShoeDazzle US-Giftco'
           WHEN mcr.business_unit = 'ShoeDazzle US' THEN 'ShoeDazzle US'
           WHEN mcr.business_unit = 'JustFab US' AND issued_month < '2018-12-01' THEN 'JustFab US-Giftco'
           WHEN mcr.business_unit = 'JustFab US' THEN 'JustFab US'
           WHEN mcr.business_unit = 'Fabletics Womens US' AND issued_month < '2018-12-01'
               THEN 'Fabletics Womens US-Giftco'
           ELSE mcr.business_unit END                                          business_unit,
       currency,
       deferred_recognition_label,
       store_credit_reason,
       '24X10'                                                                 assumption_months_baseline,
       issued_month,
       (DATE_TRUNC(MONTH, mcr.recognition_booking_month)::DATE)                recognition_booking_month,
       credit_tenure                                                           months_of_activity_from_issuance,
       (DATEADD(MONTH, -23, recognition_booking_month)::DATE)                  credit_cohort_start_blend,
       (recognition_booking_month::DATE)                                       credit_cohort_end_blend,
       24                                                                      blended_credit_cohorts,
       IFF(credit_tenure <= 24, 'Clean Blended Cohorts', 'Actual Rates')       blended_credit_cohorts_type,
       issued_to_date                                                          issued_amount,
       NULL                                                                    redeemed_m10,
       NULL                                                                    cancelled_m10,
       NULL                                                                    unredeemed_m10,
       redeemed_to_date,
       cancelled_to_date,
       0                                                                       expired_to_date,
       unredeemed_to_date,
       NULL                                                                    redeemed_m11plus,
       NULL                                                                    cancelled_m11plus,
       unredeemed_cant_recognize,
       redeemed_rate_to_date                                                   redeemed_to_date_pct,
       cancelled_to_date_pct,
       unredeemed_to_date_pct,
       expected_redeemed_rate                                                  assumption_cumulative_redeemed_pct,
       NULL                                                                    assumption_cumulative_redeemed_m10_pct,
       NULL                                                                    assumption_cumulative_redeemed_m11plus_pct,
       NULL                                                                    assumption_cumulative_redeemed_m19or24_pct,
       assumption_cumulative_cancelled_pct,
       NULL                                                                    assumption_cumulative_cancelled_m10_pct,
       NULL                                                                    assumption_cumulative_cancelled_m11plus_pct,
       NULL                                                                    assumption_cumulative_unredeemed_m10_pct,
       expected_unredeemed_rate                                                assumption_cumulative_unredeemed_pct,
       expected_redeemed,
       NULL                                                                    expected_redeemed_m10,
       NULL                                                                    expected_redeemed_m11plus,
       expected_additional_redeemed,
       expected_cancelled,
       NULL                                                                    expected_cancelled_m10,
       NULL                                                                    expected_cancelled_m11plus,
       expected_additional_cancelled,
       NULL                                                                    expected_unredeemed_m10,
       expected_unredeemed,
       actual_redeemed_rate_vs_expected                                        redeemed_to_date_vs_expected_redeemed_m10,
       expected_breakage                                                       reporting_expected_breakage,
       expected_cannot_recognize,
       breakage_to_record,
       IFF(RANK() OVER (PARTITION BY business_unit,currency, deferred_recognition_label, store_credit_reason,issued_month
           ORDER BY recognition_booking_month) = 1, breakage_to_record,
           (breakage_to_record - LAG(breakage_to_record)
                                     OVER (PARTITION BY business_unit,currency, deferred_recognition_label, store_credit_reason,issued_month
                                         ORDER BY recognition_booking_month))) change_in_breakage_to_record,

       IFF(RANK() OVER (PARTITION BY business_unit,store_credit_reason, currency, deferred_recognition_label,
           assumption_months_baseline,issued_month
           ORDER BY recognition_booking_month) = 1, redeemed_to_date,
           (redeemed_to_date - LAG(redeemed_to_date)
                                   OVER (PARTITION BY business_unit,store_credit_reason, currency, deferred_recognition_label,
                                       assumption_months_baseline,issued_month
                                       ORDER BY recognition_booking_month)))   monthly_redemption,


       IFF(RANK() OVER (PARTITION BY business_unit,store_credit_reason, currency, deferred_recognition_label,
           assumption_months_baseline,issued_month
           ORDER BY recognition_booking_month) = 1, cancelled_to_date,
           (cancelled_to_date - LAG(cancelled_to_date)
                                    OVER (PARTITION BY business_unit,store_credit_reason, currency, deferred_recognition_label,
                                        assumption_months_baseline,issued_month
                                        ORDER BY recognition_booking_month)))  monthly_cancellation,


       IFF(RANK() OVER (PARTITION BY business_unit,store_credit_reason, currency, deferred_recognition_label,
           assumption_months_baseline,issued_month
           ORDER BY recognition_booking_month) = 1, expired_to_date,
           (expired_to_date - LAG(expired_to_date)
                                  OVER (PARTITION BY business_unit,store_credit_reason, currency, deferred_recognition_label,
                                      assumption_months_baseline,issued_month
                                      ORDER BY recognition_booking_month)))    monthly_expiration,


       IFF(blended_credit_cohorts_type = 'Actual Rates', unredeemed_cant_recognize,
           expected_cannot_recognize)                                          reporting_cannot_recognize,
       expected_breakage                                                       reporting_expected_breakage,
       'JFB' || mcr.region                                                     region,
       CASE
           WHEN mcr.business_unit = 'FabKids US' AND issued_month < '2018-12-01' THEN 'FKUS-Giftco'
           WHEN mcr.business_unit = 'FabKids US' THEN 'FKUS'
           WHEN mcr.business_unit = 'ShoeDazzle US' AND issued_month < '2018-12-01' THEN 'SDUS-Giftco'
           WHEN mcr.business_unit = 'ShoeDazzle US' THEN 'SDUS'
           WHEN mcr.business_unit = 'JustFab US' AND issued_month < '2018-12-01' THEN 'JFUS-Giftco'
           WHEN mcr.business_unit = 'JustFab US' THEN 'JFUS'
           WHEN mcr.business_unit = 'Fabletics Womens US' AND issued_month < '2018-12-01' THEN 'FLUS-F-Giftco'
           WHEN mcr.business_unit = 'Fabletics Womens US' THEN 'FLUSDONOTUSE'
           WHEN mcr.business_unit = 'JustFab CA' THEN 'JFCA'
           WHEN mcr.business_unit = 'JustFab DE' THEN 'JFDE'
           WHEN mcr.business_unit = 'JustFab DK' THEN 'JFDK'
           WHEN mcr.business_unit = 'JustFab ES' THEN 'JFES'
           WHEN mcr.business_unit = 'JustFab FR' THEN 'JFFR'
           WHEN mcr.business_unit = 'JustFab NL' THEN 'JFNL'
           WHEN mcr.business_unit = 'JustFab SE' THEN 'JFSE'
           WHEN mcr.business_unit = 'JustFab UK' THEN 'JFUK'
           ELSE mcr.business_unit END                                          display_store,
       CONCAT('JFB', SUBSTR(business_unit, -2, 2), '-',
              'F', '-',
              store_credit_reason)                                             store_credit_type,
       breakage_type

FROM reporting_base_prod.shared.jfb_sxf_membership_credit_breakage_report mcr
WHERE business_unit IN
      ('FabKids US',
       'Fabletics Womens US',
       'JustFab CA',
       'JustFab DE',
       'JustFab DK',
       'JustFab ES',
       'JustFab FR',
       'JustFab NL',
       'JustFab SE',
       'JustFab UK',
       'JustFab US',
       'ShoeDazzle US'
          )
  AND store_credit_reason = 'Membership Credit'
  AND display_store <> 'FLUSDONOTUSE'
  AND NOT (currency = 'Local Net VAT' AND region = 'NA')
  AND CONTAINS(currency, 'Local');
