CREATE OR REPLACE VIEW breakage.jfb_breakage_report_ssrs_base AS
with nonemp as
    (select business_unit                    target_business_unit,
            recognition_booking_month        last_nonemp_month,
            issued_month,
            sum(breakage_to_record)          breakage_to_record
    from breakage.jfb_sxf_membership_credit_breakage_report last_nonemp_details
    where recognition_booking_month = (select max(recognition_booking_month)
                                        from breakage.jfb_sxf_membership_credit_breakage_report last_nonemp_month
                                        where last_nonemp_month.business_unit=last_nonemp_details.business_unit
                                          and issued_month>='2018-12-01')
      and business_unit in ('FabKids US','JustFab US','ShoeDazzle US')
      and issued_month>='2018-12-01'
      and currency = 'Local Gross VAT'
    group by business_unit,recognition_booking_month,issued_month),

    giftco as
    (select business_unit,
            brand || ' ' || country          target_business_unit,
            recognition_booking_month,
            issued_month                     giftco_issued_month,
            min(recognition_booking_month)   first_token_month,
            sum(redeemed_to_date)            first_token_redeemed,
            sum(cancelled_to_date)           first_token_cancelled,
            sum(breakage_to_record)          first_token_breakage
    from breakage.jfb_converted_breakage_report
    where business_unit like '%Token to Giftco'
    and recognition_booking_month = (select min(recognition_booking_month)
                                          from breakage.jfb_converted_breakage_report
                                      where business_unit like '%Token to Giftco')
    group by business_unit,
             brand || ' ' || country,
             recognition_booking_month,
             issued_month)

select cbr.business_unit,
       currency,
       deferred_recognition_label,
       cbr.store_credit_reason,
       '36x12 + 24x12'                                                                                     assumption_months_baseline,
       cbr.issued_month,
       cbr.recognition_booking_month,
       credit_tenure                                                                                       months_of_activity_from_issuance,
       DATEADD(MONTH, -24, cbr.recognition_booking_month)::DATE                                            credit_cohort_start_blend,
       cbr.recognition_booking_month::DATE                                                                 credit_cohort_end_blend,
       24                                                                                                  blended_credit_cohorts,
       IFF(credit_tenure <= 24, 'Clean Blended Cohorts', 'Actual Rates')                                   blended_credit_cohorts_type,
       issued_to_date                                                                                      issued_amount,
       redeemed_to_date,
       cancelled_to_date,
       expired_to_date,
       unredeemed_to_date,
       0                                                                                                   unredeemed_cant_recognize,
       redeemed_rate_to_date,
       cancelled_to_date_pct,
       unredeemed_to_date_pct,
       expected_redeemed_rate                                                                              assumption_cumulative_redeemed_pct,
       expected_redeemed_rate_max                                                                          assumption_cumulative_redeemed_max_pct,
       assumption_cumulative_cancelled_pct,
       expected_unredeemed_rate                                                                            assumption_cumulative_unredeemed_pct,
       issued_to_date * expected_redeemed_rate                                                             expected_redeemed,
       expected_additional_redeemed,
       issued_to_date * assumption_cumulative_cancelled_pct                                                expected_cancelled,
       expected_additional_cancelled,
       issued_to_date * expected_unredeemed_rate                                                           expected_unredeemed,
       actual_redeemed_rate_vs_expected,
       expected_breakage,
       0                                                                                                   expected_cannot_recognize,
       cbr.breakage_to_record,
       CASE WHEN cbr.business_unit like '%Token to Giftco%'
       THEN IFF(RANK() OVER (PARTITION BY cbr.business_unit,cbr.store_credit_reason,cbr.issued_month
           ORDER BY cbr.recognition_booking_month) = 1, -1 * (cbr.redeemed_to_date + cbr.cancelled_to_date),
           (cbr.breakage_to_record - LAG(cbr.breakage_to_record) OVER
               (PARTITION BY cbr.business_unit,cbr.store_credit_reason,cbr.issued_month
                                                               ORDER BY cbr.recognition_booking_month)))

       WHEN cbr.RECOGNITION_BOOKING_MONTH=first_token_month
       THEN IFF(RANK() OVER (PARTITION BY cbr.business_unit,cbr.store_credit_reason,cbr.issued_month
           ORDER BY cbr.recognition_booking_month) = 1, cbr.breakage_to_record - IFNULL(nonemp.breakage_to_record,0),
           (cbr.breakage_to_record - LAG(cbr.breakage_to_record) OVER
               (PARTITION BY cbr.business_unit,cbr.store_credit_reason,cbr.issued_month
                                                               ORDER BY cbr.recognition_booking_month)))
                + giftco.first_token_breakage + giftco.first_token_redeemed + giftco.first_token_cancelled

       ELSE IFF(RANK() OVER (PARTITION BY cbr.business_unit,cbr.store_credit_reason,cbr.issued_month
           ORDER BY cbr.recognition_booking_month) = 1, cbr.breakage_to_record - IFNULL(nonemp.breakage_to_record,0),
           (cbr.breakage_to_record - LAG(cbr.breakage_to_record) OVER
               (PARTITION BY cbr.business_unit,cbr.store_credit_reason,cbr.issued_month
                                                               ORDER BY cbr.recognition_booking_month)))
       END                                                                                                 change_in_breakage_to_record,
       IFF(blended_credit_cohorts_type = 'Actual Rates', unredeemed_cant_recognize,
           expected_cannot_recognize)                                                                      reporting_cannot_recognize,
       expected_breakage                                                                                   reporting_expected_breakage,
       breakage_type,
       'JFB' || region                                                                                     region,
       CASE
           WHEN cbr.business_unit = 'FabKids US' THEN 'FKUS'
           WHEN cbr.business_unit = 'ShoeDazzle US' THEN 'SDUS'
           WHEN cbr.business_unit = 'JustFab US' THEN 'JFUS'
           WHEN cbr.business_unit = 'FabKids US Token to Giftco' THEN 'FKUS-Token to Giftco'
           WHEN cbr.business_unit = 'ShoeDazzle US Token to Giftco' THEN 'SDUS-Token to Giftco'
           WHEN cbr.business_unit = 'JustFab US Token to Giftco' THEN 'JFUS-Token to Giftco'
           ELSE cbr.business_unit END                                                                       display_store,
       CONCAT(display_store, '-',
              'F', '-', cbr.original_credit_type)                                                           store_credit_type
FROM breakage.jfb_converted_breakage_report cbr
left join nonemp
    on cbr.business_unit=nonemp.target_business_unit
           and cbr.issued_month=nonemp.issued_month
           and cbr.recognition_booking_month=dateadd(month,1,nonemp.last_nonemp_month)
left join giftco
    on cbr.business_unit = giftco.target_business_unit
           and cbr.recognition_booking_month=giftco.first_token_month
           and cbr.issued_month=giftco.giftco_issued_month
WHERE cbr.business_unit IN ('JustFab US', 'FabKids US', 'ShoeDazzle US',
                            'JustFab US Token to Giftco','FabKids US Token to Giftco','ShoeDazzle US Token to Giftco')
  AND NOT (currency = 'Local Net VAT' AND region = 'NA')
  AND CONTAINS(currency, 'Local')

UNION ALL

SELECT business_unit,
       currency,
       deferred_recognition_label,
       store_credit_reason,
       '13x12'                                                                                             assumption_months_baseline,
       issued_month,
       recognition_booking_month,
       credit_tenure                                                                                       months_of_activity_from_issuance,
       (DATEADD(MONTH, -23, recognition_booking_month)::DATE)                                              credit_cohort_start_blend,
       (recognition_booking_month::DATE)                                                                   credit_cohort_end_blend,
       24                                                                                                  blended_credit_cohorts,
       IFF(credit_tenure <= 24, 'Clean Blended Cohorts', 'Actual Rates')                                   blended_credit_cohorts_type,
       issued_to_date                                                                                      issued_amount,
       redeemed_to_date,
       cancelled_to_date,
       expired_to_date,
       unredeemed_to_date,
       0                                                                                                   unredeemed_cant_recognize,
       redeemed_rate_to_date,
       cancelled_to_date_pct,
       unredeemed_to_date_pct,
       expected_redeemed_rate                                                                              assumption_cumulative_redeemed_pct,
       expected_redeemed_rate_max                                                                          assumption_cumulative_redeemed_max_pct,
       assumption_cumulative_cancelled_pct,
       expected_unredeemed_rate                                                                            assumption_cumulative_unredeemed_pct,
       issued_to_date * expected_redeemed_rate                                                             expected_redeemed,
       expected_additional_redeemed,
       issued_to_date * assumption_cumulative_cancelled_pct                                                expected_cancelled,
       expected_additional_cancelled,
       issued_to_date * expected_unredeemed_rate                                                           expected_unredeemed,
       actual_redeemed_rate_vs_expected,
       expected_breakage,
       0                                                                                                   expected_cannot_recognize,
       breakage_to_record,
       IFF(RANK() OVER (PARTITION BY business_unit,store_credit_reason,issued_month
           ORDER BY recognition_booking_month) = 1, breakage_to_record,
           (breakage_to_record - LAG(breakage_to_record) OVER (PARTITION BY business_unit,store_credit_reason,issued_month
                                                                   ORDER BY recognition_booking_month)))   change_in_breakage_to_record,
       0                                                                                                   reporting_cannot_recognize,
       expected_breakage                                                                                   reporting_expected_breakage,
       breakage_type,
       'JFB' || region                                                                                     region,
       CASE
           WHEN business_unit = 'FabKids US' THEN 'FKUS'
           WHEN business_unit = 'ShoeDazzle US' THEN 'SDUS'
           WHEN business_unit = 'JustFab US' THEN 'JFUS'
           ELSE business_unit END                                                                          display_store,
       CONCAT(display_store, '-',
              'F', '-', original_credit_type)                                                              store_credit_type
FROM breakage.jfb_token_breakage_report
WHERE business_unit IN ('JustFab US', 'FabKids US', 'ShoeDazzle US')
  AND NOT (currency = 'Local Net VAT' AND region = 'NA')
  AND CONTAINS(currency, 'Local')

UNION ALL

SELECT business_unit,
       currency,
       deferred_recognition_label,
       store_credit_reason,
       '24x10'                                                                                             assumption_months_baseline,
       issued_month,
       recognition_booking_month,
       credit_tenure                                                                                       months_of_activity_from_issuance,
       DATEADD(MONTH, -23, recognition_booking_month)::DATE                                                credit_cohort_start_blend,
       recognition_booking_month::DATE                                                                     credit_cohort_end_blend,
       24                                                                                                  blended_credit_cohorts,
       IFF(credit_tenure <= 24, 'Clean Blended Cohorts', 'Actual Rates')                                   blended_credit_cohorts_type,
       issued_to_date                                                                                      issued_amount,
       redeemed_to_date,
       cancelled_to_date,
       expired_to_date,
       unredeemed_to_date,
       unredeemed_cant_recognize,
       redeemed_rate_to_date,
       cancelled_to_date_pct,
       unredeemed_to_date_pct,
       expected_redeemed_rate                                                                              assumption_cumulative_redeemed_pct,
       expected_redeemed_rate_max                                                                          assumption_cumulative_redeemed_max_pct,
       assumption_cumulative_cancelled_pct,
       expected_unredeemed_rate                                                                            assumption_cumulative_unredeemed_pct,
       expected_redeemed,
       expected_additional_redeemed,
       expected_cancelled,
       expected_additional_cancelled,
       expected_unredeemed,
       actual_redeemed_rate_vs_expected,
       expected_breakage,
       expected_cannot_recognize,
       breakage_to_record,
       IFF(RANK() OVER (PARTITION BY business_unit,currency,deferred_recognition_label,store_credit_reason,issued_month
           ORDER BY recognition_booking_month) = 1, breakage_to_record,
           (breakage_to_record - LAG(breakage_to_record) OVER (PARTITION BY business_unit,currency,deferred_recognition_label,store_credit_reason,issued_month
                                                                   ORDER BY recognition_booking_month)))   change_in_breakage_to_record,
       IFF(blended_credit_cohorts_type = 'Actual Rates', unredeemed_cant_recognize,
           expected_cannot_recognize)                                                                      reporting_cannot_recognize,
       expected_breakage                                                                                   reporting_expected_breakage,
       breakage_type,
       'JFB' || region                                                                                     region,--new column
       CASE
           WHEN business_unit = 'FabKids US' AND issued_month < '2018-12-01' THEN 'FKUS-Giftco'
           WHEN business_unit = 'FabKids US' THEN 'FKUS'
           WHEN business_unit = 'ShoeDazzle US' AND issued_month < '2018-12-01' THEN 'SDUS-Giftco'
           WHEN business_unit = 'ShoeDazzle US' THEN 'SDUS'
           WHEN business_unit = 'JustFab US' AND issued_month < '2018-12-01' THEN 'JFUS-Giftco'
           WHEN business_unit = 'JustFab US' THEN 'JFUS'
           WHEN business_unit = 'Fabletics Womens US' AND issued_month < '2018-12-01' THEN 'FLUS-F-Giftco'
           WHEN business_unit = 'Fabletics Womens US' THEN 'FLUSDONOTUSE'
           WHEN business_unit = 'JustFab CA' THEN 'JFCA'
           WHEN business_unit = 'JustFab DE' THEN 'JFDE'
           WHEN business_unit = 'JustFab DK' THEN 'JFDK'
           WHEN business_unit = 'JustFab ES' THEN 'JFES'
           WHEN business_unit = 'JustFab FR' THEN 'JFFR'
           WHEN business_unit = 'JustFab NL' THEN 'JFNL'
           WHEN business_unit = 'JustFab SE' THEN 'JFSE'
           WHEN business_unit = 'JustFab UK' THEN 'JFUK'
           ELSE business_unit END                                                                          display_store,
       CONCAT(display_store, '-',
              'F', '-', original_credit_type)                                                              store_credit_type
FROM breakage.jfb_sxf_membership_credit_breakage_report
WHERE business_unit IN ('FabKids US',
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
                        'ShoeDazzle US',
                        'FabKids US Giftco 2',
                        'JustFab US Giftco 2',
                        'ShoeDazzle US Giftco 2'
    )
  AND store_credit_reason = 'Membership Credit'
  AND display_store <> 'FLUSDONOTUSE'
  AND NOT (currency = 'Local Net VAT' AND region = 'NA')
  AND CONTAINS(currency, 'Local');
