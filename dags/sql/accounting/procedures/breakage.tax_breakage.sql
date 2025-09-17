SET target_date = DATEADD(MONTH, -1, DATE_TRUNC(MONTH, CURRENT_DATE));
SET execution_datetime = CURRENT_TIMESTAMP();

CREATE OR REPLACE TEMPORARY TABLE _fabletics_breakage AS
SELECT currency,
       IFF(region LIKE '%EU%', 'EU', 'NA') AS region,
       LEFT(display_store, 4)              AS display_store,
       issued_month,
       YEAR(issued_month)                  AS issued_year,
       business_unit,
       issued_amount,
       redeemed_to_date,
       expired_to_date,
       unredeemed_to_date,
       breakage_to_record,
       store_credit_type,
       deferred_recognition_label,
       store_credit_reason,
       assumption_months_baseline,
       recognition_booking_month,
       months_of_activity_from_issuance,
       credit_cohort_start_blend,
       credit_cohort_end_blend,
       blended_credit_cohorts,
       blended_credit_cohorts_type
FROM breakage.fabletics_breakage_report_ssrs_base
WHERE currency IN ('Local Gross VAT', 'Local Net VAT')
  AND recognition_booking_month = $target_date;

CREATE OR REPLACE TEMPORARY TABLE _savagex_breakage AS
SELECT currency,
       IFF(region LIKE '%EU%', 'EU', 'NA') AS region,
       LEFT(display_store, 4)              AS display_store,
       issued_month,
       YEAR(issued_month)                  AS issued_year,
       business_unit,
       issued_amount,
       redeemed_to_date,
       expired_to_date,
       unredeemed_to_date,
       breakage_to_record,
       store_credit_type,
       deferred_recognition_label,
       store_credit_reason,
       assumption_months_baseline,
       recognition_booking_month,
       months_of_activity_from_issuance,
       credit_cohort_start_blend,
       credit_cohort_end_blend,
       blended_credit_cohorts,
       blended_credit_cohorts_type
FROM breakage.sxf_breakage_report_ssrs_base
WHERE currency IN ('Local Gross VAT', 'Local Net VAT')
  AND recognition_booking_month = $target_date;

CREATE OR REPLACE TEMPORARY TABLE _jfb_breakage AS
SELECT currency,
       IFF(region LIKE '%EU%', 'EU', 'NA') AS region,
       CASE
           WHEN display_store LIKE '%Giftco'
               THEN LEFT(display_store, 4) || '-150'
           WHEN LEFT(display_store, 4) = 'JFUS' THEN 'JFUS-110'
           WHEN LEFT(display_store, 4) = 'FKUS' THEN 'FKUS-130'
           WHEN LEFT(display_store, 4) = 'SDUS' THEN 'SDUS-120'
           ELSE LEFT(display_store, 4)
           END                             AS display_store,
       issued_month,
       YEAR(issued_month)                  AS issued_year,
       business_unit,
       issued_amount,
       redeemed_to_date,
       expired_to_date,
       unredeemed_to_date,
       breakage_to_record,
       store_credit_type,
       deferred_recognition_label,
       store_credit_reason,
       assumption_months_baseline,
       recognition_booking_month,
       months_of_activity_from_issuance,
       credit_cohort_start_blend,
       credit_cohort_end_blend,
       blended_credit_cohorts,
       blended_credit_cohorts_type
FROM breakage.jfb_breakage_report_ssrs_base
WHERE currency IN ('Local Gross VAT', 'Local Net VAT')
  AND recognition_booking_month = $target_date;

CREATE OR REPLACE TEMPORARY TABLE _combined_breakage_base AS
SELECT *
FROM _fabletics_breakage
UNION ALL
SELECT *
FROM _savagex_breakage
UNION ALL
SELECT *
FROM _jfb_breakage;

CREATE OR REPLACE TEMPORARY TABLE _combined_breakage AS
SELECT currency,
       region,
       display_store,
       issued_year,
       issued_amount,
       redeemed_to_date,
       expired_to_date,
       unredeemed_to_date,
       breakage_to_record
FROM _combined_breakage_base
WHERE recognition_booking_month = $target_date;

CREATE OR REPLACE TEMPORARY TABLE _breakage_mix_vat AS
SELECT 'Mix VAT'                                                AS currency,
       COALESCE(gross_vat.region, net_vat.region)               AS region,
       COALESCE(gross_vat.display_store, net_vat.display_store) AS display_store,
       COALESCE(gross_vat.issued_year, net_vat.issued_year)     AS issued_year,
       CASE
           WHEN (RIGHT(COALESCE(gross_vat.business_unit, net_vat.business_unit), 2) = 'ES' AND
                 COALESCE(net_vat.issued_month, gross_vat.issued_month) <= '2017-03-01'
                    )
               OR (RIGHT(COALESCE(gross_vat.business_unit, net_vat.business_unit), 2) = 'FR' AND
                   COALESCE(net_vat.issued_month, gross_vat.issued_month) <= '2019-05-01'
                    )
               OR (RIGHT(COALESCE(gross_vat.business_unit, net_vat.business_unit), 2) = 'DE' AND
                   COALESCE(net_vat.issued_month, gross_vat.issued_month) <= '2018-12-01'
                    )
               OR (RIGHT(COALESCE(gross_vat.business_unit, net_vat.business_unit), 2) = 'NL' AND
                   COALESCE(net_vat.issued_month, gross_vat.issued_month) <= '2018-12-01'
                    )
               OR (RIGHT(COALESCE(gross_vat.business_unit, net_vat.business_unit), 2) = 'UK' AND
                   COALESCE(net_vat.issued_month, gross_vat.issued_month) <= '2014-05-01'
                    )
               OR (RIGHT(COALESCE(gross_vat.business_unit, net_vat.business_unit), 2) = 'DK' AND
                   COALESCE(net_vat.issued_month, gross_vat.issued_month) <= '2018-12-01'
                    )
               OR RIGHT(COALESCE(gross_vat.business_unit, net_vat.business_unit), 2) IN ('US', 'CA')
               THEN COALESCE(gross_vat.issued_amount, 0)
           ELSE COALESCE(net_vat.issued_amount, 0)
           END                                                  AS issued_amount,
       CASE
           WHEN (RIGHT(COALESCE(gross_vat.business_unit, net_vat.business_unit), 2) = 'ES' AND
                 COALESCE(net_vat.issued_month, gross_vat.issued_month) <= '2017-03-01'
                    )
               OR (RIGHT(COALESCE(gross_vat.business_unit, net_vat.business_unit), 2) = 'FR' AND
                   COALESCE(net_vat.issued_month, gross_vat.issued_month) <= '2019-05-01'
                    )
               OR (RIGHT(COALESCE(gross_vat.business_unit, net_vat.business_unit), 2) = 'DE' AND
                   COALESCE(net_vat.issued_month, gross_vat.issued_month) <= '2018-12-01'
                    )
               OR (RIGHT(COALESCE(gross_vat.business_unit, net_vat.business_unit), 2) = 'NL' AND
                   COALESCE(net_vat.issued_month, gross_vat.issued_month) <= '2018-12-01'
                    )
               OR (RIGHT(COALESCE(gross_vat.business_unit, net_vat.business_unit), 2) = 'UK' AND
                   COALESCE(net_vat.issued_month, gross_vat.issued_month) <= '2014-05-01'
                    )
               OR (RIGHT(COALESCE(gross_vat.business_unit, net_vat.business_unit), 2) = 'DK' AND
                   COALESCE(net_vat.issued_month, gross_vat.issued_month) <= '2018-12-01'
                    )
               OR RIGHT(COALESCE(gross_vat.business_unit, net_vat.business_unit), 2) IN ('US', 'CA')
               THEN COALESCE(gross_vat.redeemed_to_date, 0)
           ELSE COALESCE(net_vat.redeemed_to_date, 0)
           END                                                  AS redeemed_to_date,
       CASE
           WHEN (RIGHT(COALESCE(gross_vat.business_unit, net_vat.business_unit), 2) = 'ES' AND
                 COALESCE(net_vat.issued_month, gross_vat.issued_month) <= '2017-03-01'
                    )
               OR (RIGHT(COALESCE(gross_vat.business_unit, net_vat.business_unit), 2) = 'FR' AND
                   COALESCE(net_vat.issued_month, gross_vat.issued_month) <= '2019-05-01'
                    )
               OR (RIGHT(COALESCE(gross_vat.business_unit, net_vat.business_unit), 2) = 'DE' AND
                   COALESCE(net_vat.issued_month, gross_vat.issued_month) <= '2018-12-01'
                    )
               OR (RIGHT(COALESCE(gross_vat.business_unit, net_vat.business_unit), 2) = 'NL' AND
                   COALESCE(net_vat.issued_month, gross_vat.issued_month) <= '2018-12-01'
                    )
               OR (RIGHT(COALESCE(gross_vat.business_unit, net_vat.business_unit), 2) = 'UK' AND
                   COALESCE(net_vat.issued_month, gross_vat.issued_month) <= '2014-05-01'
                    )
               OR (RIGHT(COALESCE(gross_vat.business_unit, net_vat.business_unit), 2) = 'DK' AND
                   COALESCE(net_vat.issued_month, gross_vat.issued_month) <= '2018-12-01'
                    )
               OR RIGHT(COALESCE(gross_vat.business_unit, net_vat.business_unit), 2) IN ('US', 'CA')
               THEN COALESCE(gross_vat.expired_to_date, 0)
           ELSE COALESCE(net_vat.expired_to_date, 0)
           END                                                  AS expired_to_date,
       CASE
           WHEN (RIGHT(COALESCE(gross_vat.business_unit, net_vat.business_unit), 2) = 'ES' AND
                 COALESCE(net_vat.issued_month, gross_vat.issued_month) <= '2017-03-01'
                    )
               OR (RIGHT(COALESCE(gross_vat.business_unit, net_vat.business_unit), 2) = 'FR' AND
                   COALESCE(net_vat.issued_month, gross_vat.issued_month) <= '2019-05-01'
                    )
               OR (RIGHT(COALESCE(gross_vat.business_unit, net_vat.business_unit), 2) = 'DE' AND
                   COALESCE(net_vat.issued_month, gross_vat.issued_month) <= '2018-12-01'
                    )
               OR (RIGHT(COALESCE(gross_vat.business_unit, net_vat.business_unit), 2) = 'NL' AND
                   COALESCE(net_vat.issued_month, gross_vat.issued_month) <= '2018-12-01'
                    )
               OR (RIGHT(COALESCE(gross_vat.business_unit, net_vat.business_unit), 2) = 'UK' AND
                   COALESCE(net_vat.issued_month, gross_vat.issued_month) <= '2014-05-01'
                    )
               OR (RIGHT(COALESCE(gross_vat.business_unit, net_vat.business_unit), 2) = 'DK' AND
                   COALESCE(net_vat.issued_month, gross_vat.issued_month) <= '2018-12-01'
                    )
               OR RIGHT(COALESCE(gross_vat.business_unit, net_vat.business_unit), 2) IN ('US', 'CA')
               THEN COALESCE(gross_vat.unredeemed_to_date, 0)
           ELSE COALESCE(net_vat.unredeemed_to_date, 0)
           END                                                  AS unredeemed_to_date,
       CASE
           WHEN (RIGHT(COALESCE(gross_vat.business_unit, net_vat.business_unit), 2) = 'ES' AND
                 COALESCE(net_vat.issued_month, gross_vat.issued_month) <= '2017-03-01'
                    )
               OR (RIGHT(COALESCE(gross_vat.business_unit, net_vat.business_unit), 2) = 'FR' AND
                   COALESCE(net_vat.issued_month, gross_vat.issued_month) <= '2019-05-01'
                    )
               OR (RIGHT(COALESCE(gross_vat.business_unit, net_vat.business_unit), 2) = 'DE' AND
                   COALESCE(net_vat.issued_month, gross_vat.issued_month) <= '2018-12-01'
                    )
               OR (RIGHT(COALESCE(gross_vat.business_unit, net_vat.business_unit), 2) = 'NL' AND
                   COALESCE(net_vat.issued_month, gross_vat.issued_month) <= '2018-12-01'
                    )
               OR (RIGHT(COALESCE(gross_vat.business_unit, net_vat.business_unit), 2) = 'UK' AND
                   COALESCE(net_vat.issued_month, gross_vat.issued_month) <= '2014-05-01'
                    )
               OR (RIGHT(COALESCE(gross_vat.business_unit, net_vat.business_unit), 2) = 'DK' AND
                   COALESCE(net_vat.issued_month, gross_vat.issued_month) <= '2018-12-01'
                    )
               OR RIGHT(COALESCE(gross_vat.business_unit, net_vat.business_unit), 2) IN ('US', 'CA')
               THEN COALESCE(gross_vat.breakage_to_record, 0)
           ELSE COALESCE(net_vat.breakage_to_record, 0)
           END                                                  AS breakage_to_record
FROM (SELECT region,
             display_store,
             issued_month,
             issued_year,
             business_unit,
             issued_amount,
             redeemed_to_date,
             expired_to_date,
             unredeemed_to_date,
             breakage_to_record,
             store_credit_type,
             deferred_recognition_label,
             store_credit_reason,
             assumption_months_baseline,
             recognition_booking_month,
             months_of_activity_from_issuance,
             credit_cohort_start_blend,
             credit_cohort_end_blend,
             blended_credit_cohorts,
             blended_credit_cohorts_type
      FROM _combined_breakage_base
      WHERE currency = 'Local Gross VAT') AS gross_vat
         FULL OUTER JOIN (SELECT region,
                                 display_store,
                                 issued_month,
                                 issued_year,
                                 business_unit,
                                 issued_amount,
                                 redeemed_to_date,
                                 expired_to_date,
                                 unredeemed_to_date,
                                 breakage_to_record,
                                 store_credit_type,
                                 deferred_recognition_label,
                                 store_credit_reason,
                                 assumption_months_baseline,
                                 recognition_booking_month,
                                 months_of_activity_from_issuance,
                                 credit_cohort_start_blend,
                                 credit_cohort_end_blend,
                                 blended_credit_cohorts,
                                 blended_credit_cohorts_type
                          FROM _combined_breakage_base
                          WHERE currency = 'Local Net VAT') AS net_vat
                         ON
                                     gross_vat.business_unit = net_vat.business_unit
                                 AND gross_vat.display_store = net_vat.display_store
                                 AND gross_vat.region = net_vat.region
                                 AND gross_vat.store_credit_type = net_vat.store_credit_type
                                 AND gross_vat.deferred_recognition_label = net_vat.deferred_recognition_label
                                 AND gross_vat.store_credit_reason = net_vat.store_credit_reason
                                 AND gross_vat.assumption_months_baseline = net_vat.assumption_months_baseline
                                 AND gross_vat.issued_month = net_vat.issued_month
                                 AND gross_vat.issued_year = net_vat.issued_year
                                 AND gross_vat.recognition_booking_month = net_vat.recognition_booking_month
                                 AND gross_vat.months_of_activity_from_issuance =
                                     net_vat.months_of_activity_from_issuance
                                 AND gross_vat.credit_cohort_start_blend = net_vat.credit_cohort_start_blend
                                 AND gross_vat.credit_cohort_end_blend = net_vat.credit_cohort_end_blend
                                 AND gross_vat.blended_credit_cohorts = net_vat.blended_credit_cohorts
                                 AND gross_vat.blended_credit_cohorts_type = net_vat.blended_credit_cohorts_type;

CREATE OR REPLACE TEMPORARY TABLE _breakage_vat_unioned AS
SELECT *
FROM _combined_breakage
UNION ALL
SELECT *
FROM _breakage_mix_vat;

CREATE OR REPLACE TRANSIENT TABLE breakage.tax_breakage AS
SELECT currency,
       region,
       display_store,
       issued_year,
       ROUND(SUM(issued_amount), 2)      AS issued_amount,
       ROUND(SUM(redeemed_to_date), 2)   AS redeemed_to_date,
       ROUND(SUM(expired_to_date), 2)    AS expired_to_date,
       ROUND(SUM(unredeemed_to_date), 2) AS unredeemed_to_date,
       ROUND(SUM(breakage_to_record), 2) AS breakage_to_record
FROM _breakage_vat_unioned
GROUP BY currency, region, display_store, issued_year
ORDER BY currency, region, display_store, issued_year;

INSERT INTO breakage.tax_breakage_snapshot
SELECT currency,
       region,
       display_store,
       issued_year,
       issued_amount,
       redeemed_to_date,
       unredeemed_to_date,
       breakage_to_record,
       $target_date        AS target_date,
       $execution_datetime AS snapshot_timestamp,
       $execution_datetime AS meta_create_datetime,
       $execution_datetime AS meta_update_datetime,
       expired_to_date
FROM breakage.tax_breakage;
