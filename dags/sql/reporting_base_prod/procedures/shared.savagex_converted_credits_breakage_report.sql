SET month_thru = DATEADD(MONTH, -1, DATE_TRUNC(MONTH, CURRENT_TIMESTAMP()::DATE));

CREATE OR REPLACE TEMPORARY TABLE _append_sx_converted AS
WITH _original_credit_activity AS
         (SELECT CASE
                     WHEN store_brand = 'Fabletics' AND gender = 'M' THEN 'Fabletics Mens'
                     WHEN store_brand = 'Fabletics' THEN 'Fabletics Womens'
                     ELSE store_brand END                                             AS brand,
                 store_country                                                        AS country,
                 store_region                                                         AS region,
                 credit_activity_type,
                 original_credit_tender,
                 original_credit_type,
                 original_credit_reason,
                 DATE_TRUNC('month', dcf.original_credit_issued_local_datetime::DATE) AS original_issued_month,
                 DATE_TRUNC('month', fce.credit_activity_local_datetime::DATE)        AS activity_month,
                 SUM(fce.credit_activity_gross_vat_local_amount)                      AS activity_amount
          FROM shared.dim_credit dcf -- new credit key
                   JOIN edw_prod.stg.dim_customer dc ON dc.customer_id = dcf.customer_id
                   JOIN shared.fact_credit_event fce
                        ON dcf.credit_key = fce.credit_key -- join to get all activity (need join to be on new to capture all activity, we just pull attributes from the original)
                   JOIN edw_prod.data_model.dim_store st ON st.store_id = dcf.store_id
          WHERE fce.credit_activity_type IN
                ('Issued', 'Redeemed', 'Cancelled', 'Expired')       -- only include the activity we want to consider
            AND fce.original_credit_activity_type_action = 'Include' -- not including issued-exclude on purpose so that we include the Activity (Redemption,Cancellation,Expiration activity of the credits))
            AND st.store_brand = 'Savage X'
            AND st.store_country = 'US'
          GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9)
    ,
     _pivot_extended
         AS -- only for SXF with 18 months extended after 2022-01-01. For other brands, use _pivot code (above this table) instead
         (WITH _month AS (SELECT month_date                                       calendar_month,
                                 YEAR(month_date) || '-' || MONTHNAME(month_date) month_name
                          FROM edw_prod.data_model.dim_date),
               _issuance AS (SELECT *
                             FROM _original_credit_activity c
                                      JOIN _month dm
                                           ON dm.calendar_month >= c.original_issued_month
                                               AND dm.calendar_month <= DATEADD(MONTH, 24, CURRENT_DATE())
                             WHERE credit_activity_type = 'Issued')
          SELECT DISTINCT i.brand,
                          i.region,
                          i.country,
                          i.original_credit_tender,
                          i.original_credit_type,
                          i.original_credit_reason,
                          i.original_issued_month,
                          i.activity_amount                                  original_issued_amount,
                          COALESCE(p.activity_month::DATE, i.calendar_month) activity_month,
                          NVL(p."'Issued'", 0)                               issued,
                          NVL(p."'Redeemed'", 0)                             redeemed,
                          NVL(p."'Cancelled'", 0)                            cancelled,
                          NVL(p."'Expired'", 0)                              expired
          FROM _issuance i
                   LEFT JOIN _original_credit_activity
              PIVOT (SUM(activity_amount)
              FOR credit_activity_type IN ('Issued','Redeemed', 'Cancelled', 'Expired')) p
                             ON
                                         i.brand = p.brand
                                     AND i.region = p.region
                                     AND i.country = p.country
                                     AND i.original_credit_tender = p.original_credit_tender
                                     AND i.original_credit_type = p.original_credit_type
                                     AND i.original_credit_reason = p.original_credit_reason
                                     AND i.activity_month = p.original_issued_month
                                     AND i.calendar_month = p.activity_month)
    ,
     _sx_base AS
         (SELECT brand,
                 region,
                 country,
                 original_credit_tender,
                 original_credit_reason,
                 original_credit_type,
                 original_issued_month,
                 original_issued_amount,
                 activity_month,
                 DATEDIFF(MONTH, original_issued_month, activity_month) + 1 credit_tenure,
                 SUM(issued)                                                issued,
                 SUM(redeemed)                                              redeemed,
                 SUM(cancelled)                                             cancelled,
                 SUM(expired)                                               expired
          FROM _pivot_extended -- only for SXF, use _pivot for other brands
          WHERE 1 = 1
            AND original_credit_tender = 'Cash'
            AND ((original_credit_type = 'Fixed Credit' AND
                  original_credit_reason = 'Membership Credit')) -- ONLY CREDITS, NO TOKENS
            AND original_issued_month >= '2019-02-01'
          GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    ,
     _issued AS (SELECT brand,
                        region,
                        country,
                        original_credit_tender,
                        original_credit_reason,
                        original_credit_type,
                        original_issued_month,
                        original_issued_amount,
                        $month_thru                                                   activity_month,
                        DATEDIFF(MONTH, original_issued_month, $month_thru) + 1       credit_tenure,
                        DATEDIFF(MONTH, original_issued_month, '2022-01-01') + 1 + 23 projected_unredeem_breakage_tenure, -- tenure until the conversion month (2022-01-01) adds the projected months (18 months)
                        DATEDIFF(MONTH, original_issued_month, '2022-01-01') + 1 + 11 projected_redeem_breakage_tenure,
                        DATEDIFF(MONTH, original_issued_month, '2022-01-01') + 1 + 23 projected_redeem_breakage_tenure_m24,

                        NVL(SUM(issued), 0)                                           i,
                        NVL(SUM(redeemed), 0)                                         r,
                        NVL(SUM(cancelled), 0)                                        c,
                        NVL(SUM(expired), 0)                                          e,
                        i - r - c                                                     unrd
                 FROM _sx_base b
                 WHERE original_issued_month BETWEEN '2019-02-01' AND $month_thru
                   AND activity_month BETWEEN '2019-02-01' AND $month_thru
                   AND original_credit_reason = 'Membership Credit'
                   AND original_credit_type = 'Fixed Credit'
                 GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9)
SELECT i.brand,
       i.region,
       i.country,
       i.original_credit_tender,
       i.original_credit_reason,
       i.original_credit_type,
       i.original_issued_month,
       i.original_issued_amount,
       i.activity_month,
       i.credit_tenure,
       i.projected_redeem_breakage_tenure,
       i.projected_unredeem_breakage_tenure,
       i.i                                                                            issued_to_date,
       i.r                                                                            redeemed_to_date,
       i.c                                                                            cancelled_to_date,
       i.e                                                                            expired_to_date,
       i.unrd                                                                         unredeemed_to_date,
       NULL                                                                           expected_cumulative_redeemed_rate,
       NULL                                                                           expected_cumulative_unredeemed_rate,
       i.unrd                                                                         expected_breakage_to_record,
       i.unrd                                                                         breakage_recorded,
       0 expected_cumulative_redeemed_rate_m24,
       0 expected_additional_redeemed,
       0 expected_additional_cancelled
FROM _issued i;

MERGE INTO shared.savagex_converted_credits_breakage_report t --work.dbo.sxf_breakage_report_converted_credits_24x12_fixed_assumption t
    USING _append_sx_converted a
    ON t.brand = a.brand AND
       t.region = a.region AND
       t.country = a.country AND
       t.original_credit_tender = a.original_credit_tender AND
       t.original_credit_reason = a.original_credit_reason AND
       t.original_credit_type = a.original_credit_type AND
       t.original_issued_month = a.original_issued_month AND
       t.activity_month = a.activity_month AND
       t.credit_tenure = a.credit_tenure
    WHEN NOT MATCHED THEN
        INSERT (brand, region, country, original_credit_tender, original_credit_reason, original_credit_type,
                original_issued_amount,
                original_issued_month, activity_month,
                credit_tenure, projected_redeem_breakage_tenure, projected_unredeem_breakage_tenure, issued_to_date,
                redeemed_to_date, cancelled_to_date,
                expired_to_date,
                unredeemed_to_date, expected_cumulative_redeemed_rate,
                expected_cumulative_unredeemed_rate, breakage_recorded, expected_cumulative_redeemed_rate_m24,
                expected_breakage_to_record,
                expected_additional_redeemed, expected_additional_cancelled)
            VALUES (brand, region, country, original_credit_tender, original_credit_reason, original_credit_type,
                    original_issued_amount,
                    original_issued_month, activity_month,
                    credit_tenure, projected_redeem_breakage_tenure, projected_unredeem_breakage_tenure, issued_to_date,
                    redeemed_to_date, cancelled_to_date,
                    expired_to_date,
                    unredeemed_to_date, expected_cumulative_redeemed_rate,
                    expected_cumulative_unredeemed_rate, breakage_recorded, expected_cumulative_redeemed_rate_m24,
                    expected_breakage_to_record,
                    expected_additional_redeemed, expected_additional_cancelled)
    WHEN MATCHED AND
        (NOT EQUAL_NULL(t.issued_to_date, a.issued_to_date) OR
         NOT EQUAL_NULL(t.redeemed_to_date, a.redeemed_to_date) OR
         NOT EQUAL_NULL(t.cancelled_to_date, a.cancelled_to_date) OR
         NOT EQUAL_NULL(t.expired_to_date, a.expired_to_date) OR
         NOT EQUAL_NULL(t.unredeemed_to_date, a.unredeemed_to_date) OR
         NOT EQUAL_NULL(t.expected_cumulative_redeemed_rate, a.expected_cumulative_redeemed_rate) OR
         NOT EQUAL_NULL(t.expected_cumulative_unredeemed_rate, a.expected_cumulative_unredeemed_rate) OR
         NOT EQUAL_NULL(t.expected_breakage_to_record, a.expected_breakage_to_record) OR
         NOT EQUAL_NULL(t.breakage_recorded, a.breakage_recorded) OR
         NOT EQUAL_NULL(t.expected_cumulative_redeemed_rate_m24, a.expected_cumulative_redeemed_rate_m24) OR
         NOT EQUAL_NULL(t.expected_additional_cancelled, a.expected_additional_cancelled))
        THEN
        UPDATE
            SET t.issued_to_date = a.issued_to_date,
                t.redeemed_to_date = a.redeemed_to_date,
                t.cancelled_to_date = a.cancelled_to_date,
                t.expired_to_date = a.expired_to_date,
                t.unredeemed_to_date = a.unredeemed_to_date,
                t.expected_cumulative_redeemed_rate = a.expected_cumulative_redeemed_rate,
                t.expected_cumulative_unredeemed_rate = a.expected_cumulative_unredeemed_rate,
                t.breakage_recorded = a.breakage_recorded,
                t.expected_breakage_to_record = a.expected_breakage_to_record,
                t.expected_cumulative_redeemed_rate_m24 = a.expected_cumulative_redeemed_rate_m24,
                t.expected_additional_redeemed = a.expected_additional_redeemed,
                t.expected_additional_cancelled = a.expected_additional_cancelled;

