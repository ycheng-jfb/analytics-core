CREATE OR REPLACE VIEW shared.fabletics_breakage_analysis_base_reset AS

SELECT 'New Token'                                             "Reporting Credit Category",
       brand,
       region                                                  "Region",
       country                                                 "Country",
       currency                                                "Currency",
       original_credit_tender                                  "Original Credit Tender",
       original_credit_reason                                  "Original Credit Reason",
       original_credit_type                                    "Original Credit Type",
       original_issued_month                                   "Original Issued Month",
       original_issued_amount                                  "Original Issued Amount",
       activity_month                                          "Breakage Booking Month",
       credit_tenure                                           "Credit Tenure",
       issued_to_date                                          "Issued To Date",
       redeemed_to_date                                        "Redeemed To Date",
       cancelled_to_date                                       "Cancelled To Date",
       expired_to_date                                         "Expired To Date",
       unredeemed_to_date                                      "Unredeemed To Date",

       expected_redeemed_rate                                  "Expected Cumulative Redeemed Rate",
       expected_redeemed_rate_max                              "Expected M24or19 Cumulative Redeemed Rate",
       expected_unredeemed_rate                                "Expected Cumulative Unredeemed Rate",
       breakage_recorded                                       "Breakage Recorded From Reporting Table",

       expected_additional_redeemed                            "Expected Additional Redeemed",
       expected_additional_cancelled                           "Expected Additional Cancelled",
       expected_additional_activity_amount_m14to19             "Expected Additional Activity Amount M14To19",

       expected_breakage_to_record                             "Expected Breakage From Reporting Table",

       LAG(breakage_recorded)
           OVER (PARTITION BY brand,region,country,currency, original_credit_tender,original_credit_type,
               original_issued_month
               ORDER BY activity_month)                        "Previous Breakage Recorded",

       LAG(redeemed_to_date)
           OVER (PARTITION BY brand,region,country,currency,original_credit_tender,original_credit_type,
               original_issued_month
               ORDER BY activity_month)                        "Previous Redeemed To Date",

       LAG(cancelled_to_date)
           OVER (PARTITION BY brand,region,country,currency,original_credit_tender,original_credit_type,
               original_issued_month
               ORDER BY activity_month)                        "Previous Cancelled To Date",

       LAG(expired_to_date)
           OVER (PARTITION BY brand,region,country,currency,original_credit_tender,original_credit_type,
               original_issued_month
               ORDER BY activity_month)                        "Previous Expired To Date",

       IFF(RANK() OVER (PARTITION BY brand,region,country,original_credit_tender,
           original_credit_type,original_issued_month
           ORDER BY activity_month) = 1, breakage_recorded,
           (breakage_recorded - "Previous Breakage Recorded")) "Change In Breakage To Record",


       IFF(RANK() OVER (PARTITION BY brand,region,country,original_credit_tender,
           original_credit_type,original_issued_month
           ORDER BY activity_month) = 1, redeemed_to_date,
           (redeemed_to_date - "Previous Redeemed To Date"))   "Monthly Redemption",

       IFF(RANK() OVER (PARTITION BY brand,region,country,original_credit_tender,
           original_credit_type,original_issued_month
           ORDER BY activity_month) = 1, cancelled_to_date,
           (cancelled_to_date - "Previous Cancelled To Date")) "Monthly Cancellation",

       IFF(RANK() OVER (PARTITION BY brand,region,country,original_credit_tender,
           original_credit_type,original_issued_month
           ORDER BY activity_month) = 1, expired_to_date,
           (expired_to_date - "Previous Cancelled To Date"))   "Monthly Expiration"

FROM shared.fabletics_token_breakage_report_reset

UNION ALL

SELECT 'Converted Credit'                                      "Reporting Credit Category",
       brand,
       region                                                  "Region",
       country                                                 "Country",
       currency                                                "Currency",
       original_credit_tender                                  "Original Credit Tender",
       original_credit_reason                                  "Original Credit Reason",
       original_credit_type                                    "Original Credit Type",
       original_issued_month                                   "Original Issued Month",
       original_issued_amount                                  "Original Issued Amount",
       activity_month                                          "Breakage Booking Month",
       credit_tenure                                           "Credit Tenure",
       issued_to_date                                          "Issued To Date",
       redeemed_to_date                                        "Redeemed To Date",
       cancelled_to_date                                       "Cancelled To Date",
       expired_to_date                                         "Expired To Date",
       unredeemed_to_date                                      "Unredeemed To Date",

       expected_redeemed_rate                                  "Expected Cumulative Redeemed Rate",
       expected_redeemed_rate_m24                              "Expected M24 Cumulative Redeemed Rate",
       expected_unredeemed_rate                                "Expected Cumulative Unredeemed Rate",
       breakage_recorded                                       "Breakage Recorded From Reporting Table",

       expected_additional_redeemed                            "Expected Additional Redeemed",
       expected_additional_cancelled                           "Expected Additional Cancelled",
       0                                                       "Expected Additional Activity Amount M14To19",

       expected_breakage_to_record                             "Expected Breakage From Reporting Table",

       LAG(breakage_recorded)
           OVER (PARTITION BY brand,region,country,currency, original_credit_tender,original_credit_type,
               original_issued_month
               ORDER BY activity_month)                        "Previous Breakage Recorded",

       LAG(redeemed_to_date)
           OVER (PARTITION BY brand,region,country,currency,original_credit_tender,original_credit_type,
               original_issued_month
               ORDER BY activity_month)                        "Previous Redeemed To Date",

       LAG(cancelled_to_date)
           OVER (PARTITION BY brand,region,country,currency,original_credit_tender,original_credit_type,
               original_issued_month
               ORDER BY activity_month)                        "Previous Cancelled To Date",

       LAG(expired_to_date)
           OVER (PARTITION BY brand,region,country,currency,original_credit_tender,original_credit_type,
               original_issued_month
               ORDER BY activity_month)                        "Previous Expired To Date",


       IFF(RANK() OVER (PARTITION BY brand,region,country,currency,original_credit_tender,
           original_credit_type,original_issued_month
           ORDER BY activity_month) = 1, breakage_recorded,
           (breakage_recorded - "Previous Breakage Recorded")) "Change In Breakage To Record",
       IFF(RANK() OVER (PARTITION BY brand,region,country,currency,original_credit_tender,
           original_credit_type,original_issued_month
           ORDER BY activity_month) = 1, redeemed_to_date,
           (redeemed_to_date - "Previous Redeemed To Date"))   "Monthly Redemption",

       IFF(RANK() OVER (PARTITION BY brand,region,country,currency,original_credit_tender,
           original_credit_type,original_issued_month
           ORDER BY activity_month) = 1, cancelled_to_date,
           (cancelled_to_date - "Previous Cancelled To Date")) "Monthly Cancellation",

       IFF(RANK() OVER (PARTITION BY brand,region,country,currency,original_credit_tender,
           original_credit_type,original_issued_month
           ORDER BY activity_month) = 1, expired_to_date,
           (expired_to_date - "Previous Cancelled To Date"))   "Monthly Expiration"

FROM shared.fabletics_converted_credits_breakage_report;
