CREATE OR REPLACE VIEW breakage.fabletics_breakage_analysis_base AS
SELECT 'New Token'                                             "Reporting Credit Category",
    brand,
    region                                                  "Region",
    country                                                 "Country",
    currency                                                "Currency",
    'Recognize'                                             "Deferred Recognition Label",
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
       OVER (PARTITION BY brand,region,country, currency, original_credit_tender,original_credit_type,
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
    IFF(RANK() OVER (PARTITION BY brand,region,country, currency,original_credit_tender,
       original_credit_type,original_issued_month
       ORDER BY activity_month) = 1, breakage_recorded,
       (breakage_recorded - "Previous Breakage Recorded")) "Change In Breakage To Record",
    IFF(RANK() OVER (PARTITION BY brand,region,country, currency, original_credit_tender,
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
FROM breakage.fabletics_token_breakage_report

UNION ALL

SELECT 'Converted Credit'                                                     "Reporting Credit Category",
    brand,
    region                                                                 "Region",
    country                                                                "Country",
    currency                                                               "Currency",
   'Recognize'                                                             "Deferred Recognition Label",
    original_credit_tender                                                 "Original Credit Tender",
    original_credit_reason                                                 "Original Credit Reason",
    original_credit_type                                                   "Original Credit Type",
    original_issued_month                                                  "Original Issued Month",
    original_issued_amount                                                 "Original Issued Amount",
    activity_month                                                         "Breakage Booking Month",
    credit_tenure                                                          "Credit Tenure",
    issued_to_date                                                         "Issued To Date",
    redeemed_to_date                                                       "Redeemed To Date",
    cancelled_to_date                                                      "Cancelled To Date",
    expired_to_date                                                        "Expired To Date",
    unredeemed_to_date                                                     "Unredeemed To Date",
    expected_redeemed_rate                                                 "Expected Cumulative Redeemed Rate",
    expected_redeemed_rate_m24                                             "Expected M24 Cumulative Redeemed Rate",
    expected_unredeemed_rate                                               "Expected Cumulative Unredeemed Rate",
    breakage_recorded                                                      "Breakage Recorded From Reporting Table",
    expected_additional_redeemed                                           "Expected Additional Redeemed",
    expected_additional_cancelled                                          "Expected Additional Cancelled",
    0                                                                      "Expected Additional Activity Amount M14To19",
    expected_breakage_to_record                                            "Expected Breakage From Reporting Table",
    LAG(breakage_recorded)
       OVER (PARTITION BY brand,region,country, currency, original_credit_tender,original_credit_type,
           original_issued_month
           ORDER BY activity_month)                                       "Previous Breakage Recorded",
    LAG(redeemed_to_date)
       OVER (PARTITION BY brand,region,country,currency, original_credit_tender,original_credit_type,
           original_issued_month
           ORDER BY activity_month)                                       "Previous Redeemed To Date",
    LAG(cancelled_to_date)
       OVER (PARTITION BY brand,region,country,currency, original_credit_tender,original_credit_type,
           original_issued_month
           ORDER BY activity_month)                                       "Previous Cancelled To Date",
    LAG(expired_to_date)
       OVER (PARTITION BY brand,region,country,currency, original_credit_tender,original_credit_type,
           original_issued_month
           ORDER BY activity_month)                                       "Previous Expired To Date",
    IFF(RANK() OVER (PARTITION BY brand,region,country,currency, original_credit_tender,
       original_credit_type,original_issued_month
       ORDER BY activity_month) = 1, breakage_recorded,
       (breakage_recorded - "Previous Breakage Recorded"))                "Change In Breakage To Record",
    IFF(RANK() OVER (PARTITION BY brand,region,country,currency, original_credit_tender,
       original_credit_type,original_issued_month
       ORDER BY activity_month) = 1, redeemed_to_date,
       (redeemed_to_date - "Previous Redeemed To Date"))                  "Monthly Redemption",
    IFF(RANK() OVER (PARTITION BY brand,region,country,currency, original_credit_tender,
       original_credit_type,original_issued_month
       ORDER BY activity_month) = 1, cancelled_to_date,
       (cancelled_to_date - "Previous Cancelled To Date"))                "Monthly Cancellation",
    IFF(RANK() OVER (PARTITION BY brand,region,country,currency, original_credit_tender,
       original_credit_type,original_issued_month
       ORDER BY activity_month) = 1, expired_to_date,
       (expired_to_date - "Previous Cancelled To Date"))                  "Monthly Expiration"
FROM  breakage.fabletics_converted_credits_breakage_report

UNION ALL
/*Refund Variable Credits*/
SELECT original_credit_type                                 "Reporting Credit Category",
    brand,
    region                                                  "Region",
    country                                                 "Country",
    currency                                                "Currency",
    deferred_recognition_label                                           "Deferred Recognition Label",
    original_credit_tender                                  "Original Credit Tender",
    store_credit_reason                                     "Original Credit Reason",
    original_credit_type                                    "Original Credit Type",
    issued_month                                            "Original Issued Month",
    original_issued_amount                                  "Original Issued Amount",
    recognition_booking_month                               "Breakage Booking Month",
    credit_tenure                                           "Credit Tenure",
    issued_to_date                                          "Issued To Date",
    redeemed_to_date                                        "Redeemed To Date",
    cancelled_to_date                                       "Cancelled To Date",
    expired_to_date                                         "Expired To Date",
    unredeemed_to_date                                      "Unredeemed To Date",
    expected_redeemed_rate                                  "Expected Cumulative Redeemed Rate",
    expected_redeemed_rate_max                              "Expected M24 Cumulative Redeemed Rate",
    expected_unredeemed_rate                                "Expected Cumulative Unredeemed Rate",
    breakage_to_record                                      "Breakage Recorded From Reporting Table",
    expected_additional_redeemed                            "Expected Additional Redeemed",
    expected_additional_cancelled                           "Expected Additional Cancelled",
    0                                                       "Expected Additional Activity Amount M14To19",
    expected_breakage                                       "Expected Breakage From Reporting Table",
    LAG(breakage_to_record)
       OVER (PARTITION BY brand,region,country, currency, original_credit_tender,original_credit_type,deferred_recognition_label,
           issued_month
           ORDER BY recognition_booking_month)                                       "Previous Breakage Recorded",
    LAG(redeemed_to_date)
       OVER (PARTITION BY brand,region,country,currency, original_credit_tender,original_credit_type,deferred_recognition_label,
           issued_month
           ORDER BY recognition_booking_month)                                       "Previous Redeemed To Date",
    LAG(cancelled_to_date)
       OVER (PARTITION BY brand,region,country,currency, original_credit_tender,original_credit_type,deferred_recognition_label,
           issued_month
           ORDER BY recognition_booking_month)                                       "Previous Cancelled To Date",
    LAG(expired_to_date)
       OVER (PARTITION BY brand,region,country,currency, original_credit_tender,original_credit_type,deferred_recognition_label,
           issued_month
           ORDER BY recognition_booking_month)                                       "Previous Expired To Date",
    IFF(RANK() OVER (PARTITION BY brand,region,country,currency, original_credit_tender,deferred_recognition_label,
       original_credit_type,issued_month
       ORDER BY recognition_booking_month) = 1, breakage_to_record,
       (breakage_to_record - "Previous Breakage Recorded"))                "Change In Breakage To Record",
    IFF(RANK() OVER (PARTITION BY brand,region,country,currency, original_credit_tender,deferred_recognition_label,
       original_credit_type,issued_month
       ORDER BY recognition_booking_month) = 1, redeemed_to_date,
       (redeemed_to_date - "Previous Redeemed To Date"))                  "Monthly Redemption",
    IFF(RANK() OVER (PARTITION BY brand,region,country,currency, original_credit_tender,deferred_recognition_label,
       original_credit_type,issued_month
       ORDER BY recognition_booking_month) = 1, cancelled_to_date,
       (cancelled_to_date - "Previous Cancelled To Date"))                "Monthly Cancellation",
    IFF(RANK() OVER (PARTITION BY brand,region,country,currency, original_credit_tender,deferred_recognition_label,
       original_credit_type,issued_month
       ORDER BY recognition_booking_month) = 1, expired_to_date,
       (expired_to_date - "Previous Cancelled To Date"))                  "Monthly Expiration"
from breakage.jfb_sxf_membership_credit_breakage_report
where 1=1
and recognition_booking_month >= '2024-12-01'
and brand in ('Fabletics Mens','Fabletics Womens','Yitty')
and ((original_credit_type = 'Variable Credit' and store_credit_reason = 'Refund') OR
     (original_credit_type = 'Giftcard' AND store_credit_reason = 'Gift Card - Paid'))
;
