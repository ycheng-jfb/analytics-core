CREATE OR REPLACE VIEW breakage.breakage_by_state AS
WITH _fl_converted AS (WITH _state AS
                           (SELECT brand,
                                   country,
                                   original_credit_type,
                                   state,
                                   deferred_recognition_label,
                                   activity_month,
                                   original_issued_month,
                                   SUM(issued_to_date)     AS state_issued,
                                   SUM(redeemed_to_date)   AS state_redeemed,
                                   SUM(cancelled_to_date)  AS state_cancelled,
                                   SUM(unredeemed_to_date) AS state_unredeemed
                            FROM breakage.breakage_by_state_actuals
                            WHERE brand IN ('Fabletics Womens')
                              AND original_credit_type = 'Fixed Credit'
                              AND country IN ('US','CA')
                            GROUP BY brand,
                                     country,
                                     original_credit_type,
                                     state,
                                     deferred_recognition_label,
                                     activity_month,
                                     original_issued_month),
                       _country AS (SELECT brand,
                                           country,
                                           activity_month,
                                           'Recognize' AS deferred_recognition_label,
                                           original_issued_month,
                                           original_credit_type,
                                           SUM(unredeemed_to_date) AS       total_unredeemed,
                                           SUM(expected_breakage_to_record) total_expected_breakage,
                                           SUM(breakage_recorded)  AS       total_breakage_recorded
                                    FROM breakage.fabletics_converted_credits_breakage_report
                                    WHERE country IN ('US','CA')
                                    GROUP BY brand,
                                             country,
                                             activity_month,
                                             deferred_recognition_label,
                                             original_issued_month,
                                             original_credit_type)
                  SELECT s.brand || ' ' || s.country                                                    AS business_unit,
                         s.brand,
                         s.country,
                         s.state,
                         s.deferred_recognition_label,
                         s.original_issued_month,
                         s.original_credit_type,
                         s.activity_month,
                         s.state_issued,
                         s.state_redeemed,
                         s.state_cancelled,
                         s.state_unredeemed,
                         C.total_unredeemed,
                         C.total_breakage_recorded,
                         NULLIFZERO(s.state_unredeemed / C.total_unredeemed) * C.total_breakage_recorded AS breakage_recorded
                  FROM _state s
                           JOIN _country C ON s.brand = C.brand
                                                  AND s.country = C.country
                                                  AND s.activity_month = C.activity_month
                                                  AND s.original_issued_month = C.original_issued_month
                                                  AND s.original_credit_type=C.original_credit_type
                                                  AND s.deferred_recognition_label = C.deferred_recognition_label),
     _fl_token AS
         (WITH _state AS
                   (SELECT brand,
                           country,
                           original_credit_type,
                           state,
                           deferred_recognition_label,
                           activity_month,
                           original_issued_month,
                           SUM(issued_to_date)     AS state_issued,
                           SUM(redeemed_to_date)   AS state_redeemed,
                           SUM(cancelled_to_date)  AS state_cancelled,
                           SUM(unredeemed_to_date) AS state_unredeemed
                    FROM breakage.breakage_by_state_actuals
                    WHERE brand IN ('Fabletics Mens','Fabletics Womens','Yitty')
                      AND country IN ('US','CA')
                    GROUP BY brand,
                             country,
                             original_credit_type,
                             state,
                             deferred_recognition_label,
                             activity_month,
                             original_issued_month),
               _country AS (SELECT brand,
                                   country,
                                   activity_month,
                                   'Recognize' AS deferred_recognition_label,
                                   original_issued_month,
                                   original_credit_type,
                                   SUM(unredeemed_to_date) AS       total_unredeemed,
                                   SUM(expected_breakage_to_record) total_expected_breakage,
                                   SUM(breakage_recorded)  AS       total_breakage_recorded
                            FROM breakage.fabletics_token_breakage_report
                            WHERE country  IN ('US','CA')
                            GROUP BY brand,
                                     country,
                                     activity_month,
                                     'Recognize',
                                     original_issued_month,
                                     original_credit_type)
          SELECT s.brand || ' ' || s.country                                                    AS business_unit,
                 s.brand,
                 s.country,
                 s.state,
                 s.deferred_recognition_label,
                 s.original_issued_month,
                 s.original_credit_type,
                 s.activity_month,
                 s.state_issued,
                 s.state_redeemed,
                 s.state_cancelled,
                 s.state_unredeemed,
                 C.total_unredeemed,
                 C.total_breakage_recorded,
                 s.state_unredeemed / NULLIFZERO(C.total_unredeemed) * C.total_breakage_recorded  AS breakage_recorded
          FROM _state s
                   JOIN _country C
                        ON s.brand = C.brand
                            AND s.country = C.country
                            AND s.activity_month = C.activity_month
                            AND s.original_issued_month = C.original_issued_month
                            AND s.original_credit_type=C.original_credit_type
                            AND s.deferred_recognition_label = C.deferred_recognition_label),
    _sx_total AS (
        WITH _state AS
                   (SELECT brand,
                           country,
                           'Fixed Credit or Token' AS original_credit_type,
                           state,
                           deferred_recognition_label,
                           activity_month,
                           original_issued_month,
                           SUM(issued_to_date)     AS state_issued,
                           SUM(redeemed_to_date)   AS state_redeemed,
                           SUM(cancelled_to_date)  AS state_cancelled,
                           SUM(unredeemed_to_date) AS state_unredeemed
                    FROM breakage.breakage_by_state_actuals
                    WHERE brand IN ('Savage X')
                      AND country IN ('US')
                    GROUP BY brand,
                             country,
                             'Fixed Credit or Token',
                             state,
                             deferred_recognition_label,
                             activity_month,
                             original_issued_month),
               _country AS (SELECT brand,
                                   country,
                                   activity_month,
                                   deferred_recognition_label_credit AS deferred_recognition_label,
                                   original_issued_month,
                                   'Fixed Credit or Token' AS original_credit_type,
                                   SUM(unredeemed_to_date) AS       total_unredeemed,
                                   SUM(expected_breakage_to_record) total_expected_breakage,
                                   SUM(breakage_recorded)  AS       total_breakage_recorded
                            FROM breakage.savagex_us_breakage_report
                            WHERE country = 'US'
                            GROUP BY brand,
                                     country,
                                     activity_month,
                                     deferred_recognition_label_credit,
                                     original_issued_month,
                                     'Fixed Credit or Token')
          SELECT s.brand || ' ' || s.country                                                    AS business_unit,
                 s.brand,
                 s.country,
                 s.state,
                 s.deferred_recognition_label,
                 s.original_issued_month,
                 s.original_credit_type,
                 s.activity_month,
                 s.state_issued,
                 s.state_redeemed,
                 s.state_cancelled,
                 s.state_unredeemed,
                 C.total_unredeemed,
                 C.total_breakage_recorded,
                 s.state_unredeemed / NULLIFZERO(C.total_unredeemed) * C.total_breakage_recorded  AS breakage_recorded
          FROM _state s
                   JOIN _country C
                        ON s.brand = C.brand
                               AND s.country = C.country
                               AND s.activity_month = C.activity_month
                               AND s.original_issued_month = C.original_issued_month
                               AND s.original_credit_type = C.original_credit_type
                               AND s.deferred_recognition_label = C.deferred_recognition_label
    ),
    _jfb_token AS (
                WITH _state AS
                   (SELECT brand,
                           country,
                           original_credit_type,
                           state,
                           deferred_recognition_label,
                           activity_month,
                           original_issued_month,
                           SUM(issued_to_date)     AS state_issued,
                           SUM(redeemed_to_date)   AS state_redeemed,
                           SUM(cancelled_to_date)  AS state_cancelled,
                           SUM(unredeemed_to_date) AS state_unredeemed
                    FROM breakage.breakage_by_state_actuals
                    WHERE brand IN ('JustFab','FabKids','ShoeDazzle')
                      AND country IN ('US')
                    GROUP BY brand,
                             country,
                             original_credit_type,
                             state,
                             deferred_recognition_label,
                             activity_month,
                             original_issued_month),
               _country AS (SELECT brand,
                                   country,
                                   recognition_booking_month  AS activity_month,
                                   deferred_recognition_label,
                                   issued_month               AS original_issued_month,
                                   original_credit_type,
                                   SUM(unredeemed_to_date)    AS total_unredeemed,
                                   SUM(expected_breakage)     AS total_expected_breakage,
                                   SUM(breakage_to_record)    AS total_breakage_recorded
                            FROM breakage.jfb_token_breakage_report
                            WHERE country = 'US'
                            GROUP BY brand,
                                     country,
                                     recognition_booking_month,
                                     deferred_recognition_label,
                                     issued_month,
                                     original_credit_type)
          SELECT s.brand || ' ' || s.country                                                    AS business_unit,
                 s.brand,
                 s.country,
                 s.state,
                 s.deferred_recognition_label,
                 s.original_issued_month,
                 s.original_credit_type,
                 s.activity_month,
                 s.state_issued,
                 s.state_redeemed,
                 s.state_cancelled,
                 s.state_unredeemed,
                 C.total_unredeemed,
                 C.total_breakage_recorded,
                 s.state_unredeemed / NULLIFZERO(C.total_unredeemed) * C.total_breakage_recorded  AS breakage_recorded
          FROM _state s
                   JOIN _country C
                        ON s.brand = C.brand
                            AND s.country = C.country
                            AND s.activity_month = C.activity_month
                            AND s.original_issued_month = C.original_issued_month
                            AND s.original_credit_type=C.original_credit_type
                            AND s.deferred_recognition_label = C.deferred_recognition_label
    ),
    _jfb_converted AS (
                WITH _state AS
                   (SELECT brand,
                           country,
                           original_credit_type,
                           state,
                           deferred_recognition_label,
                           activity_month,
                           original_issued_month,
                           SUM(issued_to_date)     AS state_issued,
                           SUM(redeemed_to_date)   AS state_redeemed,
                           SUM(cancelled_to_date)  AS state_cancelled,
                           SUM(unredeemed_to_date) AS state_unredeemed
                    FROM breakage.breakage_by_state_actuals
                    WHERE brand IN ('JustFab','FabKids','ShoeDazzle')
                      AND country IN ('US')
                    GROUP BY brand,
                             country,
                             original_credit_type,
                             state,
                             deferred_recognition_label,
                             activity_month,
                             original_issued_month),
               _country AS (SELECT brand,
                                   country,
                                   recognition_booking_month  AS activity_month,
                                   deferred_recognition_label,
                                   issued_month               AS original_issued_month,
                                   original_credit_type,
                                   SUM(unredeemed_to_date)    AS total_unredeemed,
                                   SUM(expected_breakage)     AS total_expected_breakage,
                                   SUM(breakage_to_record)    AS total_breakage_recorded
                            FROM breakage.jfb_converted_breakage_report
                            WHERE country = 'US'
                            GROUP BY brand,
                                     country,
                                     recognition_booking_month,
                                     deferred_recognition_label,
                                     issued_month,
                                     original_credit_type)
          SELECT s.brand || ' ' || s.country                                                    AS business_unit,
                 s.brand,
                 s.country,
                 s.state,
                 s.deferred_recognition_label,
                 s.original_issued_month,
                 s.original_credit_type,
                 s.activity_month,
                 s.state_issued,
                 s.state_redeemed,
                 s.state_cancelled,
                 s.state_unredeemed,
                 C.total_unredeemed,
                 C.total_breakage_recorded,
                 s.state_unredeemed / NULLIFZERO(C.total_unredeemed) * C.total_breakage_recorded  AS breakage_recorded
          FROM _state s
                   JOIN _country C
                        ON s.brand = C.brand
                            AND s.country = C.country
                            AND s.activity_month = C.activity_month
                            AND s.original_issued_month = C.original_issued_month
                            AND s.original_credit_type=C.original_credit_type
                            AND s.deferred_recognition_label = C.deferred_recognition_label
    ),
    _jfb_membership AS (
                WITH _state AS
                   (SELECT brand,
                           country,
                           original_credit_type,
                           state,
                           deferred_recognition_label,
                           activity_month,
                           original_issued_month,
                           SUM(issued_to_date)     AS state_issued,
                           SUM(redeemed_to_date)   AS state_redeemed,
                           SUM(cancelled_to_date)  AS state_cancelled,
                           SUM(unredeemed_to_date) AS state_unredeemed
                    FROM breakage.breakage_by_state_actuals
                    WHERE brand IN ('JustFab','FabKids','ShoeDazzle')
                      AND country IN ('US','CA')
                    GROUP BY brand,
                             country,
                             original_credit_type,
                             state,
                             deferred_recognition_label,
                             activity_month,
                             original_issued_month),
               _country AS (SELECT brand,
                                   country,
                                   recognition_booking_month  AS activity_month,
                                   'Recognize'                AS deferred_recognition_label,
                                   issued_month               AS original_issued_month,
                                   original_credit_type,
                                   SUM(unredeemed_to_date)    AS total_unredeemed,
                                   SUM(expected_breakage)     AS total_expected_breakage,
                                   SUM(breakage_to_record)    AS total_breakage_recorded
                            FROM breakage.jfb_sxf_membership_credit_breakage_report
                            WHERE country IN ('US','CA')
                            AND recognition_booking_month IN ('2021-12-01','2022-12-01')
                            AND iff(country='US',original_issued_month >='2018-12-01', original_issued_month>='2011-02-01')
                            GROUP BY brand,
                                     country,
                                     recognition_booking_month,
                                     'Recognize',
                                     issued_month,
                                     original_credit_type)
          SELECT s.brand || ' ' || s.country                                                    AS business_unit,
                 s.brand,
                 s.country,
                 s.state,
                 s.deferred_recognition_label,
                 s.original_issued_month,
                 s.original_credit_type,
                 s.activity_month,
                 s.state_issued,
                 s.state_redeemed,
                 s.state_cancelled,
                 s.state_unredeemed,
                 C.total_unredeemed,
                 C.total_breakage_recorded,
                 s.state_unredeemed / NULLIFZERO(C.total_unredeemed) * C.total_breakage_recorded  AS breakage_recorded

          FROM _state s
                   JOIN _country C
                        ON s.brand = C.brand
                               AND s.country = C.country
                               AND s.activity_month = C.activity_month
                               AND s.original_issued_month = C.original_issued_month
                               AND s.original_credit_type=C.original_credit_type
                               AND s.deferred_recognition_label = C.deferred_recognition_label
    ),
    _fl_variable AS
         (WITH _state AS
                   (SELECT brand,
                           country,
                           original_credit_type,
                           state,
                           deferred_recognition_label,
                           activity_month,
                           original_issued_month,
                           SUM(issued_to_date)     AS state_issued,
                           SUM(redeemed_to_date)   AS state_redeemed,
                           SUM(cancelled_to_date)  AS state_cancelled,
                           SUM(unredeemed_to_date) AS state_unredeemed
                    FROM breakage.breakage_by_state_actuals
                    WHERE brand IN ('Fabletics Mens','Fabletics Womens','Yitty')
                      AND country IN ('US','CA')
                    GROUP BY brand,
                             country,
                             original_credit_type,
                             state,
                             deferred_recognition_label,
                             activity_month,
                             original_issued_month),
               _country AS (SELECT brand,
                                   country,
                                   recognition_booking_month AS activity_month,
                                   deferred_recognition_label,
                                   issued_month AS original_issued_month,
                                   original_credit_type,
                                   SUM(unredeemed_to_date) AS       total_unredeemed,
                                   SUM(expected_breakage)  AS       total_expected_breakage,
                                   SUM(breakage_to_record)  AS       total_breakage_recorded
                            FROM breakage.jfb_sxf_membership_credit_breakage_report
                            WHERE brand IN ('Fabletics Mens','Fabletics Womens','Yitty')
                            AND country IN ('US','CA')
                            AND original_credit_type = 'Variable Credit'
                            AND store_credit_reason = 'Refund'
                            AND recognition_booking_month >= '2024-12-01'
                            GROUP BY brand,
                                     country,
                                     recognition_booking_month,
                                     deferred_recognition_label,
                                     issued_month,
                                     original_credit_type)
          SELECT s.brand || ' ' || s.country                                                    AS business_unit,
                 s.brand,
                 s.country,
                 s.state,
                 s.deferred_recognition_label,
                 s.original_issued_month,
                 s.original_credit_type,
                 s.activity_month,
                 s.state_issued,
                 s.state_redeemed,
                 s.state_cancelled,
                 s.state_unredeemed,
                 C.total_unredeemed,
                 C.total_breakage_recorded,
                 s.state_unredeemed / NULLIFZERO(C.total_unredeemed) * C.total_breakage_recorded  AS breakage_recorded
          FROM _state s
                   JOIN _country C
                        ON s.brand = C.brand
                            AND s.country = C.country
                            AND s.activity_month = C.activity_month
                            AND s.original_issued_month = C.original_issued_month
                            AND s.original_credit_type=C.original_credit_type
                            AND s.deferred_recognition_label = C.deferred_recognition_label),
    _fl_gift_card AS
         (WITH _state AS
                   (SELECT brand,
                           country,
                           original_credit_type,
                           state,
                           deferred_recognition_label,
                           activity_month,
                           original_issued_month,
                           SUM(issued_to_date)     AS state_issued,
                           SUM(redeemed_to_date)   AS state_redeemed,
                           SUM(cancelled_to_date)  AS state_cancelled,
                           SUM(unredeemed_to_date) AS state_unredeemed
                    FROM breakage.breakage_by_state_actuals
                    WHERE brand IN ('Fabletics Mens','Fabletics Womens')
                      AND country IN ('US')
                    GROUP BY brand,
                             country,
                             original_credit_type,
                             state,
                             deferred_recognition_label,
                             activity_month,
                             original_issued_month),
               _country AS (SELECT brand,
                                   country,
                                   recognition_booking_month AS activity_month,
                                   deferred_recognition_label,
                                   issued_month AS original_issued_month,
                                   original_credit_type,
                                   SUM(unredeemed_to_date) AS       total_unredeemed,
                                   SUM(expected_breakage)  AS       total_expected_breakage,
                                   SUM(breakage_to_record)  AS       total_breakage_recorded
                            FROM breakage.jfb_sxf_membership_credit_breakage_report
                            WHERE brand IN ('Fabletics Mens','Fabletics Womens')
                            AND country IN ('US')
                            AND original_credit_type = 'Giftcard'
                            AND store_credit_reason = 'Gift Card - Paid'
                            GROUP BY brand,
                                     country,
                                     recognition_booking_month,
                                     deferred_recognition_label,
                                     issued_month,
                                     original_credit_type)
          SELECT s.brand || ' ' || s.country                                                    AS business_unit,
                 s.brand,
                 s.country,
                 s.state,
                 s.deferred_recognition_label,
                 s.original_issued_month,
                 s.original_credit_type,
                 s.activity_month,
                 s.state_issued,
                 s.state_redeemed,
                 s.state_cancelled,
                 s.state_unredeemed,
                 C.total_unredeemed,
                 C.total_breakage_recorded,
                 s.state_unredeemed / NULLIFZERO(C.total_unredeemed) * C.total_breakage_recorded  AS breakage_recorded
          FROM _state s
                   JOIN _country C
                        ON s.brand = C.brand
                            AND s.country = C.country
                            AND s.activity_month = C.activity_month
                            AND s.original_issued_month = C.original_issued_month
                            AND s.original_credit_type=C.original_credit_type
                            AND s.deferred_recognition_label = C.deferred_recognition_label)

SELECT *
FROM _fl_converted
UNION ALL
SELECT *
FROM _fl_token
UNION ALL
SELECT *
FROM _sx_total
UNION ALL
SELECT *
FROM _jfb_token
UNION ALL
SELECT *
FROM _jfb_converted
UNION ALL
SELECT *
FROM _jfb_membership
UNION ALL
SELECT *
FROM _fl_variable
UNION ALL
SELECT *
FROM _fl_gift_card;
