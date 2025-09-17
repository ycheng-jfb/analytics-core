CREATE OR REPLACE VIEW reporting_prod.shared.breakage_by_state AS
WITH _fl_converted AS (WITH _state AS
                           (SELECT brand,
                                   country,
                                   original_credit_type,
                                   state,
                                   activity_month,
                                   original_issued_month,
                                   SUM(issued_to_date)     AS state_issued,
                                   SUM(redeemed_to_date)   AS state_redeemed,
                                   SUM(cancelled_to_date)  AS state_cancelled,
                                   SUM(unredeemed_to_date) AS state_unredeemed
                            FROM reporting_prod.shared.breakage_by_state_actuals
                            WHERE brand IN ('Fabletics Mens','Fabletics Womens','Yitty')
                              AND country IN ('US','CA')
                            GROUP BY brand,
                                     country,
                                     original_credit_type,
                                     state,
                                     activity_month,
                                     original_issued_month),
                       _country AS (SELECT brand,
                                           country,
                                           activity_month,
                                           original_issued_month,
                                           original_credit_type,
                                           SUM(unredeemed_to_date) AS       total_unredeemed,
                                           SUM(expected_breakage_to_record) total_expected_breakage,
                                           SUM(breakage_recorded)  AS       total_breakage_recorded
                                    FROM reporting_base_prod.shared.fabletics_converted_credits_breakage_report
                                    WHERE country  IN ('US','CA')
                                    GROUP BY brand,
                                             country,
                                             activity_month,
                                             original_issued_month,
                                             original_credit_type)
                  SELECT s.brand || ' ' || s.country                                                    AS business_unit,
                         s.brand,
                         s.country,
                         s.state,
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
                           JOIN _country C ON s.brand = C.brand AND s.country = C.country AND
                                              s.activity_month = C.activity_month AND
                                              s.original_issued_month = C.original_issued_month
                                          AND s.original_credit_type=C.original_credit_type),

     _fl_token AS
         (WITH _state AS
                   (SELECT brand,
                           country,
                           original_credit_type,
                           state,
                           activity_month,
                           original_issued_month,
                           SUM(issued_to_date)     AS state_issued,
                           SUM(redeemed_to_date)   AS state_redeemed,
                           SUM(cancelled_to_date)  AS state_cancelled,
                           SUM(unredeemed_to_date) AS state_unredeemed
                    FROM reporting_prod.shared.breakage_by_state_actuals
                    WHERE brand IN ('Fabletics Mens','Fabletics Womens','Yitty')
                      AND country IN ('US','CA')
                    GROUP BY brand,
                             country,
                             original_credit_type,
                             state,
                             activity_month,
                             original_issued_month),
               _country AS (SELECT brand,
                                   country,
                                   activity_month,
                                   original_issued_month,
                                   original_credit_type,
                                   SUM(unredeemed_to_date) AS       total_unredeemed,
                                   SUM(expected_breakage_to_record) total_expected_breakage,
                                   SUM(breakage_recorded)  AS       total_breakage_recorded
                            FROM reporting_base_prod.shared.fabletics_token_breakage_report
                            WHERE country  IN ('US','CA')
                            GROUP BY brand,
                                     country,
                                     activity_month,
                                     original_issued_month,
                                     original_credit_type)
          SELECT s.brand || ' ' || s.country                                                    AS business_unit,
                 s.brand,
                 s.country,
                 s.state,
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
                        ON s.brand = C.brand AND s.country = C.country AND s.activity_month = C.activity_month AND
                           s.original_issued_month = C.original_issued_month
                        AND s.original_credit_type=C.original_credit_type),
    _sx_token AS (
        WITH _state AS
                   (SELECT brand,
                           country,
                           original_credit_type,
                           state,
                           activity_month,
                           original_issued_month,
                           SUM(issued_to_date)     AS state_issued,
                           SUM(redeemed_to_date)   AS state_redeemed,
                           SUM(cancelled_to_date)  AS state_cancelled,
                           SUM(unredeemed_to_date) AS state_unredeemed
                    FROM reporting_prod.shared.breakage_by_state_actuals
                    WHERE brand IN ('Savage X')
                      AND country IN ('US')
                    GROUP BY brand,
                             country,
                             original_credit_type,
                             state,
                             activity_month,
                             original_issued_month),
               _country AS (SELECT brand,
                                   country,
                                   activity_month,
                                   original_issued_month,
                                   original_credit_type,
                                   SUM(unredeemed_to_date) AS       total_unredeemed,
                                   SUM(expected_breakage_to_record) total_expected_breakage,
                                   SUM(breakage_recorded)  AS       total_breakage_recorded
                            FROM reporting_base_prod.shared.savagex_token_breakage_report
                            WHERE country = 'US'
                            GROUP BY brand,
                                     country,
                                     activity_month,
                                     original_issued_month,
                                     original_credit_type)
          SELECT s.brand || ' ' || s.country                                                    AS business_unit,
                 s.brand,
                 s.country,
                 s.state,
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
                        ON s.brand = C.brand AND s.country = C.country AND s.activity_month = C.activity_month AND
                           s.original_issued_month = C.original_issued_month AND s.original_credit_type=C.original_credit_type
    ),
    _sx_converted AS (
        WITH _state AS
                   (SELECT brand,
                           country,
                           original_credit_type,
                           state,
                           activity_month,
                           original_issued_month,
                           SUM(issued_to_date)     AS state_issued,
                           SUM(redeemed_to_date)   AS state_redeemed,
                           SUM(cancelled_to_date)  AS state_cancelled,
                           SUM(unredeemed_to_date) AS state_unredeemed
                    FROM reporting_prod.shared.breakage_by_state_actuals
                    WHERE brand IN ('Savage X')
                      AND country IN ('US')
                    GROUP BY brand,
                             country,
                             original_credit_type,
                             state,
                             activity_month,
                             original_issued_month),
               _country AS (SELECT brand,
                                   country,
                                   activity_month,
                                   original_issued_month,
                                   original_credit_type,
                                   SUM(unredeemed_to_date) AS       total_unredeemed,
                                   SUM(expected_breakage_to_record) total_expected_breakage,
                                   SUM(breakage_recorded)  AS       total_breakage_recorded
                            FROM reporting_base_prod.shared.savagex_converted_credits_breakage_report
                            WHERE country = 'US'
                            GROUP BY brand,
                                     country,
                                     activity_month,
                                     original_issued_month,
                                     original_credit_type)
          SELECT s.brand || ' ' || s.country                                                    AS business_unit,
                 s.brand,
                 s.country,
                 s.state,
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
                        ON s.brand = C.brand AND s.country = C.country AND s.activity_month = C.activity_month AND
                           s.original_issued_month = C.original_issued_month AND s.original_credit_type=C.original_credit_type
    ),
    _jfb_token AS (
                WITH _state AS
                   (SELECT brand,
                           country,
                           original_credit_type,
                           state,
                           activity_month,
                           original_issued_month,
                           SUM(issued_to_date)     AS state_issued,
                           SUM(redeemed_to_date)   AS state_redeemed,
                           SUM(cancelled_to_date)  AS state_cancelled,
                           SUM(unredeemed_to_date) AS state_unredeemed
                    FROM reporting_prod.shared.breakage_by_state_actuals
                    WHERE brand IN ('JustFab','FabKids','ShoeDazzle')
                      AND country IN ('US')
                    GROUP BY brand,
                             country,
                             original_credit_type,
                             state,
                             activity_month,
                             original_issued_month),
               _country AS (SELECT brand,
                                   country,
                                   recognition_booking_month  AS activity_month,
                                   issued_month               AS original_issued_month,
                                   original_credit_type,
                                   SUM(unredeemed_to_date)    AS total_unredeemed,
                                   SUM(expected_breakage)     AS total_expected_breakage,
                                   SUM(breakage_to_record)    AS total_breakage_recorded
                            FROM reporting_base_prod.shared.jfb_token_breakage_report
                            WHERE country = 'US'
                            GROUP BY brand,
                                     country,
                                     recognition_booking_month,
                                     issued_month,
                                     original_credit_type)
          SELECT s.brand || ' ' || s.country                                                    AS business_unit,
                 s.brand,
                 s.country,
                 s.state,
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
                        ON s.brand = C.brand AND s.country = C.country AND s.activity_month = C.activity_month AND
                           s.original_issued_month = C.original_issued_month AND s.original_credit_type=C.original_credit_type
    ),
    _jfb_converted AS (
                WITH _state AS
                   (SELECT brand,
                           country,
                           original_credit_type,
                           state,
                           activity_month,
                           original_issued_month,
                           SUM(issued_to_date)     AS state_issued,
                           SUM(redeemed_to_date)   AS state_redeemed,
                           SUM(cancelled_to_date)  AS state_cancelled,
                           SUM(unredeemed_to_date) AS state_unredeemed
                    FROM reporting_prod.shared.breakage_by_state_actuals
                    WHERE brand IN ('JustFab','FabKids','ShoeDazzle')
                      AND country IN ('US')
                    GROUP BY brand,
                             country,
                             original_credit_type,
                             state,
                             activity_month,
                             original_issued_month),
               _country AS (SELECT brand,
                                   country,
                                   recognition_booking_month  AS activity_month,
                                   issued_month               AS original_issued_month,
                                   original_credit_type,
                                   SUM(unredeemed_to_date)    AS total_unredeemed,
                                   SUM(expected_breakage)     AS total_expected_breakage,
                                   SUM(breakage_to_record)    AS total_breakage_recorded
                            FROM reporting_base_prod.shared.jfb_converted_breakage_report
                            WHERE country = 'US'
                            GROUP BY brand,
                                     country,
                                     recognition_booking_month,
                                     issued_month,
                                     original_credit_type)
          SELECT s.brand || ' ' || s.country                                                    AS business_unit,
                 s.brand,
                 s.country,
                 s.state,
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
                        ON s.brand = C.brand AND s.country = C.country AND s.activity_month = C.activity_month AND
                           s.original_issued_month = C.original_issued_month AND s.original_credit_type=C.original_credit_type
    ),
    _jfb_membership AS (
                WITH _state AS
                   (SELECT brand,
                           country,
                           original_credit_type,
                           state,
                           activity_month,
                           original_issued_month,
                           SUM(issued_to_date)     AS state_issued,
                           SUM(redeemed_to_date)   AS state_redeemed,
                           SUM(cancelled_to_date)  AS state_cancelled,
                           SUM(unredeemed_to_date) AS state_unredeemed
                    FROM reporting_prod.shared.breakage_by_state_actuals
                    WHERE brand IN ('JustFab','FabKids','ShoeDazzle')
                      AND country IN ('US','CA')
                    GROUP BY brand,
                             country,
                             original_credit_type,
                             state,
                             activity_month,
                             original_issued_month),
               _country AS (SELECT brand,
                                   country,
                                   recognition_booking_month  AS activity_month,
                                   issued_month               AS original_issued_month,
                                   original_credit_type,
                                   SUM(unredeemed_to_date)    AS total_unredeemed,
                                   SUM(expected_breakage)     AS total_expected_breakage,
                                   SUM(breakage_to_record)    AS total_breakage_recorded
                            FROM reporting_base_prod.shared.jfb_sxf_membership_credit_breakage_report
                            WHERE country IN ('US','CA')
                            AND recognition_booking_month IN ('2021-12-01','2022-12-01')
                            AND iff(country='US',original_issued_month >='2018-12-01', original_issued_month>='2011-02-01')
                            GROUP BY brand,
                                     country,
                                     recognition_booking_month,
                                     issued_month,
                                     original_credit_type)
          SELECT s.brand || ' ' || s.country                                                    AS business_unit,
                 s.brand,
                 s.country,
                 s.state,
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
                        ON s.brand = C.brand AND s.country = C.country AND s.activity_month = C.activity_month AND
                           s.original_issued_month = C.original_issued_month AND s.original_credit_type=C.original_credit_type
    )

SELECT *
FROM _fl_converted
UNION ALL
SELECT *
FROM _fl_token
UNION ALL
SELECT *
FROM _sx_token
UNION ALL
SELECT *
FROM _sx_converted
UNION ALL
SELECT *
FROM _jfb_token
UNION ALL
SELECT *
FROM _jfb_converted
UNION ALL
SELECT *
FROM _jfb_membership;
