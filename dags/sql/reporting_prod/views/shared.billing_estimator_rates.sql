CREATE OR REPLACE VIEW shared.billing_estimator_rates
            (
             order_date_added,
             store_id,
             label,
             credited_all,
             credited_first_time,
             total_attempts,
             total_attempts_less_credited_first_time,
             retry_success,
             credited_all_5day_sum,
             credited_first_time_5day_sum,
             total_attempts_5day_sum,
             retry_attempts_5day_sum,
             retry_success_5day_sum,
             credited_all_prior_month,
             credited_first_time_prior_month,
             total_attempts_prior_month,
             total_attempts_less_credited_first_time_prior_month,
             retry_success_prior_month,
             credited_all_6months,
             credited_first_time_6months,
             total_attempts_6months,
             retry_attempts_6months,
             retry_success_6months,
             label_rank,
             credited_first_time_prior_day,
             total_attempts_prior_day,
             credited_first_time_mtd,
             total_attempts_mtd,
             credited_first_time_pmsd,
             total_attempts_pmsd
                )
AS
WITH moving_avg AS (SELECT DATE(p.date_period_start)                                          AS billing_period,
                           DATE(o.datetime_added)                                             AS order_date_added,
                           DATE_TRUNC('MONTH', o.datetime_added)                              AS order_date_added_month,
                           ADD_MONTHS(DATE_TRUNC('MONTH', o.datetime_added), 1)               AS next_month,
                           s.store_id,
                           s.label,
                           COUNT(DISTINCT CASE WHEN mp.statuscode = 3957 THEN o.order_id END) AS credited_all,
                           COUNT(DISTINCT CASE
                                              WHEN mp.statuscode = 3957 AND
                                                   (q.billing_retry_schedule_id IS NULL OR q.statuscode = 4209)
                                                  THEN o.order_id END)                        AS credited_first_time,
                           COUNT(DISTINCT o.order_id)                                         AS total_attempts,
                           COUNT(DISTINCT o.order_id)
                               - COUNT(DISTINCT CASE
                                                    WHEN mp.statuscode = 3957 AND q.billing_retry_schedule_id IS NULL
                                                        THEN o.order_id END)                  AS total_attempts_less_credited_first_time,
                           COUNT(DISTINCT CASE
                                              WHEN q.datetime_last_retry IS NOT NULL AND
                                                   (q.statuscode = 4205 OR mp.statuscode = 3957)
                                                  THEN q.order_id END)                        AS retry_success
                    FROM lake_consolidated_view.ultra_merchant.membership_period mp
                             LEFT OUTER JOIN lake_consolidated_view.ultra_merchant."ORDER" o
                                             ON o.order_id = mp.credit_order_id
                             LEFT OUTER JOIN lake_consolidated_view.ultra_merchant.billing_retry_schedule_queue q
                                             ON q.order_id = o.order_id
                             JOIN lake_consolidated_view.ultra_merchant.membership m
                                  ON mp.membership_id = m.membership_id
                             JOIN lake_consolidated_view.ultra_merchant.store s ON s.store_id = m.store_id
                             JOIN lake_consolidated_view.ultra_merchant.period p ON p.period_id = mp.period_id
                    WHERE DATE(p.date_period_start) >= '2018-01-01'
                      AND m.store_id IN
                          (46, 52, 79, 26, 41, 121, 55, 36, 38, 48, 50, 57, 59, 61, 63, 65, 67, 69, 71, 73, 75, 77,
                           125, 127, 129, 131, 133, 135, 137, 139)
                    GROUP BY DATE(p.date_period_start),
                             DATE(o.datetime_added),
                             DATE_TRUNC('MONTH', o.datetime_added),
                             ADD_MONTHS(DATE_TRUNC('MONTH', o.datetime_added), 1),
                             s.store_id,
                             s.label
                    UNION
                    SELECT DATE(p.date_period_start)                                                   AS billing_period,
                           DATE(o.datetime_added)                                                      AS order_date_added,
                           DATE_TRUNC('MONTH', o.datetime_added)                                       AS order_date_added_month,
                           ADD_MONTHS(DATE_TRUNC('MONTH', o.datetime_added), 1)                        AS next_month,
                           s.store_id,
                           s.label,
                           COUNT(DISTINCT CASE WHEN mp.statuscode IN (5170, 5171) THEN o.order_id END) AS credited_all,
                           COUNT(DISTINCT CASE
                                              WHEN mp.statuscode IN (5170, 5171) AND
                                                   (q.billing_retry_schedule_id IS NULL OR q.statuscode = 4209)
                                                  THEN o.order_id END)                                 AS credited_first_time,
                           COUNT(DISTINCT o.order_id)                                                  AS total_attempts,
                           COUNT(DISTINCT o.order_id)
                               - COUNT(DISTINCT CASE
                                                    WHEN mp.statuscode IN (5170, 5171) AND q.billing_retry_schedule_id IS NULL
                                                        THEN o.order_id END)                           AS total_attempts_less_credited_first_time,
                           COUNT(DISTINCT CASE
                                              WHEN q.datetime_last_retry IS NOT NULL AND
                                                   (q.statuscode = 4205 OR mp.statuscode IN (5170, 5171))
                                                  THEN q.order_id END)                                 AS retry_success
                    FROM lake_consolidated_view.ultra_merchant.membership_billing mp
                             LEFT OUTER JOIN lake_consolidated_view.ultra_merchant."ORDER" o ON o.order_id = mp.order_id
                             LEFT OUTER JOIN lake_consolidated_view.ultra_merchant.billing_retry_schedule_queue q
                                             ON q.order_id = o.order_id
                             JOIN lake_consolidated_view.ultra_merchant.membership m
                                  ON mp.membership_id = m.membership_id
                             JOIN lake_consolidated_view.ultra_merchant.store s ON s.store_id = m.store_id
                             JOIN lake_consolidated_view.ultra_merchant.period p ON p.period_id = mp.period_id
                    WHERE DATE(p.date_period_start) >= '2021-01-01'
                      AND mp.membership_type_id = 3
                    GROUP BY DATE(p.date_period_start),
                             DATE(o.datetime_added),
                             DATE_TRUNC('MONTH', o.datetime_added),
                             ADD_MONTHS(DATE_TRUNC('MONTH', o.datetime_added), 1),
                             s.store_id,
                             s.label)
SELECT ttl.order_date_added,
       ttl.store_id,
       ttl.label,
       ttl.credited_all,
       ttl.credited_first_time,
       ttl.total_attempts,
       ttl.total_attempts_less_credited_first_time,
       ttl.retry_success,
       SUM(ttl.credited_all)
           OVER (PARTITION BY ttl.label ORDER BY ttl.order_date_added ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING )                       AS credited_all_5day_sum,
       SUM(ttl.credited_first_time)
           OVER (PARTITION BY ttl.label ORDER BY ttl.order_date_added ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING )                       AS credited_first_time_5day_sum,
       SUM(ttl.total_attempts)
           OVER (PARTITION BY ttl.label ORDER BY ttl.order_date_added ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING )                       AS total_attempts_5day_sum,
       SUM(ttl.total_attempts_less_credited_first_time)
           OVER (PARTITION BY ttl.label ORDER BY ttl.order_date_added ROWS BETWEEN 7 PRECEDING AND 2 PRECEDING )                       AS retry_attempts_5day_sum,
       SUM(ttl.retry_success)
           OVER (PARTITION BY ttl.label ORDER BY ttl.order_date_added ROWS BETWEEN 7 PRECEDING AND 2 PRECEDING )                       AS retry_success_5day_sum,
       prior_month.credited_all                                                                                                        AS credited_all_prior_month,
       prior_month.credited_first_time                                                                                                 AS credited_first_time_prior_month,
       prior_month.total_attempts                                                                                                      AS total_attempts_prior_month,
       prior_month.total_attempts_less_credited_first_time                                                                             AS total_attempts_less_credited_first_time_prior_month,
       prior_month.retry_success                                                                                                       AS retry_success_prior_month,
       six_months.credited_all_6months                                                                                                 AS credited_all_6months,
       six_months.credited_first_time_6months                                                                                          AS credited_first_time_6months,
       six_months.total_attempts_6months                                                                                               AS total_attempts_6months,
       six_months.retry_attempts_6months                                                                                               AS retry_attempts_6months,
       six_months.retry_success_6months                                                                                                AS retry_success_6months,
       RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', ttl.order_date_added), ttl.store_id, ttl.label ORDER BY ttl.order_date_added ASC) AS label_rank,
       LAG(ttl.credited_first_time)
           OVER (PARTITION BY DATE_TRUNC('MONTH', ttl.order_date_added), ttl.store_id, ttl.label ORDER BY ttl.order_date_added ASC)    AS credited_first_time_prior_day,
       LAG(ttl.total_attempts)
           OVER (PARTITION BY DATE_TRUNC('MONTH', ttl.order_date_added), ttl.store_id, ttl.label ORDER BY ttl.order_date_added ASC)    AS total_attempts_prior_day,
       mtd.credited_first_time_mtd,
       mtd.total_attempts_mtd,
       pmsd.credited_first_time_pmsd,
       pmsd.total_attempts_pmsd
FROM (SELECT mv.order_date_added,
             mv.store_id,
             mv.label,
             SUM(credited_all)                            AS credited_all,
             SUM(credited_first_time)                     AS credited_first_time,
             SUM(total_attempts)                          AS total_attempts,
             SUM(total_attempts_less_credited_first_time) AS total_attempts_less_credited_first_time,
             SUM(retry_success)                           AS retry_success
      FROM (SELECT order_date_added,
                   store_id,
                   IFF(label = 'Yitty', 'Yitty NA', label) AS label,
                   credited_all,
                   credited_first_time,
                   total_attempts,
                   total_attempts_less_credited_first_time,
                   retry_success
            FROM moving_avg
            UNION
            SELECT DISTINCT full_date,
                            store_id,
                            IFF(label = 'Yitty', 'Yitty NA', label) AS label,
                            0,
                            0,
                            0,
                            0,
                            0
            FROM reporting_prod.shared.billing_estimator_estimates,
                 edw_prod.data_model.dim_date
            WHERE edw_prod.data_model.dim_date.calendar_year >= 2018
              AND edw_prod.data_model.dim_date.full_date <= CURRENT_DATE) mv
      WHERE DATE_PART('DAY', mv.order_date_added) BETWEEN 6 AND 14
      GROUP BY mv.order_date_added,
               mv.store_id,
               mv.label) ttl
         LEFT OUTER JOIN (SELECT moving_avg.next_month,
                                 moving_avg.store_id,
                                 IFF(moving_avg.label = 'Yitty', 'Yitty NA', moving_avg.label) AS label,
                                 SUM(moving_avg.credited_all)                                  AS credited_all,
                                 SUM(moving_avg.credited_first_time)                           AS credited_first_time,
                                 SUM(moving_avg.total_attempts)                                AS total_attempts,
                                 SUM(moving_avg.total_attempts_less_credited_first_time)       AS total_attempts_less_credited_first_time,
                                 SUM(moving_avg.retry_success)                                 AS retry_success
                          FROM moving_avg
                          GROUP BY moving_avg.next_month,
                                   moving_avg.store_id,
                                   IFF(moving_avg.label = 'Yitty', 'Yitty NA', moving_avg.label)) prior_month
                         ON DATE(prior_month.next_month) = DATE_TRUNC('month', ttl.order_date_added)
                             AND prior_month.store_id = ttl.store_id
                             AND prior_month.label = ttl.label
         LEFT OUTER JOIN (SELECT mv.next_month,
                                 mv.store_id,
                                 mv.label,
                                 SUM(mv.credited_all)
                                     OVER (PARTITION BY mv.label ORDER BY mv.next_month ROWS BETWEEN 5 PRECEDING AND CURRENT ROW ) credited_all_6months,
                                 SUM(mv.credited_first_time)
                                     OVER (PARTITION BY mv.label ORDER BY mv.next_month ROWS BETWEEN 5 PRECEDING AND CURRENT ROW ) credited_first_time_6months,
                                 SUM(mv.total_attempts)
                                     OVER (PARTITION BY mv.label ORDER BY mv.next_month ROWS BETWEEN 5 PRECEDING AND CURRENT ROW ) total_attempts_6months,
                                 SUM(mv.total_attempts_less_credited_first_time)
                                     OVER (PARTITION BY mv.label ORDER BY mv.next_month ROWS BETWEEN 5 PRECEDING AND CURRENT ROW ) retry_attempts_6months,
                                 SUM(mv.retry_success)
                                     OVER (PARTITION BY mv.label ORDER BY mv.next_month ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)  retry_success_6months
                          FROM (SELECT next_month,
                                       store_id,
                                       IFF(label = 'Yitty', 'Yitty NA', label)      AS label,
                                       SUM(credited_all)                            AS credited_all,
                                       SUM(credited_first_time)                     AS credited_first_time,
                                       SUM(total_attempts)                          AS total_attempts,
                                       SUM(total_attempts_less_credited_first_time) AS total_attempts_less_credited_first_time,
                                       SUM(retry_success)                           AS retry_success
                                FROM moving_avg
                                WHERE next_month >= '2018-01-01'
                                GROUP BY next_month, store_id, IFF(label = 'Yitty', 'Yitty NA', label)) mv) six_months
                         ON DATE(six_months.next_month) = DATE_TRUNC('month', ttl.order_date_added)
                             AND six_months.store_id = ttl.store_id
                             AND six_months.label = ttl.label
         LEFT JOIN (SELECT billing_period,
                           order_date_added,
                           store_id,
                           label,
                           SUM(credited_first_time)
                               OVER (PARTITION BY billing_period, store_id, label ORDER BY order_date_added ASC) AS credited_first_time_mtd,
                           SUM(total_attempts)
                               OVER (PARTITION BY billing_period, store_id, label ORDER BY order_date_added ASC) AS total_attempts_mtd
                    FROM (SELECT billing_period,
                                 order_date_added,
                                 store_id,
                                 label,
                                 SUM(credited_first_time) AS credited_first_time,
                                 SUM(total_attempts)      AS total_attempts
                          FROM moving_avg
                          WHERE billing_period = DATE_TRUNC('MONTH', order_date_added)
                          GROUP BY billing_period,
                                   order_date_added,
                                   store_id,
                                   label) ma) mtd ON mtd.order_date_added = ttl.order_date_added
    AND mtd.label = ttl.label
         LEFT JOIN (SELECT DATEADD('MONTH', 1, order_date_added) AS order_date_added,
                           store_id,
                           label,
                           credited_first_time                   AS credited_first_time_pmsd,
                           total_attempts                        AS total_attempts_pmsd
                    FROM (SELECT order_date_added,
                                 store_id,
                                 label,
                                 SUM(credited_first_time) AS credited_first_time,
                                 SUM(total_attempts)      AS total_attempts
                          FROM moving_avg
                          WHERE billing_period = DATE_TRUNC('MONTH', order_date_added)
                            AND DATE_PART('DAY', order_date_added) BETWEEN 6 AND 14
                          GROUP BY billing_period,
                                   order_date_added,
                                   store_id,
                                   label) ma) pmsd ON pmsd.order_date_added = ttl.order_date_added
    AND pmsd.store_id = ttl.store_id;
