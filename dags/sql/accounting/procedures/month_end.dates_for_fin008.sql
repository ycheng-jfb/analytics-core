CREATE OR REPLACE TRANSIENT TABLE month_end.dates_for_fin008 AS
WITH _all_dates AS (SELECT DISTINCT month_date
                    FROM edw_prod.data_model.dim_date
                    WHERE month_date >= LAST_DAY(DATEADD(MONTH, - 2, CURRENT_TIMESTAMP()))
                      AND month_date < DATE_TRUNC(MONTH, CURRENT_TIMESTAMP())),
     _date_combinations AS (SELECT LAST_DAY(month_date)::DATE                          AS credit_active_as_of_date,
                                   credit_active_as_of_date                            AS credit_issued_max_date,
                                   DATE_TRUNC(MONTH, credit_issued_max_date::DATE) - 1 AS vip_cohort_max_date,
                                   credit_active_as_of_date::DATE                      AS member_status_date
                            FROM _all_dates
                            UNION ALL
                            SELECT LAST_DAY(month_date)::DATE                          AS credit_active_as_of_date,
                                   DATEADD(MONTH, -12, credit_active_as_of_date)       AS credit_issued_max_date,
                                   DATE_TRUNC(MONTH, credit_issued_max_date::DATE) - 1 AS vip_cohort_max_date,
                                   credit_active_as_of_date::DATE                      AS member_status_date
                            FROM _all_dates)
SELECT credit_active_as_of_date,
       credit_issued_max_date,
       vip_cohort_max_date,
       member_status_date,
       row_number() over (ORDER BY credit_active_as_of_date ASC) AS num_for_recursion
FROM _date_combinations
ORDER BY credit_active_as_of_date DESC;
