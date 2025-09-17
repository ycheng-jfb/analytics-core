-- we are alerting for previous day
SET previous_day = DATEADD(DAY, -1, CURRENT_DATE);
SET last_month_same_day = DATEADD(MONTH, -1, $previous_day);

-- getting percent of all true segments by date
CREATE OR REPLACE TEMPORARY TABLE _last_month_percent AS
SELECT s.session_local_datetime::DATE date,
       ds.store_brand,
       (SUM(CASE WHEN is_in_segment = TRUE THEN 1 ELSE 0 END)*100)/COUNT(*) AS segement_sessions
FROM reporting_base_prod.shared.session s
         LEFT JOIN edw_prod.data_model.dim_store ds ON s.store_id = ds.store_id
WHERE ds.store_type <> 'Retail'
  AND s.session_local_datetime::DATE >= $last_month_same_day
  AND s.session_local_datetime::DATE < CURRENT_DATE
  AND NOT is_bot
GROUP BY s.session_local_datetime::DATE,
         ds.store_brand
ORDER BY date;

-- average percent of past month excluding previous day for which we are giving alert
-- and excluding same day of last month
CREATE OR REPLACE TEMPORARY TABLE _avg_percent AS
SELECT store_brand, AVG(segement_sessions) AS segement_sessions
FROM _last_month_percent
WHERE date < (SELECT MAX(date) FROM _last_month_percent)
  AND date > (SELECT MIN(date) FROM _last_month_percent)
GROUP BY store_brand;

-- percent of previous date segments and percent of last month same day
CREATE OR REPLACE TEMPORARY TABLE _date_percent AS
SELECT date, store_brand, segement_sessions, 'previous_date'AS day
FROM _last_month_percent
WHERE date = (SELECT MAX(date) FROM _last_month_percent)
UNION ALL
SELECT date, store_brand, segement_sessions, 'last_month_same_date' AS day
FROM _last_month_percent
WHERE date = (SELECT MIN(date) FROM _last_month_percent);

TRUNCATE TABLE reference.session_segment_percent;

-- comparing percentage of previous day with average of last 30 days and same day of previous month
INSERT INTO reference.session_segment_percent
SELECT a.store_brand,
       pd.date                                                             AS previous_date,
       ld.date                                                             AS last_month_same_date,
       a.segement_sessions                                                 AS average_sessions_percent,
       pd.segement_sessions                                                AS previous_date_sessions_percent,
       ld.segement_sessions                                                AS last_month_same_date_sessions_percent,
       a.segement_sessions - pd.segement_sessions                          AS average_percent_diff,
       ld.segement_sessions - pd.segement_sessions                         AS date_percent_diff,
       IFF(average_percent_diff > 5 OR date_percent_diff > 5, TRUE, FALSE) AS alert
FROM _avg_percent a
         JOIN _date_percent pd
              ON a.store_brand = pd.store_brand
                  AND pd.day = 'previous_date'
         JOIN _date_percent ld
              ON a.store_brand = ld.store_brand
                  AND ld.day = 'last_month_same_date';
