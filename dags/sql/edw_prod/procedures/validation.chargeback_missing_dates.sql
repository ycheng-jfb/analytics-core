CREATE OR REPLACE TEMP TABLE _chargeback_missing_dates AS
WITH _dates AS (SELECT full_date
                FROM stg.dim_date
                WHERE full_date BETWEEN DATEADD(MM, -12, current_date) AND current_date - 1)
SELECT full_date AS missing_date,
       'US'      AS file_source
FROM _dates
         LEFT JOIN lake.oracle_ebs.chargeback_us
                   ON TO_DATE(full_date) = TO_DATE(settlement_date)
WHERE settlement_date IS NULL
UNION ALL
SELECT full_date AS missing_date,
       'EU'      AS file_source
FROM _dates
         LEFT JOIN lake.oracle_ebs.chargeback_eu
                   ON TO_DATE(full_date) = TO_DATE(settlement_date)
WHERE settlement_date IS NULL;

SELECT *
FROM _chargeback_missing_dates
ORDER BY missing_date;
