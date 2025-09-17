INSERT INTO oracle_ebs.chargeback_snapshot
(source,
 settlement_date,
 record_count,
 snapshot_datetime)
SELECT 'US'                          AS source,
       CAST(settlement_date AS DATE) AS settlement_date,
       COUNT(*)                      AS record_count,
       CURRENT_TIMESTAMP()           AS snapshot_datetime
FROM lake.oracle_ebs.chargeback_us
WHERE CAST(settlement_date AS DATE) > '2024-06-01'
GROUP BY CAST(settlement_date AS DATE)
UNION ALL
SELECT 'EU'                          AS source,
       CAST(settlement_date AS DATE) AS settlement_date,
       COUNT(*)                      AS record_count,
       CURRENT_TIMESTAMP()           AS snapshot_datetime
FROM lake.oracle_ebs.chargeback_eu
WHERE CAST(settlement_date AS DATE) > '2024-06-01'
GROUP BY settlement_date;
