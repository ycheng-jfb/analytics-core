SELECT
    db.database_name,
    CAST(db.created AS DATE) AS date_cloned,
    CAST(MAX(COALESCE(u.last_altered, db.created)) AS DATE) AS last_write_activity
FROM snowflake.account_usage.databases AS db
LEFT JOIN (
    /* Grab last Write activity */
    SELECT
        database_name AS db_name,
        MAX(last_altered) AS last_altered
    FROM snowflake.account_usage.databases
    WHERE database_name ILIKE '%EDW%DEV%'
    GROUP BY database_name

    UNION ALL
    SELECT
        catalog_name AS db_name,
        MAX(last_altered) AS last_altered
    FROM snowflake.account_usage.schemata
    WHERE catalog_name ILIKE '%EDW%DEV%'
    GROUP BY catalog_name

    UNION ALL
    SELECT
        table_catalog AS db_name,
        MAX(last_altered) AS last_altered
    FROM snowflake.account_usage.tables
    WHERE table_catalog ILIKE '%EDW%DEV%'
    GROUP BY table_catalog
    ) AS u
    ON db.database_name = u.db_name
WHERE db.deleted IS NULL
  AND db.database_name ILIKE '%EDW%DEV%'
  AND db.database_name NOT IN ('DBT_EDW_DEV')
  AND db.created < DATEADD(DAY, -7, CURRENT_DATE())
GROUP BY db.database_name,
    CAST(db.created AS DATE)
ORDER BY db.database_name ASC;
