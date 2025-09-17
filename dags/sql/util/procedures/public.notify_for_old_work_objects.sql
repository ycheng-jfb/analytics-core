/*
Includes commented-out lines for adhoc analysis
*/

WITH
    _user_access AS (
        SELECT
            ah.user_name,
            replace(f2.Value, '"', '') AS object_name,
            ah.query_start_time,
        FROM UTIL.PUBLIC.access_history AS ah
             , lateral flatten(base_objects_accessed) AS f1
             , lateral flatten(f1.value) AS f2
        WHERE f2.Path = 'objectName'
        QUALIFY ROW_NUMBER() OVER(PARTITION BY replace(f2.Value, '"', '') ORDER BY query_start_time DESC) = 1
    ),
    _user_attribution AS (
    SELECT DISTINCT
            U.NAME,
            CASE
                /* Use Workday Company */
                WHEN COMPANY IN ('105 TechStyle, Inc.', '432 TechStyle SLU')
                THEN 'TSOS'
                WHEN COMPANY IN ('180 Fabletics LLC', '410 Fabletics GmbH')
                THEN 'FBL'
                WHEN COMPANY IN ('110 JustFab, LLC.')
                THEN 'JFB'
                WHEN COMPANY IN ('190 Savage X Fenty', '415 Lavender Lingerie GmbH')
                THEN 'SXF'
                /* Use email domain */
                WHEN U.EMAIL ILIKE '%@TECHSTYLE.COM'
                THEN 'TSOS'
                WHEN U.EMAIL ILIKE '%@FABLETICS.COM'
                THEN 'FBL'
                WHEN U.EMAIL ILIKE '%@JUSTFAB.COM'
                THEN 'JFB'
                WHEN U.EMAIL ILIKE '%@SAVAGEX.COM'
                THEN 'SXF'
                ELSE NULL END AS ATTRIBUTED,
            E.COMPANY,
            E.BRAND
        FROM UTIL.PUBLIC.USERS AS U
        LEFT JOIN (
                SELECT
                    *,
                    ROW_NUMBER() OVER(PARTITION BY EMPLOYEE_ID ORDER BY START_DATE DESC) AS RNK
                FROM lake.workday.employees
                WHERE EMAIL LIKE '%@%'
                    AND EMAIL NOT LIKE '0%'
                    AND EMPLOYEE_ID NOT IN ('019365', '023928') /* Excluded employee_ids due to Email Duplication */
                QUALIFY ROW_NUMBER() OVER(PARTITION BY EMPLOYEE_ID ORDER BY START_DATE DESC) = 1
           ) AS E
            ON UPPER(U.EMAIL) = UPPER(E.EMAIL)
    )

SELECT
    UPPER(COALESCE(
        CASE WHEN T.LAST_DDL_BY ILIKE '%@%' THEN T.LAST_DDL_BY END,
        U.NAME, AH.user_name, T.LAST_DDL_BY, 'UNKNOWN'
        )) AS USER_NAME,
    /*
    CASE
        WHEN X.ATTRIBUTED IN ('TSOS', 'FBL', 'JFB', 'SXF')
        THEN X.ATTRIBUTED
        WHEN UPPER(COALESCE(T.LAST_DDL_BY, U.NAME, AH.user_name, 'UNKNOWN')) NOT ILIKE '%@%'
        THEN
            CASE
            WHEN T.TABLE_SCHEMA ILIKE ANY ('FABLETICS', 'SEGMENT_FL_DEV')
            THEN 'FBL'
            WHEN T.TABLE_SCHEMA ILIKE ANY ('GFB', 'SEGMENT_JF_DEV')
            THEN 'JFB'
            WHEN T.TABLE_SCHEMA ILIKE ANY ('SXF', 'SEGMENT_SX_DEV')
            THEN 'SXF'
            END
        END AS ATTRIBUTED,
    CASE
        WHEN UPPER(COALESCE(T.LAST_DDL_BY, U.NAME, AH.user_name)) IS NULL
        THEN 'User not found'
        WHEN UPPER(COALESCE(T.LAST_DDL_BY, U.NAME, AH.user_name)) IS NOT NULL
            AND UN.DELETED = TRUE
        THEN 'User Deleted'
        WHEN UPPER(COALESCE(T.LAST_DDL_BY, U.NAME, AH.user_name)) IS NOT NULL
            AND UN.DISABLED ILIKE 'true'
        THEN 'User Disabled'
        ELSE 'User Active'
        END AS USER_STATUS,
    */
    CONCAT(T.TABLE_CATALOG, '.', T.TABLE_SCHEMA, '.', T.TABLE_NAME) AS OBJECT_NAME,
    ROUND(COALESCE(BYTES,0)/1024.0/1024.0,1) AS MEGABYTES,
    DATEDIFF(DAY, T.LAST_ALTERED, CURRENT_DATE()) AS DAYS_OLD,
    DATEDIFF(DAY, COALESCE(AH.query_start_time, T.LAST_ALTERED), CURRENT_DATE()) AS DAYS_SINCE_LAST_ACCESSED,
    AH.user_name AS LAST_ACCESSED_BY,
    T.LAST_DDL_BY,
    CONCAT(
        'DROP ',
        CASE WHEN T.TABLE_TYPE = 'VIEW' THEN 'VIEW' ELSE 'TABLE' END,
        ' IF EXISTS ',
        T.TABLE_CATALOG, '.', T.TABLE_SCHEMA, '.', T.TABLE_NAME,
        '; '
        ) AS DROP_QUERY
FROM WORK.INFORMATION_SCHEMA.TABLES AS T
LEFT JOIN _user_access AS AH
    ON AH.object_name = CONCAT(T.TABLE_CATALOG, '.', T.TABLE_SCHEMA, '.', T.TABLE_NAME)
LEFT JOIN (
    SELECT
        NAME,
        CASE
            WHEN UPPER(SUBSTR(NAME, 0, POSITION('@' IN NAME) - 1)) ILIKE 'EAHUF%'
            THEN 'EAHUF'
            ELSE UPPER(SUBSTR(NAME, 0, POSITION('@' IN NAME) - 1))
        END AS SCHEMA_NAME
    FROM UTIL.PUBLIC.USERS
    WHERE SUBSTR(NAME, 0, POSITION('@' IN NAME) - 1) <> ''
    QUALIFY ROW_NUMBER() OVER(PARTITION BY NAME ORDER BY CREATED_ON DESC) = 1
    ) AS U
    ON T.TABLE_SCHEMA = U.SCHEMA_NAME
LEFT JOIN (
    SELECT
        NAME,
        DISABLED,
        CASE WHEN DELETED_ON IS NOT NULL THEN TRUE ELSE FALSE END AS DELETED
    FROM UTIL.PUBLIC.USERS
    WHERE SUBSTR(NAME, 0, POSITION('@' IN NAME) - 1) <> ''
    QUALIFY ROW_NUMBER() OVER(PARTITION BY NAME ORDER BY CREATED_ON DESC) = 1
    ) AS UN
    ON UPPER(UN.NAME) = UPPER(COALESCE(
        CASE WHEN T.LAST_DDL_BY ILIKE '%@%' THEN T.LAST_DDL_BY END,
        U.NAME, AH.user_name, T.LAST_DDL_BY, 'UNKNOWN'
        ))
LEFT JOIN _user_attribution AS X
    ON UPPER(X.NAME) = UPPER(COALESCE(
        CASE WHEN T.LAST_DDL_BY ILIKE '%@%' THEN T.LAST_DDL_BY END,
        U.NAME, AH.user_name, T.LAST_DDL_BY, 'UNKNOWN'
        ))
WHERE T.LAST_ALTERED <= DATEADD(DAY, -15, CURRENT_DATE())

    AND CASE
        WHEN COALESCE(
            CASE WHEN T.LAST_DDL_BY ILIKE '%@%' THEN T.LAST_DDL_BY END,
            U.NAME, AH.user_name, T.LAST_DDL_BY) IS NULL
        THEN 'User not found'
        WHEN COALESCE(
            CASE WHEN T.LAST_DDL_BY ILIKE '%@%' THEN T.LAST_DDL_BY END,
            U.NAME, AH.user_name, T.LAST_DDL_BY) IS NOT NULL
            AND UN.DELETED = TRUE
        THEN 'User Deleted'
        WHEN COALESCE(
            CASE WHEN T.LAST_DDL_BY ILIKE '%@%' THEN T.LAST_DDL_BY END,
            U.NAME, AH.user_name, T.LAST_DDL_BY) IS NOT NULL
            AND UN.DISABLED ILIKE 'true'
        THEN 'User Disabled'
        ELSE 'User Active'
        END = 'User Active'
    AND COALESCE(
        CASE WHEN T.LAST_DDL_BY ILIKE '%@%' THEN T.LAST_DDL_BY END,
        U.NAME, AH.user_name, T.LAST_DDL_BY, 'UNKNOWN'
        ) ILIKE '%@%'
ORDER BY USER_NAME ASC, OBJECT_NAME ASC
;
