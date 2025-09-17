SET low_watermark_ltz = %(low_watermark)s :: TIMESTAMP_LTZ(3);

CREATE OR REPLACE TEMP TABLE _session_uri_base AS
SELECT DISTINCT session_id
FROM (
    SELECT session_id
    FROM lake_consolidated_view.ultra_merchant.session AS s
    JOIN edw_prod.data_model.dim_store AS t
        ON t.store_id = s.store_id
    WHERE s.meta_update_datetime > $low_watermark_ltz
        AND t.store_type = 'Mobile App'

    UNION ALL
    SELECT session_id
    FROM reporting_base_prod.staging.session_uri
    WHERE meta_update_datetime > $low_watermark_ltz

    UNION ALL
    SELECT session_id
    FROM reporting_base_prod.staging.session_ga
    WHERE meta_update_datetime > $low_watermark_ltz) AS a
WHERE session_id IS NOT NULL
ORDER BY session_id ASC;

CREATE OR REPLACE TEMP TABLE _session_uri_stg AS
SELECT DISTINCT
    s.session_id,
    s.customer_id,
    s.store_id,
    s.datetime_added AS session_datetime,
    COALESCE(u.browser, ga.ga_browser) AS browser,
    CASE
        WHEN LOWER(u.os) <> 'unknown' AND u.os IS NOT NULL
        THEN u.os
        WHEN s.datetime_added <= '2021-12-05'
        THEN 'iOS'
        WHEN ga.ga_browser ILIKE '%%Safari%%' OR u.browser ILIKE '%%Safari%%'
        THEN 'iOS'
        ELSE NULL END AS os
FROM (
    SELECT
        s.session_id,
        s.customer_id,
        s.store_id,
        s.datetime_added
    FROM lake_consolidated_view.ultra_merchant.session AS s
    JOIN _session_uri_base AS x
        ON x.session_id = s.session_id) AS s
    JOIN edw_prod.data_model.dim_store AS t
        ON t.store_id = s.store_id
LEFT JOIN (
    SELECT
        u.session_id,
        u.browser,
        u.os
    FROM reporting_base_prod.staging.session_uri AS u
    JOIN _session_uri_base AS x
        ON u.session_id = x.session_id
    ) AS u
    ON u.session_id = s.session_id
LEFT JOIN (
    SELECT
        ga.session_id,
        ga.ga_browser
    FROM reporting_base_prod.staging.session_ga AS ga
    JOIN _session_uri_base AS x
        ON ga.session_id = x.session_id
    ) AS ga
    ON ga.session_id = s.session_id
WHERE t.store_type = 'Mobile App'
ORDER BY s.session_id ASC;


CREATE OR REPLACE TEMP TABLE _mobile_app_last_os AS
SELECT DISTINCT
    s.session_id,
    s.customer_id,
    u.browser,
    u.os,
    s.datetime_added AS session_datetime,
    COALESCE(LEAD(DATEADD('millisecond', -1, s.datetime_added)) OVER (PARTITION BY s.customer_id ORDER BY s.datetime_added), '9999-12-31') AS next_session_datetime
FROM lake_consolidated_view.ultra_merchant.session AS s
    JOIN (
        SELECT DISTINCT customer_id FROM _session_uri_stg WHERE os IS NULL
    ) AS stg
        ON stg.customer_id = s.customer_id
    JOIN reporting_base_prod.staging.session_uri AS u
        ON u.session_id = s.session_id
    JOIN edw_prod.data_model.dim_store AS t
        ON t.store_id = s.store_id
WHERE t.store_type = 'Mobile App'
    AND u.os IS NOT NULL
    AND LOWER(u.os) <> 'unknown'
ORDER BY s.session_id ASC;


CREATE OR REPLACE TEMP TABLE _mobile_last_os AS
SELECT DISTINCT
    s.session_id,
    s.customer_id,
    u.browser,
    u.os,
    NVL(INITCAP(IFF(NVL(t.store_type, 'Online') = 'Mobile App', 'Mobile App', COALESCE(u.device, ga.ga_device))), 'Unknown')
    AS platform,
    s.datetime_added AS session_datetime,
    COALESCE(LEAD(DATEADD('millisecond',-1,s.datetime_added)) OVER (PARTITION BY s.customer_id ORDER BY s.datetime_added), '9999-12-31')
    AS next_session_datetime
FROM lake_consolidated_view.ultra_merchant.session AS s
    JOIN (
        SELECT DISTINCT customer_id FROM _session_uri_stg WHERE os IS NULL
    ) AS stg
        ON stg.customer_id = s.customer_id
    JOIN edw_prod.data_model.dim_store AS t
        ON t.store_id = s.store_id
    LEFT JOIN reporting_base_prod.staging.session_uri AS u
        ON u.session_id = s.session_id
    LEFT JOIN reporting_base_prod.staging.session_ga AS ga
        ON ga.session_id = s.session_id
WHERE platform = 'Mobile'
    AND u.os IS NOT NULL
    AND LOWER(u.os) <> 'unknown'
ORDER BY s.session_id ASC;

CREATE OR REPLACE TEMP TABLE _mobile_app_session_os_updated_stg AS
SELECT DISTINCT
    s.session_id,
    s.customer_id,
    s.browser,
    COALESCE(s.os, ma.os, m.os, 'Unknown') AS os,
    CONVERT_TIMEZONE(NVL(st.time_zone, 'America/Los_Angeles'),
    s.session_datetime::TIMESTAMP_TZ) AS session_local_datetime
FROM _session_uri_stg AS s
LEFT JOIN _mobile_app_last_os AS ma
    ON ma.customer_id = s.customer_id
    AND s.session_datetime BETWEEN ma.session_datetime AND ma.next_session_datetime
LEFT JOIN _mobile_last_os AS m
    ON m.customer_id = s.customer_id
    AND s.session_datetime BETWEEN m.session_datetime AND m.next_session_datetime
LEFT JOIN edw_prod.reference.store_timezone AS st
    ON st.store_id = s.store_id
ORDER BY s.session_id ASC;

MERGE INTO shared.mobile_app_session_os_updated AS t
USING (
    SELECT
        session_id,
        customer_id,
        browser,
        os,
        session_local_datetime,
        edw_prod.stg.udf_unconcat_brand(session_id) AS meta_original_session_id,
        HASH(session_id, customer_id, browser, os) AS meta_row_hash
    FROM _mobile_app_session_os_updated_stg s
    ORDER BY s.session_id
    ) AS src
    ON src.session_id = t.session_id
    WHEN NOT MATCHED THEN INSERT (
        session_id,
        customer_id,
        browser,
        os,
        session_local_datetime,
        meta_original_session_id,
        meta_row_hash)
    VALUES (
        src.session_id,
        src.customer_id,
        src.browser,
        src.os,
        src.session_local_datetime,
        src.meta_original_session_id,
        src.meta_row_hash)
    WHEN MATCHED
        AND src.meta_row_hash != t.meta_row_hash
    THEN UPDATE SET
        t.customer_id = src.customer_id,
        t.browser = src.browser,
        t.os = src.os,
        t.session_local_datetime = src.session_local_datetime,
        t.meta_original_session_id = src.meta_original_session_id,
        t.meta_row_hash = src.meta_row_hash,
        t.meta_update_datetime = CURRENT_TIMESTAMP;
