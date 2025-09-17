SET target_table = 'stg.dim_address';
SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
--SET is_full_refresh = (SELECT IFF(stg.udf_get_watermark($target_table, NULL) = '1900-01-01'::TIMESTAMP_LTZ(3), TRUE, FALSE));


-- Initial Load / Full Refresh
UPDATE stg.meta_table_dependency_watermark
SET high_watermark_datetime = '1900-01-01',
    meta_update_datetime = CURRENT_TIMESTAMP()
WHERE table_name = $target_table;
SET is_full_refresh = TRUE;


-- Use watermark variables for each dependent table to allow pruning of micro-partitions which doesn't happen with UDFs.
SET wm_self = (SELECT stg.udf_get_watermark($target_table, NULL));
SET wm_lake_ultra_merchant_address = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.address'));

/*
SELECT
    $wm_self,
    $wm_lake_ultra_merchant_address;
*/

CREATE OR REPLACE TEMP TABLE _dim_address__address_base(address_id int);

-- Full Refresh
INSERT INTO _dim_address__address_base (address_id)
SELECT DISTINCT a.address_id
FROM lake_consolidated.ultra_merchant.address AS a
WHERE $is_full_refresh = TRUE
ORDER BY a.address_id;

-- Incremental Refresh
INSERT INTO _dim_address__address_base (address_id)
SELECT DISTINCT incr.address_id
FROM (
    -- Self-check for manual updates
    SELECT address_id
    FROM stg.dim_address
    WHERE meta_update_datetime > $wm_self

    UNION ALL

    SELECT a.address_id
    FROM lake_consolidated.ultra_merchant.address AS a
    WHERE a.meta_update_datetime > $wm_lake_ultra_merchant_address

    UNION ALL /* previously errored rows */

    SELECT address_id
    FROM excp.dim_address
--    WHERE meta_is_current_excp
--        AND meta_data_quality = 'error'
    ) AS incr
WHERE NOT $is_full_refresh
    AND incr.address_id IS NOT NULL
ORDER BY incr.address_id;

CREATE OR REPLACE TEMP TABLE _dim_address__data AS
SELECT
    a.address_id,
    a.meta_original_address_id,
    CASE WHEN a.address1 IS NULL OR a.address1 ='' THEN 'Unknown' ELSE TRIM(a.address1) END AS street_address_1,
    CASE WHEN a.address2 IS NULL OR a.address2 ='' THEN 'Unknown' ELSE TRIM(a.address2) END AS street_address_2,
    CASE WHEN a.city IS NULL OR a.city ='' THEN 'Unknown' ELSE TRIM(a.city) END AS city,
    COALESCE(TRIM(REGEXP_REPLACE(TRIM(UPPER(a.state)), '[^A-Z]+', '')), 'Unknown') AS state,
    CASE WHEN a.state IS NULL OR a.state ='' THEN 'Unknown' ELSE TRIM(a.state) END AS state_orig,
    FALSE AS is_state_valid,
    COALESCE(TRIM(REGEXP_REPLACE(LTRIM(UPPER(a.zip), '[^- ]+'), '[^0-9A-Z -]+', '')), 'Unknown') AS zip_code,
    CASE WHEN a.zip IS NULL OR a.zip ='' THEN 'Unknown' ELSE TRIM(a.zip) END AS zip_code_orig,
    FALSE AS is_zip_code_valid,
    CASE WHEN a.country_code IS NULL OR a.country_code ='' THEN 'Unknown' ELSE TRIM(UPPER(a.country_code)) END AS country_code,
    IFF(CONTAINS(UPPER(a.zip), 'REMOVED'), TRUE, FALSE) AS is_removed
FROM lake_consolidated.ultra_merchant.address AS a
    JOIN _dim_address__address_base AS base
         ON base.address_id = a.address_id;

-- Update blank values
UPDATE _dim_address__data AS d
SET
    d.city = IFF(LENGTH(d.city) = 0, 'Unknown', d.city),
    d.state = IFF(LENGTH(d.state) = 0, 'Unknown', d.state),
    d.zip_code = IFF(LENGTH(d.zip_code) = 0, 'Unknown', d.zip_code)
WHERE (LENGTH(d.city) = 0 OR LENGTH(d.state) = 0 OR LENGTH(d.zip_code) = 0);

-- Identify valid zip codes
UPDATE _dim_address__data AS d
SET
    d.zip_code = CASE
        WHEN d.is_removed THEN 'Unknown'
        WHEN REGEXP_LIKE(d.zip_code, '[0-9]{5}') THEN d.zip_code
        WHEN REGEXP_LIKE(d.zip_code, '[0-9]{5}-[0-9]{4}') THEN d.zip_code
        WHEN REGEXP_LIKE(d.zip_code, '[0-9]{9}') THEN CONCAT(LEFT(d.zip_code, 5), '-', RIGHT(d.zip_code, 4))
        WHEN REGEXP_LIKE(LEFT(d.zip_code, 6), '[0-9]{5}-') THEN LEFT(d.zip_code, 5)
        WHEN UPPER(d.zip_code) IN ('UNKNOWN', '') THEN 'Unknown'
        ELSE d.zip_code END,
    d.is_zip_code_valid = TRUE
WHERE d.country_code = 'US'
    AND (d.is_removed
        OR REGEXP_LIKE(d.zip_code, '[0-9]{5}')
        OR REGEXP_LIKE(d.zip_code, '[0-9]{5}-[0-9]{4}')
        OR REGEXP_LIKE(d.zip_code, '[0-9]{9}')
        OR REGEXP_LIKE(LEFT(d.zip_code, 6), '[0-9]{5}-')
        OR UPPER(d.zip_code) IN ('UNKNOWN', ''));

-- Update state by matching city and zip
UPDATE _dim_address__data AS d
SET d.state = z.state, d.is_state_valid = TRUE
FROM (SELECT DISTINCT city, UPPER(state) AS state, zipcode FROM reference.zip_code) AS z
WHERE d.country_code = 'US'
    AND NOT d.is_state_valid
    AND z.zipcode = LEFT(d.zip_code, 5)
    AND z.city ILIKE CONCAT('%%', d.city, '%%');

-- Update state by matching zip
UPDATE _dim_address__data AS d
SET d.state = z.state, d.is_state_valid = TRUE
FROM (SELECT DISTINCT UPPER(state) AS state, zipcode FROM reference.zip_code) AS z
WHERE d.country_code = 'US'
    AND NOT d.is_state_valid
    AND d.is_zip_code_valid
    AND z.zipcode = LEFT(d.zip_code, 5);

-- Update state by matching city
UPDATE _dim_address__data AS d
SET d.state = z.state, d.is_state_valid = TRUE
FROM (SELECT DISTINCT city, UPPER(state) AS state, UPPER(statefullname) AS statefullname, zipcode FROM reference.zip_code) AS z
WHERE d.country_code = 'US'
    AND NOT d.is_state_valid
    AND LEFT(z.statefullname, 3) = LEFT(d.state, 3)
    AND z.city ILIKE CONCAT('%%', d.city, '%%');

-- Identify valid states
UPDATE _dim_address__data AS d
SET d.is_state_valid = TRUE
WHERE d.country_code = 'US'
    AND NOT d.is_state_valid
    AND d.state IN (SELECT DISTINCT UPPER(state) AS state FROM reference.zip_code);

-- Identify invalid states
UPDATE _dim_address__data AS d
SET d.state = 'Unknown', d.is_state_valid = TRUE
WHERE d.country_code = 'US'
    AND NOT d.is_state_valid
    AND (REGEXP_REPLACE(CONCAT(UPPER(d.street_address_1), ''), '[^A-Z0-9]+', '') IN ('UNKNOWN', '')
        OR REGEXP_REPLACE(CONCAT(UPPER(d.city), ''), '[^A-Z]+', '') IN ('UNKNOWN', '')
        OR REGEXP_REPLACE(CONCAT(UPPER(d.state), ''), '[^A-Z]+', '') IN ('UNKNOWN', '')
        OR REGEXP_REPLACE(CONCAT(UPPER(d.zip_code), ''), '[^A-Z0-9]+', '') IN ('UNKNOWN', ''));

-- SELECT COUNT(1) FROM _dim_address__data WHERE country_code = 'US' AND NOT is_zip_code_valid;
-- SELECT COUNT(1) FROM _dim_address__data WHERE country_code = 'US' AND NOT is_state_valid;

CREATE OR REPLACE TEMP TABLE _dim_address__pre_stg AS
SELECT
    d.address_id,
    d.meta_original_address_id,
    d.street_address_1,
    d.street_address_2,
    IFF(d.is_removed, '[removed]', d.city) AS city,
    IFF(d.is_removed, '[removed]', d.state) AS state,
    IFF(d.is_removed, '[removed]', d.zip_code) AS zip_code,
    d.country_code,
    IFF(d.country_code = 'US', d.is_state_valid, NULL) AS is_state_valid,
    IFF(d.country_code = 'US', d.is_zip_code_valid, NULL) AS is_zip_code_valid
FROM _dim_address__data AS d;
-- SELECT * FROM _dim_address__pre_stg;

INSERT INTO stg.dim_address_stg (
    address_id,
    meta_original_address_id,
    street_address_1,
    street_address_2,
    city,
    state,
    zip_code,
    country_code,
    is_state_valid,
    is_zip_code_valid,
    meta_create_datetime,
    meta_update_datetime
    )
SELECT
    address_id,
    meta_original_address_id,
    street_address_1,
    street_address_2,
    city,
    state,
    zip_code,
    country_code,
    is_state_valid,
    is_zip_code_valid,
    $execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM _dim_address__pre_stg
ORDER BY address_id;
