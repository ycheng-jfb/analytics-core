CREATE OR REPLACE view reference.reduction_from_returns_cost AS
SELECT
    store_id,
    store_full_name AS store_name,
    store_country,
    store_region,
    store_type,
    CASE
        WHEN upper(store_region) = 'EU' AND upper(store_brand_abbr) != 'SX' THEN 0.98
        WHEN upper(store_brand_abbr) = 'FK' THEN 0.9172
        WHEN upper(store_brand_abbr) = 'FL' THEN 0.8208
        WHEN upper(store_brand_abbr) = 'JF' THEN 0.7442
        WHEN upper(store_brand_abbr) = 'SD' THEN 0.5334
        WHEN upper(store_brand_abbr) = 'SX' AND upper(store_region)  = 'NA' THEN 0.85
        WHEN upper(store_brand_abbr) = 'SX' AND upper(store_region)  = 'EU' THEN 0.90
    END AS percent_resaleable,
    '1900-01-01'::TIMESTAMP_LTZ AS effective_start_date,
    CASE
        WHEN upper(store_brand_abbr || ' ' || store_region) = 'JF NA' OR upper(store_brand_abbr) = 'SD'
            THEN '2018-07-17' ::TIMESTAMP_LTZ
        ELSE '9999-12-31' ::TIMESTAMP_LTZ
    END AS effective_end_date
FROM stg.dim_store
WHERE
    store_id != 0
    AND is_current = 1

UNION ALL

SELECT
    store_id,
    store_full_name AS store_name,
    store_country,
    store_region,
    store_type,
    CASE
        WHEN upper(store_brand_abbr) = 'JF' THEN 0.939
        WHEN upper(store_brand_abbr) = 'SD' THEN 0.891
    END AS percent_resaleable,
    '2018-07-18' AS effective_start_date,
    '9999-12-31' AS effective_end_date
FROM stg.dim_store
WHERE
    store_id != 0
    AND is_current = 1
    AND (upper(store_brand_abbr || ' ' || store_region) = 'JF NA' OR upper(store_brand_abbr) = 'SD');
