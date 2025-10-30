CREATE or replace TRANSIENT TABLE lake_consolidated.ultra_merchant_history.PRODUCT_CATEGORY (
    data_source_id INT,
    meta_company_id INT,
    product_category_id INT,
    store_group_id INT,
    parent_product_category_id INT,
    label VARCHAR(50),
    short_description VARCHAR(255),
    medium_description VARCHAR(2000),
    long_description VARCHAR,
    full_image_content_id INT,
    thumbnail_image_content_id INT,
    active INT,
    datetime_added TIMESTAMP_NTZ(3),
    datetime_modified TIMESTAMP_NTZ(3),
    product_category_type VARCHAR(50),
    archive BOOLEAN,
    meta_original_product_category_id INT,
    meta_original_parent_product_category_id INT,
    meta_row_source VARCHAR(40),
    hvr_change_op INT,
    effective_start_datetime TIMESTAMP_LTZ(3),
    effective_end_datetime TIMESTAMP_LTZ(3),
    meta_update_datetime TIMESTAMP_LTZ(3)
);


SET lake_jfb_watermark = (
    SELECT MIN(last_update)
    FROM (
        SELECT CAST(min(META_SOURCE_CHANGE_DATETIME) AS TIMESTAMP_LTZ(3)) AS last_update
        FROM lake_jfb.ultra_merchant_history.PRODUCT_CATEGORY
    ) AS A
);


CREATE OR REPLACE TEMP TABLE _lake_jfb_product_category_history AS
SELECT DISTINCT

    product_category_id,
    store_group_id,
    parent_product_category_id,
    label,
    short_description,
    medium_description,
    long_description,
    full_image_content_id,
    thumbnail_image_content_id,
    active,
    datetime_added,
    datetime_modified,
    product_category_type,
    archive,
    meta_row_source,
    hvr_change_op,
    CAST(META_SOURCE_CHANGE_DATETIME AS TIMESTAMP_LTZ(3))  as effective_start_datetime,
    10 as data_source_id
FROM lake_jfb.ultra_merchant_history.PRODUCT_CATEGORY
WHERE META_SOURCE_CHANGE_DATETIME >= DATEADD(MINUTE, -5, $lake_jfb_watermark)
QUALIFY ROW_NUMBER() OVER(PARTITION BY product_category_id, META_SOURCE_CHANGE_DATETIME
    ORDER BY HVR_CHANGE_SEQUENCE DESC) = 1;


CREATE OR REPLACE TEMP TABLE _lake_jfb_company_product_category AS
SELECT product_category_id, company_id as meta_company_id
FROM (
     SELECT product_category_id, ds.company_id
     FROM lake_jfb.ultra_merchant_history.product_category l
     JOIN (
        SELECT DISTINCT company_id
        FROM lake_jfb.REFERENCE.DIM_STORE
        WHERE company_id IS NOT NULL
        ) AS DS

    WHERE l.META_SOURCE_CHANGE_DATETIME >= DATEADD(MINUTE, -5, $lake_jfb_watermark)
    ) AS l ;


INSERT INTO lake_consolidated.ultra_merchant_history.PRODUCT_CATEGORY (
    data_source_id,
    meta_company_id,
    product_category_id,
    store_group_id,
    parent_product_category_id,
    label,
    short_description,
    medium_description,
    long_description,
    full_image_content_id,
    thumbnail_image_content_id,
    active,
    datetime_added,
    datetime_modified,
    product_category_type,
    archive,
    meta_original_product_category_id,
    meta_row_source,
    hvr_change_op,
    effective_start_datetime,
    effective_end_datetime,
    meta_update_datetime
)
SELECT DISTINCT
    s.data_source_id,
    s.meta_company_id,

    CASE WHEN NULLIF(s.product_category_id, '0') IS NOT NULL THEN CONCAT(s.product_category_id, s.meta_company_id) ELSE NULL END as product_category_id,
    s.store_group_id,
    CASE WHEN NULLIF(s.parent_product_category_id, '0') IS NOT NULL THEN CONCAT(s.parent_product_category_id, s.meta_company_id) ELSE NULL END as parent_product_category_id,
    s.label,
    s.short_description,
    s.medium_description,
    s.long_description,
    s.full_image_content_id,
    s.thumbnail_image_content_id,
    s.active,
    s.datetime_added,
    s.datetime_modified,
    s.product_category_type,
    s.archive,
    s.product_category_id as meta_original_product_category_id,
    s.meta_row_source,
    s.hvr_change_op,
    s.effective_start_datetime,
    NULL AS effective_end_datetime,
    CURRENT_TIMESTAMP() AS meta_update_datetime
FROM (
    SELECT
        A.*,
        COALESCE(CJ.meta_company_id, 40) AS meta_company_id
    FROM _lake_jfb_product_category_history AS A
    LEFT JOIN _lake_jfb_company_product_category AS CJ
        ON cj.product_category_id = a.product_category_id
    WHERE COALESCE(CJ.meta_company_id, 40) IN (10)
    ) AS s
WHERE NOT EXISTS (
    SELECT
        1
    FROM lake_consolidated.ultra_merchant_history.PRODUCT_CATEGORY AS t
    WHERE
        s.data_source_id = t.data_source_id
        AND  s.meta_company_id = t.meta_company_id
        AND          s.product_category_id = t.meta_original_product_category_id
        AND s.effective_start_datetime = t.effective_start_datetime
    )
ORDER BY s.effective_start_datetime ASC;


CREATE OR REPLACE temp table _product_category_updates as
SELECT s.*,
    row_number() over(partition by s.data_source_id,
s.meta_company_id,

        s.meta_original_product_category_id
ORDER BY s.effective_start_datetime ASC) AS rnk
FROM (
SELECT DISTINCT
    s.data_source_id,
s.meta_company_id,

        s.meta_original_product_category_id,
    s.effective_start_datetime,
    s.effective_end_datetime
FROM lake_consolidated.ultra_merchant_history.PRODUCT_CATEGORY as s
INNER JOIN (
    SELECT
        data_source_id,
meta_company_id,

        meta_original_product_category_id,
        effective_start_datetime
    FROM lake_consolidated.ultra_merchant_history.PRODUCT_CATEGORY
    WHERE effective_end_datetime is NULL
    ) AS t
    ON s.data_source_id = t.data_source_id
    AND s.meta_company_id = t.meta_company_id
    AND     s.meta_original_product_category_id = t.meta_original_product_category_id
WHERE (
    s.effective_start_datetime >= t.effective_start_datetime
    OR s.effective_end_datetime = '9999-12-31 00:00:00.000 -0800'
)) s;


CREATE OR REPLACE TEMP TABLE _product_category_delta AS
SELECT
    s.data_source_id,
s.meta_company_id,

        s.meta_original_product_category_id,
    s.effective_start_datetime,
    COALESCE(dateadd(MILLISECOND, -1, t.effective_start_datetime), '9999-12-31 00:00:00.000 -0800') AS new_effective_end_datetime
FROM _product_category_updates AS s
    LEFT JOIN _product_category_updates AS t
    ON s.data_source_id = t.data_source_id
    AND s.meta_company_id = t.meta_company_id
    AND     s.meta_original_product_category_id = t.meta_original_product_category_id
    AND S.rnk = T.rnk - 1
WHERE (S.effective_end_datetime <> dateadd(MILLISECOND, -1, T.effective_start_datetime)
    OR S.effective_end_datetime IS NULL);


UPDATE lake_consolidated.ultra_merchant_history.PRODUCT_CATEGORY AS t
SET t.effective_end_datetime = s.new_effective_end_datetime,
    META_UPDATE_DATETIME = current_timestamp()
FROM _product_category_delta AS s
WHERE s.data_source_id = t.data_source_id
    AND s.meta_company_id = t.meta_company_id
    AND     s.meta_original_product_category_id = t.meta_original_product_category_id
    AND s.effective_start_datetime = t.effective_start_datetime
    AND COALESCE(t.effective_end_datetime, '1900-01-01 00:00:00 -0800') <> s.new_effective_end_datetime;
