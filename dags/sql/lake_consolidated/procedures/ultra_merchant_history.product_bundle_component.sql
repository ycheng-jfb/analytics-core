CREATE or replace TRANSIENT TABLE lake_consolidated.ultra_merchant_history.PRODUCT_BUNDLE_COMPONENT (
    data_source_id INT,
    meta_company_id INT,
    product_bundle_component_id INT,
    bundle_product_id INT,
    component_product_id INT,
    price_contribution_percentage DOUBLE,
    is_free INT,
    datetime_added TIMESTAMP_NTZ(3),
    datetime_modified TIMESTAMP_NTZ(3),
    meta_original_product_bundle_component_id INT,
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
        FROM lake_jfb.ultra_merchant_history.PRODUCT_BUNDLE_COMPONENT
    ) AS A
);


CREATE OR REPLACE TEMP TABLE _lake_jfb_product_bundle_component_history AS
SELECT DISTINCT

    product_bundle_component_id,
    bundle_product_id,
    component_product_id,
    price_contribution_percentage,
    is_free,
    datetime_added,
    datetime_modified,
    meta_row_source,
    hvr_change_op,
    CAST(META_SOURCE_CHANGE_DATETIME AS TIMESTAMP_LTZ(3))  as effective_start_datetime,
    10 as data_source_id
FROM lake_jfb.ultra_merchant_history.PRODUCT_BUNDLE_COMPONENT
WHERE META_SOURCE_CHANGE_DATETIME >= DATEADD(MINUTE, -5, $lake_jfb_watermark)
QUALIFY ROW_NUMBER() OVER(PARTITION BY product_bundle_component_id, META_SOURCE_CHANGE_DATETIME
    ORDER BY HVR_CHANGE_SEQUENCE DESC) = 1;


CREATE OR REPLACE TEMP TABLE _lake_jfb_company_product_bundle_component AS
SELECT product_bundle_component_id, company_id as meta_company_id
FROM (
      SELECT DISTINCT
         L.product_bundle_component_id,
         DS.company_id
         from
             lake_jfb.REFERENCE.dim_store ds
            join lake_jfb.ultra_merchant.product p
                on ds.store_group_id=p.store_group_id
            join lake_jfb.ultra_merchant_history.product_bundle_component L
                         on L.bundle_product_id=p.product_id
    WHERE l.META_SOURCE_CHANGE_DATETIME >= DATEADD(MINUTE, -5, $lake_jfb_watermark)
    ) AS l
QUALIFY ROW_NUMBER() OVER(PARTITION BY product_bundle_component_id ORDER BY company_id ASC) = 1;


INSERT INTO lake_consolidated.ultra_merchant_history.PRODUCT_BUNDLE_COMPONENT (
    data_source_id,
    meta_company_id,
    product_bundle_component_id,
    bundle_product_id,
    component_product_id,
    price_contribution_percentage,
    is_free,
    datetime_added,
    datetime_modified,
    meta_original_product_bundle_component_id,
    meta_row_source,
    hvr_change_op,
    effective_start_datetime,
    effective_end_datetime,
    meta_update_datetime
)
SELECT DISTINCT
    s.data_source_id,
    s.meta_company_id,

    CASE WHEN NULLIF(s.product_bundle_component_id, '0') IS NOT NULL THEN CONCAT(s.product_bundle_component_id, s.meta_company_id) ELSE NULL END as product_bundle_component_id,
    CASE WHEN NULLIF(s.bundle_product_id, '0') IS NOT NULL THEN CONCAT(s.bundle_product_id, s.meta_company_id) ELSE NULL END as bundle_product_id,
    CASE WHEN NULLIF(s.component_product_id, '0') IS NOT NULL THEN CONCAT(s.component_product_id, s.meta_company_id) ELSE NULL END as component_product_id,
    s.price_contribution_percentage,
    s.is_free,
    s.datetime_added,
    s.datetime_modified,
    s.product_bundle_component_id as meta_original_product_bundle_component_id,
    s.meta_row_source,
    s.hvr_change_op,
    s.effective_start_datetime,
    NULL AS effective_end_datetime,
    CURRENT_TIMESTAMP() AS meta_update_datetime
FROM (
    SELECT
        A.*,
        COALESCE(CJ.meta_company_id, 40) AS meta_company_id
    FROM _lake_jfb_product_bundle_component_history AS A
    LEFT JOIN _lake_jfb_company_product_bundle_component AS CJ
        ON cj.product_bundle_component_id = a.product_bundle_component_id
    WHERE COALESCE(CJ.meta_company_id, 40) IN (10)
    ) AS s
WHERE NOT EXISTS (
    SELECT
        1
    FROM lake_consolidated.ultra_merchant_history.PRODUCT_BUNDLE_COMPONENT AS t
    WHERE
        s.data_source_id = t.data_source_id
        AND          s.product_bundle_component_id = t.meta_original_product_bundle_component_id
        AND s.effective_start_datetime = t.effective_start_datetime
    )
ORDER BY s.effective_start_datetime ASC;


CREATE OR REPLACE temp table _product_bundle_component_updates as
SELECT s.*,
    row_number() over(partition by s.data_source_id,

        s.meta_original_product_bundle_component_id
ORDER BY s.effective_start_datetime ASC) AS rnk
FROM (
SELECT DISTINCT
    s.data_source_id,

        s.meta_original_product_bundle_component_id,
    s.effective_start_datetime,
    s.effective_end_datetime
FROM lake_consolidated.ultra_merchant_history.PRODUCT_BUNDLE_COMPONENT as s
INNER JOIN (
    SELECT
        data_source_id,

        meta_original_product_bundle_component_id,
        effective_start_datetime
    FROM lake_consolidated.ultra_merchant_history.PRODUCT_BUNDLE_COMPONENT
    WHERE effective_end_datetime is NULL
    ) AS t
    ON s.data_source_id = t.data_source_id
    AND     s.meta_original_product_bundle_component_id = t.meta_original_product_bundle_component_id
WHERE (
    s.effective_start_datetime >= t.effective_start_datetime
    OR s.effective_end_datetime = '9999-12-31 00:00:00.000 -0800'
)) s;


CREATE OR REPLACE TEMP TABLE _product_bundle_component_delta AS
SELECT
    s.data_source_id,

        s.meta_original_product_bundle_component_id,
    s.effective_start_datetime,
    COALESCE(dateadd(MILLISECOND, -1, t.effective_start_datetime), '9999-12-31 00:00:00.000 -0800') AS new_effective_end_datetime
FROM _product_bundle_component_updates AS s
    LEFT JOIN _product_bundle_component_updates AS t
    ON s.data_source_id = t.data_source_id
    AND     s.meta_original_product_bundle_component_id = t.meta_original_product_bundle_component_id
    AND S.rnk = T.rnk - 1
WHERE (S.effective_end_datetime <> dateadd(MILLISECOND, -1, T.effective_start_datetime)
    OR S.effective_end_datetime IS NULL) ;


UPDATE lake_consolidated.ultra_merchant_history.PRODUCT_BUNDLE_COMPONENT AS t
SET t.effective_end_datetime = s.new_effective_end_datetime,
    META_UPDATE_DATETIME = current_timestamp()
FROM _product_bundle_component_delta AS s
WHERE s.data_source_id = t.data_source_id
    AND     s.meta_original_product_bundle_component_id = t.meta_original_product_bundle_component_id
    AND s.effective_start_datetime = t.effective_start_datetime
    AND COALESCE(t.effective_end_datetime, '1900-01-01 00:00:00 -0800') <> s.new_effective_end_datetime;
