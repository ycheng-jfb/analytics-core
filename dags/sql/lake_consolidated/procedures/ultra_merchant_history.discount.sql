--Full refresh
create or replace TRANSIENT TABLE lake_consolidated.ultra_merchant_history.DISCOUNT (
    data_source_id INT,
    meta_company_id INT,
    discount_id INT,
    applied_to VARCHAR(15),
    calculation_method VARCHAR(25),
    label VARCHAR(50),
    percentage DOUBLE,
    rate NUMBER(19, 4),
    datetime_added TIMESTAMP_NTZ(3),
    date_expires TIMESTAMP_NTZ(0),
    statuscode INT,
    datetime_modified TIMESTAMP_NTZ(3),
    meta_original_discount_id INT,
    meta_row_source VARCHAR(40),
    hvr_change_op INT,
    effective_start_datetime TIMESTAMP_LTZ(3),
    effective_end_datetime TIMESTAMP_LTZ(3),
    meta_update_datetime TIMESTAMP_LTZ(3)
);

SET lake_jfb_watermark = (
    select min(last_update)
    from (
        select cast(min(META_SOURCE_CHANGE_DATETIME) as TIMESTAMP_LTZ(3)) AS last_update
        FROM lake_jfb.ultra_merchant_history.DISCOUNT
    ) AS A
);


CREATE OR REPLACE TEMP TABLE _lake_jfb_discount_history AS
SELECT DISTINCT

    discount_id,
    applied_to,
    calculation_method,
    label,
    percentage,
    rate,
    datetime_added,
    date_expires,
    statuscode,
    datetime_modified,
    meta_row_source,
    hvr_change_op,
    CAST(META_SOURCE_CHANGE_DATETIME AS TIMESTAMP_LTZ(3))  as effective_start_datetime,
    10 as data_source_id
FROM lake_jfb.ultra_merchant_history.DISCOUNT
WHERE META_SOURCE_CHANGE_DATETIME >= DATEADD(MINUTE, -5, $lake_jfb_watermark)
QUALIFY ROW_NUMBER() OVER(PARTITION BY discount_id, META_SOURCE_CHANGE_DATETIME
    ORDER BY HVR_CHANGE_SEQUENCE DESC) = 1;


CREATE OR REPLACE TEMP TABLE _lake_jfb_company_discount AS
SELECT discount_id, company_id as meta_company_id
FROM (
    SELECT DISTINCT
        L.discount_id,
        DS.company_id
    FROM lake_jfb.ultra_merchant_history.discount AS L
    JOIN (
        SELECT DISTINCT company_id
        FROM lake_jfb.REFERENCE.DIM_STORE
        WHERE company_id IS NOT NULL
        ) AS DS
    WHERE l.META_SOURCE_CHANGE_DATETIME >= DATEADD(MINUTE, -5, $lake_jfb_watermark)
    ) AS l ;


INSERT INTO lake_consolidated.ultra_merchant_history.DISCOUNT (
    data_source_id,
    meta_company_id,
    discount_id,
    applied_to,
    calculation_method,
    label,
    percentage,
    rate,
    datetime_added,
    date_expires,
    statuscode,
    datetime_modified,
    meta_original_discount_id,
    meta_row_source,
    hvr_change_op,
    effective_start_datetime,
    effective_end_datetime,
    meta_update_datetime
)
SELECT DISTINCT
    s.data_source_id,
    s.meta_company_id,

    CASE WHEN NULLIF(s.discount_id, '0') IS NOT NULL THEN CONCAT(s.discount_id, s.meta_company_id) ELSE NULL END as discount_id,
    s.applied_to,
    s.calculation_method,
    s.label,
    s.percentage,
    s.rate,
    s.datetime_added,
    s.date_expires,
    s.statuscode,
    s.datetime_modified,
    s.discount_id as meta_original_discount_id,
    s.meta_row_source,
    s.hvr_change_op,
    s.effective_start_datetime,
    NULL AS effective_end_datetime,
    CURRENT_TIMESTAMP() AS meta_update_datetime
FROM (
    SELECT
        A.*,
        COALESCE(CJ.meta_company_id, 40) AS meta_company_id
    FROM _lake_jfb_discount_history AS A
    LEFT JOIN _lake_jfb_company_discount AS CJ
        ON cj.discount_id = a.discount_id
    WHERE COALESCE(CJ.meta_company_id, 40) IN (10)
    ) AS s
WHERE NOT EXISTS (
    SELECT
        1
    FROM lake_consolidated.ultra_merchant_history.DISCOUNT AS t
    WHERE
        s.data_source_id = t.data_source_id
        AND  s.meta_company_id = t.meta_company_id
        AND          s.discount_id = t.meta_original_discount_id
        AND s.effective_start_datetime = t.effective_start_datetime
    )
ORDER BY s.effective_start_datetime ASC;


CREATE OR REPLACE temp table _discount_updates as
SELECT s.*,
    row_number() over(partition by s.data_source_id,
s.meta_company_id,

        s.meta_original_discount_id
ORDER BY s.effective_start_datetime ASC) AS rnk
FROM (
SELECT DISTINCT
    s.data_source_id,
s.meta_company_id,

        s.meta_original_discount_id,
    s.effective_start_datetime,
    s.effective_end_datetime
FROM lake_consolidated.ultra_merchant_history.DISCOUNT as s
INNER JOIN (
    SELECT
        data_source_id,
meta_company_id,

        meta_original_discount_id,
        effective_start_datetime
    FROM lake_consolidated.ultra_merchant_history.DISCOUNT
    WHERE effective_end_datetime is NULL
    ) AS t
    ON s.data_source_id = t.data_source_id
    AND s.meta_company_id = t.meta_company_id
    AND     s.meta_original_discount_id = t.meta_original_discount_id
WHERE (
    s.effective_start_datetime >= t.effective_start_datetime
    OR s.effective_end_datetime = '9999-12-31 00:00:00.000 -0800'
)) s;


CREATE OR REPLACE TEMP TABLE _discount_delta AS
SELECT
    s.data_source_id,
s.meta_company_id,

        s.meta_original_discount_id,
    s.effective_start_datetime,
    COALESCE(dateadd(MILLISECOND, -1, t.effective_start_datetime), '9999-12-31 00:00:00.000 -0800') AS new_effective_end_datetime
FROM _discount_updates AS s
    LEFT JOIN _discount_updates AS t
    ON s.data_source_id = t.data_source_id
    AND s.meta_company_id = t.meta_company_id
    AND     s.meta_original_discount_id = t.meta_original_discount_id
    AND S.rnk = T.rnk - 1
WHERE (S.effective_end_datetime <> dateadd(MILLISECOND, -1, T.effective_start_datetime)
    OR S.effective_end_datetime IS NULL) ;


 UPDATE lake_consolidated.ultra_merchant_history.DISCOUNT AS t
SET t.effective_end_datetime = s.new_effective_end_datetime,
    META_UPDATE_DATETIME = current_timestamp()
FROM _discount_delta AS s
WHERE s.data_source_id = t.data_source_id
    AND s.meta_company_id = t.meta_company_id
    AND     s.meta_original_discount_id = t.meta_original_discount_id
    AND s.effective_start_datetime = t.effective_start_datetime
    AND COALESCE(t.effective_end_datetime, '1900-01-01 00:00:00 -0800') <> s.new_effective_end_datetime;
