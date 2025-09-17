CREATE OR REPLACE TABLE stg.dim_promo_history_init AS
SELECT
    promo_id,
    promo_name,
    promo_code,
    promo_description,
    promo_type,
    promo_cms_type,
    promo_cms_description,
    promo_classification,
    discount_method,
    subtotal_discount,
    shipping_discount,
    promo_status_code,
    promo_status,
    promo_start_datetime,
    promo_end_datetime,
    meta_event_datetime
FROM stg.dim_promo_history_stg
WHERE 0 = 1; -- Create table using this technique to ensure data types match


/*
Historical data  is loaded from reference.dim_promo_history_archive.
This data is copied from archive_edw01_edw.dbo.dim_promo_history_first_time.
*/

INSERT INTO stg.dim_promo_history_init (
    promo_id,
    promo_name,
    promo_code,
    promo_description,
    promo_type,
    promo_cms_type,
    promo_cms_description,
    promo_classification,
    discount_method,
    subtotal_discount,
    shipping_discount,
    promo_status_code,
    promo_status,
    promo_start_datetime,
    promo_end_datetime,
    meta_event_datetime
    )
SELECT
    promo_id,
    promo_name,
    promo_code,
    promo_description,
    promo_type,
    promo_cms_type,
    promo_cms_description,
    promo_classification,
    discount_method,
    subtotal_discount,
    shipping_discount,
    promo_status_code,
    promo_status,
    promo_start_datetime,
    promo_end_datetime,
    meta_event_datetime
FROM reference.dim_promo_history_archive;

CREATE OR REPLACE TEMP TABLE _dim_promo_history__promo_hist AS
SELECT
    promo_id,
    label,
	code,
	description,
    promo_type_id,
    subtotal_discount_id,
    shipping_discount_id,
    statuscode,
    date_start,
    date_end,
    effective_from_datetime,
    COALESCE(DATEADD(MILLISECOND, -1, LEAD(effective_from_datetime) OVER (PARTITION BY promo_id ORDER BY effective_from_datetime)), '9999-12-31')::timestamp_ntz(3) AS effective_to_datetime
FROM (
    SELECT
        promo_id,
        label,
        code,
        description,
        promo_type_id,
        subtotal_discount_id,
        shipping_discount_id,
        statuscode,
        date_start,
        date_end,
        HASH(
            promo_id,
            label,
            code,
            description,
            promo_type_id,
            subtotal_discount_id,
            shipping_discount_id,
            statuscode,
            date_start,
            date_end
            ) AS meta_row_hash,
        datetime_modified::timestamp_ntz(3) AS effective_from_datetime,
        ROW_NUMBER() OVER (PARTITION BY promo_id, meta_row_hash ORDER BY effective_from_datetime ASC) AS row_num
    FROM lake_archive.ultra_merchant.promo
    QUALIFY row_num = 1
    ) AS src;

CREATE OR REPLACE TEMP TABLE _dim_promo_history__promo_type_hist AS
SELECT
    promo_type_id,
    label,
	cms_label,
	cms_description,
    effective_from_datetime,
    COALESCE(DATEADD(MILLISECOND, -1, LEAD(effective_from_datetime) OVER (PARTITION BY promo_type_id ORDER BY effective_from_datetime)), '9999-12-31')::timestamp_ntz(3) AS effective_to_datetime
FROM (
    SELECT
        promo_type_id,
        label,
        cms_label,
        cms_description,
        HASH(
            promo_type_id,
            label,
            cms_label,
            cms_description
            ) AS meta_row_hash,
        datetime_added::timestamp_ntz(3) AS effective_from_datetime,
        ROW_NUMBER() OVER (PARTITION BY promo_type_id, meta_row_hash ORDER BY effective_from_datetime ASC) AS row_num
    FROM lake_archive.ultra_merchant.promo_type
    QUALIFY row_num = 1
    ) AS src;

CREATE OR REPLACE TEMP TABLE _dim_promo_history__discount_hist AS
SELECT
    discount_id,
    calculation_method,
    label,
    effective_from_datetime,
    COALESCE(DATEADD(MILLISECOND, -1, LEAD(effective_from_datetime) OVER (PARTITION BY discount_id ORDER BY effective_from_datetime)), '9999-12-31')::timestamp_ntz(3) AS effective_to_datetime
FROM (
    SELECT
        discount_id,
        calculation_method,
        label,
        HASH(
            discount_id,
            calculation_method,
            label
             ) AS meta_row_hash,
        datetime_modified::timestamp_ntz(3) AS effective_from_datetime,
        ROW_NUMBER() OVER (PARTITION BY discount_id, meta_row_hash ORDER BY effective_from_datetime ASC) AS row_num
    FROM lake_archive.ultra_merchant.discount
    QUALIFY row_num = 1
    ) AS src;

-- Use current data since lake_archive does not have history for these tables
CREATE OR REPLACE TEMP TABLE _dim_promo_history__ranked_classification AS
SELECT
    p.promo_id,
    p.promo_classification
FROM (
    SELECT
        ppc.promo_id,
        pc.label AS promo_classification,
        CASE LOWER(pc.label)
            WHEN 'vip conversion' THEN 1
            WHEN 'free trial' THEN 2
            WHEN 'sales promo' THEN 3
            WHEN 'upsell' THEN 4
            ELSE 5
        END AS sort_order,
        ROW_NUMBER() OVER (PARTITION BY ppc.promo_id ORDER BY sort_order) AS rank
    FROM lake.ultra_merchant.promo_promo_classfication AS ppc
        JOIN lake.ultra_merchant.promo_classification AS pc
            ON pc.promo_classification_id = ppc.promo_classification_id
    QUALIFY rank = 1
    ) AS p;

CREATE OR REPLACE TEMP TABLE _dim_promo_history__dates AS
SELECT DATEADD(millisecond, -1, DATEADD(day, 1, effective_from_datetime)) AS effective_eod_datetime
FROM (
    SELECT DISTINCT dates.effective_from_datetime::timestamp_ntz(3) AS effective_from_datetime
    FROM (
        SELECT effective_from_datetime::date AS effective_from_datetime
        FROM _dim_promo_history__promo_hist
        UNION ALL
        SELECT effective_from_datetime::date AS effective_from_datetime
        FROM _dim_promo_history__promo_type_hist
        UNION ALL
        SELECT effective_from_datetime::date AS effective_from_datetime
        FROM _dim_promo_history__discount_hist
        ) AS dates
    WHERE dates.effective_from_datetime::timestamp_ntz(3) > COALESCE((SELECT MAX(meta_event_datetime)::timestamp_ntz(3) FROM stg.dim_promo_history_init), '1900-01-01')
    ) AS dt;

CREATE OR REPLACE TEMP TABLE _dim_promo_history__initial_load AS
SELECT
    promo_id,
    promo_name,
    promo_code,
    promo_description,
    promo_type,
    promo_cms_type,
    promo_cms_description,
    promo_classification,
    discount_method,
    subtotal_discount,
    shipping_discount,
    promo_status_code,
    promo_status,
    promo_start_datetime,
    promo_end_datetime,
    effective_datetime AS effective_from_datetime,
    COALESCE(DATEADD(MILLISECOND, -1, LEAD(effective_datetime) OVER (PARTITION BY promo_id ORDER BY effective_from_datetime)), '9999-12-31')::timestamp_ntz(3) AS effective_to_datetime
FROM (
    SELECT
        p.promo_id,
        COALESCE(p.label, 'Unknown') AS promo_name,
        COALESCE(p.code, 'Unknown') AS promo_code,
        COALESCE(p.description, 'Unknown') AS promo_description,
        COALESCE(pt.label, 'Unknown')  AS promo_type,
        COALESCE(pt.cms_label, 'Unknown') AS promo_cms_type,
        COALESCE(pt.cms_description, 'Unknown') AS promo_cms_description,
        COALESCE(rc.promo_classification, 'Unknown') AS promo_classification,
        COALESCE(dsub.calculation_method, 'Unknown') AS discount_method,
        COALESCE(dsub.label, 'Unknown') AS subtotal_discount,
        COALESCE(dshp.label, 'Unknown') AS shipping_discount,
        COALESCE(p.statuscode, -1) AS promo_status_code,
        COALESCE(sc.label, 'Unknown')  AS promo_status,
        COALESCE(p.date_start, '1900-01-01') AS promo_start_datetime,
        COALESCE(p.date_end, '1900-01-01') AS promo_end_datetime,
        HASH(
            p.promo_id,
            promo_name,
            promo_code,
            promo_description,
            promo_type,
            promo_cms_type,
            promo_cms_description,
            promo_classification,
            discount_method,
            subtotal_discount,
            shipping_discount,
            promo_status_code,
            promo_status,
            promo_start_datetime,
            promo_end_datetime
            ) AS meta_row_hash,
        GREATEST(
            IFF(dt.effective_eod_datetime::date = p.effective_from_datetime::date, p.effective_from_datetime, DATEADD(day, -1, dt.effective_eod_datetime)),
            IFF(dt.effective_eod_datetime::date = pt.effective_from_datetime::date, pt.effective_from_datetime, DATEADD(day, -1, dt.effective_eod_datetime)),
            IFF(dt.effective_eod_datetime::date = dsub.effective_from_datetime::date, dsub.effective_from_datetime, DATEADD(day, -1, dt.effective_eod_datetime)),
            IFF(dt.effective_eod_datetime::date = dshp.effective_from_datetime::date, dshp.effective_from_datetime, DATEADD(day, -1, dt.effective_eod_datetime))
            ) AS effective_datetime,
        ROW_NUMBER() OVER (PARTITION BY p.promo_id, meta_row_hash ORDER BY effective_datetime) AS row_num
    FROM _dim_promo_history__dates AS dt
        JOIN _dim_promo_history__promo_hist AS p
            ON dt.effective_eod_datetime BETWEEN p.effective_from_datetime AND p.effective_to_datetime
        LEFT JOIN _dim_promo_history__promo_type_hist AS pt
            ON pt.promo_type_id = p.promo_type_id
            AND dt.effective_eod_datetime BETWEEN pt.effective_from_datetime AND pt.effective_to_datetime
        LEFT JOIN _dim_promo_history__discount_hist AS dsub
            ON dsub.discount_id = p.subtotal_discount_id
            AND dt.effective_eod_datetime BETWEEN dsub.effective_from_datetime AND dsub.effective_to_datetime
        LEFT JOIN _dim_promo_history__discount_hist AS dshp
            ON dshp.discount_id = p.shipping_discount_id
            AND dt.effective_eod_datetime BETWEEN dshp.effective_from_datetime AND dshp.effective_to_datetime
        LEFT JOIN _dim_promo_history__ranked_classification AS rc
            ON rc.promo_id = p.promo_id
        LEFT JOIN lake.ultra_merchant.statuscode AS sc
            ON sc.statuscode = p.statuscode
    WHERE dt.effective_eod_datetime::date IN (
        p.effective_from_datetime::date,
        pt.effective_from_datetime::date,
        dsub.effective_from_datetime::date,
        dshp.effective_from_datetime::date
        )
    QUALIFY row_num = 1
    ) AS src;

INSERT INTO stg.dim_promo_history_init (
    promo_id,
    promo_name,
    promo_code,
    promo_description,
    promo_type,
    promo_cms_type,
    promo_cms_description,
    promo_classification,
    discount_method,
    subtotal_discount,
    shipping_discount,
    promo_status_code,
    promo_status,
    promo_start_datetime,
    promo_end_datetime,
    meta_event_datetime
    )
SELECT
    promo_id,
    promo_name,
    promo_code,
    promo_description,
    promo_type,
    promo_cms_type,
    promo_cms_description,
    promo_classification,
    discount_method,
    subtotal_discount,
    shipping_discount,
    promo_status_code,
    promo_status,
    promo_start_datetime,
    promo_end_datetime,
    effective_from_datetime::timestamp_ltz(3) AS meta_event_datetime
FROM _dim_promo_history__initial_load;
