SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
SET target_table = 'reference.po_sku_cost';

MERGE INTO stg.meta_table_dependency_watermark AS t
USING
(
    SELECT
        'reference.po_sku_cost' AS table_name,
        NULLIF(dependent_table_name,'reference.po_sku_cost') AS dependent_table_name,
        high_watermark_datetime AS new_high_watermark_datetime
    FROM(
            SELECT
                'edw_prod.reference.gsc_po_detail_dataset' AS dependent_table_name,
                max(meta_update_datetime) AS high_watermark_datetime
            FROM reference.gsc_po_detail_dataset

            UNION

            SELECT
                'reference.po_sku_cost' AS dependent_table_name,
                nvl(max(meta_update_datetime), '1900-01-01')::TIMESTAMP_LTZ AS high_watermark_datetime
            FROM reference.po_sku_cost
        ) h
) AS s
ON t.table_name = s.table_name
    AND equal_null(t.dependent_table_name, s.dependent_table_name)
WHEN MATCHED
    AND not equal_null(t.high_watermark_datetime, s.new_high_watermark_datetime)
THEN
    UPDATE
        SET t.new_high_watermark_datetime = s.new_high_watermark_datetime,
            t.meta_update_datetime = current_timestamp::timestamp_ltz(3)
WHEN NOT MATCHED
THEN
    INSERT
    (
        table_name,
        dependent_table_name,
        high_watermark_datetime,
        new_high_watermark_datetime
    )
    VALUES
    (
        s.table_name,
        s.dependent_table_name,
        '1900-01-01'::timestamp_ltz,
        s.new_high_watermark_datetime
    );

SET is_full_refresh = (SELECT IFF(stg.udf_get_watermark($target_table, NULL) = '1900-01-01'::TIMESTAMP_LTZ(3), TRUE, FALSE));
SET wm_edw_prod_reference_gsc_po_detail_dataset = stg.udf_get_watermark($target_table,'edw_prod.reference.gsc_po_detail_dataset');

CREATE OR REPLACE TEMP TABLE _po_base
(
    po_id INT
);

INSERT INTO _po_base (po_id)
SELECT DISTINCT po_id
FROM reference.gsc_po_detail_dataset
WHERE $is_full_refresh;

INSERT INTO _po_base (po_id)
SELECT DISTINCT po_id
FROM reference.gsc_po_detail_dataset
WHERE meta_update_datetime >= $wm_edw_prod_reference_gsc_po_detail_dataset
AND NOT $is_full_refresh;

CREATE OR REPLACE TEMP TABLE _po_sku_cost_stg AS
SELECT
    pdd.po_number,
    pdd.sku,
    pdd.po_line_number,
    LOWER(pdd.brand) AS brand,
    pdd.po_status_text AS po_status,
    'USD' AS currency,
    pdd.qty AS quantity,
    nvl(pdd.freight, 0) AS freight,
    nvl(pdd.duty, 0) AS duty,
    nvl(pdd.cmt, 0) AS cmt,
    nvl(pdd.cost,0) AS cost,
    nvl(pdd.landed_cost_estimated, 0) AS landed_cost, -- as per Josh input landed_cost_estimated is more accurate
    LEAST(nvl(pdd.fc_delivery, '9999-12-31'),
            nvl(pdd.xfd, '9999-12-31'),
            nvl(pdd.delivery, '9999-12-31'),
            nvl(pdd.date_launch,'9999-12-31'),
            coalesce(pdd.fc_delivery, pdd.xfd, pdd.delivery, pdd.date_launch,'1900-01-01'))::DATE AS start_date
FROM _po_base pb
JOIN reference.gsc_po_detail_dataset pdd
    ON pdd.po_id = pb.po_id
LEFT JOIN reference.po_sku_cost_history psch
    ON psch.po_number = pdd.po_number
    AND upper(psch.sku) = upper(pdd.sku)
WHERE
    psch.po_number IS NULL
    AND NOT pdd.is_deleted
    AND pdd.po_number NOT ILIKE '%CS'
    AND pdd.show_room >= '2023-09'
    AND (pdd.po_status_id IN (3,4,8,12)
        OR pdd.line_status IN ('ISSUE','INTRAN','REC'))
QUALIFY ROW_NUMBER() OVER (PARTITION BY pdd.po_number, pdd.sku, pdd.po_line_number ORDER BY pdd.qty DESC, pdd.po_id DESC) = 1;

INSERT INTO _po_sku_cost_stg(
    po_number,
    sku,
    po_line_number,
    brand,
    po_status,
    currency,
    quantity,
    freight,
    duty,
    cmt,
    cost,
    landed_cost,
    start_date
    )
SELECT
    po_number,
    sku,
    -1 AS po_line_number,
    brand,
    po_status,
    currency,
    quantity,
    freight,
    duty,
    cmt,
    cost,
    landed_cost,
    start_date
FROM reference.po_sku_cost_history
WHERE $is_full_refresh;

DELETE FROM reference.po_sku_cost
WHERE $is_full_refresh;

MERGE INTO reference.po_sku_cost t
USING (
        SELECT
            po_number,
            sku,
            po_line_number,
            brand,
            po_status,
            start_date,
            currency,
            quantity,
            freight,
            duty,
            cmt,
            cost,
            landed_cost,
            hash(*) as meta_row_hash
        FROM _po_sku_cost_stg
      ) s
    ON t.po_number = s.po_number
    AND t.sku = s.sku
    AND equal_null(t.po_line_number, s.po_line_number)
WHEN NOT MATCHED
THEN INSERT
    (
        po_number,
        sku,
        po_line_number,
        brand,
        po_status,
        start_date,
        currency,
        quantity,
        freight,
        duty,
        cmt,
        cost,
        landed_cost,
        meta_row_hash,
        meta_create_datetime,
        meta_update_datetime
    )
    VALUES
    (
        po_number,
        sku,
        po_line_number,
        brand,
        po_status,
        start_date,
        currency,
        quantity,
        freight,
        duty,
        cmt,
        cost,
        landed_cost,
        meta_row_hash,
        $execution_start_time,
        $execution_start_time
     )
WHEN MATCHED AND s.meta_row_hash != t.meta_row_hash
THEN UPDATE
    SET
        t.brand = s.brand,
        t.po_status = s.po_status,
        t.start_date = s.start_date,
        t.currency = s.currency,
        t.quantity = s.quantity,
        t.freight = s.freight,
        t.duty = s.duty,
        t.cmt = s.cmt,
        t.cost = s.cost,
        t.landed_cost = s.landed_cost,
        t.meta_row_hash = s.meta_row_hash,
        t.meta_update_datetime = $execution_start_time;

UPDATE stg.meta_table_dependency_watermark
SET
    high_watermark_datetime = IFF(
                                    dependent_table_name IS NOT NULL,
                                    new_high_watermark_datetime,
                                    (
                                        SELECT
                                            MAX(meta_update_datetime)
                                        FROM reference.po_sku_cost
                                    )
                                ),
    meta_update_datetime = current_timestamp::timestamp_ltz(3)
WHERE table_name = 'reference.po_sku_cost';
