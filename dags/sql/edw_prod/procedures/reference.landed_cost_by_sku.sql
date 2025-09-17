SET target_table = 'reference.landed_cost_by_sku';
SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);

/*
-- Initial load
UPDATE stg.meta_table_dependency_watermark
SET high_watermark_datetime = '1900-01-01',
    meta_update_datetime = current_timestamp()
WHERE table_name = $target_table;
*/

MERGE INTO stg.meta_table_dependency_watermark AS t
USING (
    SELECT
        $target_table AS table_name,
        NULLIF(dependent_table_name, $target_table) AS dependent_table_name,
        high_watermark_datetime AS new_high_watermark_datetime
    FROM (
        SELECT /* For self table */
            'reference.landed_cost_by_sku' AS dependent_table_name,
            NVL(MAX(meta_update_datetime), '1900-01-01')::TIMESTAMP_LTZ(3) AS high_watermark_datetime
        FROM reference.landed_cost_by_sku
        UNION
        SELECT
            'lake.oracle_ebs.landed_cost' AS dependent_table_name,
            NVL(MAX(meta_update_datetime), '1900-01-01')::TIMESTAMP_LTZ(3) AS high_watermark_datetime
        FROM lake.oracle_ebs.landed_cost
        ) h
    ) AS s
    ON s.table_name = t.table_name
    AND EQUAL_NULL(s.dependent_table_name, t.dependent_table_name)
WHEN NOT MATCHED THEN
    INSERT (
        table_name,
        dependent_table_name,
        high_watermark_datetime,
        new_high_watermark_datetime
        )
    VALUES (
        s.table_name,
        s.dependent_table_name,
        '1900-01-01'::TIMESTAMP_LTZ(3),
        s.new_high_watermark_datetime
        )
WHEN MATCHED AND NOT EQUAL_NULL(t.new_high_watermark_datetime, s.new_high_watermark_datetime) THEN
    UPDATE
    SET t.new_high_watermark_datetime = s.new_high_watermark_datetime,
        t.meta_update_datetime = $execution_start_time;

CREATE OR REPLACE TEMP TABLE _costed_date (costed_date DATE);

INSERT INTO _costed_date (costed_date)
SELECT DISTINCT lc.costed_date
FROM lake.oracle_ebs.landed_cost AS lc
--WHERE item_number = 'UD2146187-4301-57030' /* For testing */
WHERE lc.meta_update_datetime >= stg.udf_get_watermark($target_table,'lake.oracle_ebs.landed_cost')
ORDER BY lc.costed_date;

-- Retrieve new source data (rnk = 2)
CREATE OR REPLACE TEMP TABLE _cogs_base AS
SELECT
    l.store_id,
    l.store_region,
    l.store_brand,
    l.sku,
    l.costed_date,
    l.currency_code,
    l.unit_cost,
    2 AS rnk,
    ROW_NUMBER() OVER (PARTITION BY l.store_id, l.sku ORDER BY l.costed_date) AS first_row
FROM (
    SELECT
        t.store_id,
        t.store_region,
        t.store_brand,
        t.sku,
        t.costed_date,
        t.currency_code,
        t.unit_cost,
        LEAD(t.unit_cost) OVER (PARTITION BY t.store_id, t.sku ORDER BY t.costed_date DESC) AS lead_unit_cost
    FROM (
        SELECT
            NVL(lc.store_id, o.store_id) AS store_id,
            ds.store_region,
            ds.store_brand AS store_brand,
            TRIM(lc.item_number) AS sku,
            lc.costed_date,
            UPPER(NVL(ds.store_currency, lc.currency_code)) AS currency_code,
            CAST(AVG(CASE
                WHEN UPPER(NVL(ds.store_currency, lc.currency_code)) = UPPER(lc.currency_code) THEN lc.unit_cost
                ELSE lc.unit_cost * COALESCE(cer.exchange_rate, 1)
                END) AS NUMBER(18, 4)) AS unit_cost
        FROM lake.oracle_ebs.landed_cost AS lc
            JOIN _costed_date AS dt
                ON dt.costed_date = lc.costed_date
            LEFT JOIN lake_consolidated_view.ultra_merchant."ORDER" AS o /* use view to eliminate deletes */
                ON o.order_id = lc.sales_order
            LEFT JOIN stg.dim_store AS ds
                ON ds.store_id = NVL(lc.store_id, o.store_id)
            LEFT JOIN reference.currency_exchange_rate_by_date AS cer
                ON cer.rate_date_pst = lc.transaction_date::DATE
                AND UPPER(cer.src_currency) = UPPER(lc.currency_code)
                AND UPPER(cer.dest_currency) = UPPER(ds.store_currency)
        WHERE NVL(lc.store_id, o.store_id) IS NOT NULL
            AND lc.transaction_type_name IN ('TS Customer Shipment', 'TS Retail Shipment/Sale')
        GROUP BY
            NVL(lc.store_id, o.store_id),
            ds.store_region,
            ds.store_brand,
            TRIM(lc.item_number),
            lc.costed_date,
            UPPER(NVL(ds.store_currency, lc.currency_code))
        ) AS t
    ) AS l
WHERE (l.unit_cost != l.lead_unit_cost OR l.lead_unit_cost IS NULL);

-- Retrieve existing overlapping data (rnk = 1)
INSERT INTO _cogs_base
SELECT
    cb.store_id,
    lcbs.store_region,
    lcbs.store_brand,
    cb.sku,
    lcbs.start_date AS costed_date,
    lcbs.currency_code,
    lcbs.unit_cost,
    1 AS rnk,
    -1 AS first_row
FROM reference.landed_cost_by_sku AS lcbs
    JOIN _cogs_base AS cb
        ON cb.store_id = lcbs.store_id
        AND cb.sku = lcbs.sku;

-- Update first row's start date to 2018-04-01 if there is no existing data
UPDATE _cogs_base AS cb
SET cb.costed_date = '2018-04-01'
WHERE cb.first_row = 1
    AND NOT EXISTS (
        SELECT 1
        FROM reference.landed_cost_by_sku AS lcbs
        WHERE lcbs.store_id = cb.store_id
          AND lcbs.sku = cb.sku
        );

CREATE OR REPLACE TEMP TABLE _cogs_stg AS
SELECT
    l.store_id,
    l.store_region,
    l.store_brand,
    l.sku,
    l.currency_code,
    l.unit_cost,
    COALESCE(l.unit_cost = l.lead_unit_cost, FALSE)::BOOLEAN AS is_duplicate,
    l.costed_date AS start_date,
    NVL(DATEADD(dd, -1, LEAD(l.costed_date) OVER (PARTITION BY l.store_id, l.sku, is_duplicate ORDER BY l.costed_date)), '9999-12-31') AS end_date
FROM (
    SELECT
        cb.store_id,
        cb.store_region,
        cb.store_brand,
        cb.sku,
        cb.costed_date,
        cb.currency_code,
        cb.unit_cost,
        LEAD(cb.unit_cost) OVER (PARTITION BY cb.store_id, cb.sku ORDER BY cb.costed_date DESC) AS lead_unit_cost
    FROM (
        SELECT
            store_id,
            store_region,
            store_brand,
            sku,
            currency_code,
            costed_date,
            unit_cost
        FROM _cogs_base
        QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id, sku, costed_date ORDER BY rnk) = 1
        ) AS cb
    ) AS l;

-- Merge the data (insert/update/delete)
MERGE INTO reference.landed_cost_by_sku AS t
USING _cogs_stg AS s
    ON t.store_id = s.store_id
    AND t.sku = s.sku
    AND t.start_date = s.start_date
WHEN NOT MATCHED AND NOT s.is_duplicate THEN
    INSERT (
        store_id,
        store_region,
        store_brand,
        sku,
        currency_code,
        unit_cost,
        start_date,
        end_date,
        meta_create_datetime,
        meta_update_datetime
        )
    VALUES (
        s.store_id,
        s.store_region,
        s.store_brand,
        s.sku,
        s.currency_code,
        s.unit_cost,
        s.start_date,
        s.end_date,
        $execution_start_time,
        $execution_start_time
        )
WHEN MATCHED AND NOT s.is_duplicate
    AND (NOT EQUAL_NULL(t.unit_cost, s.unit_cost)
        OR NOT EQUAL_NULL(t.end_date, s.end_date)
        OR NOT EQUAL_NULL(t.currency_code, s.currency_code)
        OR NOT EQUAL_NULL(t.store_region, s.store_region)
        OR NOT EQUAL_NULL(t.store_brand, s.store_brand)
        ) THEN UPDATE
    SET
        t.store_id = s.store_id,
        t.store_region = s.store_region,
        t.store_brand = s.store_brand,
        t.sku = s.sku,
        t.currency_code = s.currency_code,
        t.unit_cost = s.unit_cost,
        t.start_date = s.start_date,
        t.end_date = s.end_date,
        t.meta_update_datetime = $execution_start_time
WHEN MATCHED AND s.is_duplicate THEN DELETE;

--  Success
UPDATE stg.meta_table_dependency_watermark
SET high_watermark_datetime = IFF(dependent_table_name IS NOT NULL, new_high_watermark_datetime,
        (SELECT MAX(meta_update_datetime) AS new_high_watermark_datetime FROM reference.landed_cost_by_sku)),
    meta_update_datetime = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3)
WHERE table_name = $target_table;
