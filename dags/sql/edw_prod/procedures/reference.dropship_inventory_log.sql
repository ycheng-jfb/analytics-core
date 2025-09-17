SET target_table = 'reference.dropship_inventory_log';
SET execution_start_time = CURRENT_TIMESTAMP;

--Only adding target table to watermarking process as other than that it depends on date scaffold
MERGE INTO stg.meta_table_dependency_watermark AS t
USING
(
    SELECT
        'reference.dropship_inventory_log' AS table_name,
        NULLIF(dependent_table_name,'edw_prod.reference.dropship_inventory_log') AS dependent_table_name,
        high_watermark_datetime AS new_high_watermark_datetime
    FROM(
            SELECT
                'edw_prod.reference.dropship_inventory_log' AS dependent_table_name,
                '1900-01-01'::TIMESTAMP_LTZ AS high_watermark_datetime
        ) h
) AS s
ON t.table_name = s.table_name
    AND equal_null(t.dependent_table_name, s.dependent_table_name)
WHEN MATCHED
    AND not equal_null(t.new_high_watermark_datetime, s.new_high_watermark_datetime)
THEN
    UPDATE
        SET t.new_high_watermark_datetime = s.new_high_watermark_datetime,
            t.meta_update_datetime = $execution_start_time
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

SET target_watermark = (SELECT stg.udf_get_watermark($target_table, null));
SET is_full_refresh = IFF(($target_watermark = '1900-01-01'), TRUE, FALSE);
SET min_date = IFF($is_full_refresh, (SELECT MIN(datetime_added::DATE) AS min_date
                                          FROM lake_jfb.ultra_partner.fulfillment_partner_item_warehouse_log),
                       (SELECT COALESCE(MAX(date), CURRENT_DATE) AS min_date
                        FROM reference.dropship_inventory_log));


--- collecting all the dates to create miracle mile inventory hourly log table
CREATE OR REPLACE TEMP TABLE _dates AS
SELECT d.full_date AS date
FROM stg.dim_date d
WHERE d.full_date >= $min_date
  AND d.full_date <= CURRENT_DATE
ORDER BY d.full_date;

--- Extracting next datetime added to create a rollup
CREATE OR REPLACE TEMP TABLE _all_inventory_data AS
SELECT iw.item_id,
       iw.partner_item_number,
       iw.warehouse_id,
       iw.warehouse_available_quantity,
       iw.datetime_added,
       LEAD(iw.datetime_added)
            OVER (PARTITION BY iw.item_id, iw.warehouse_id ORDER BY iw.datetime_added) AS next_log_datetime
FROM lake_jfb.ultra_partner.fulfillment_partner_item_warehouse_log AS iw
         JOIN lake_consolidated.ultra_merchant.item AS i
              ON i.meta_original_item_id = iw.item_id
                  AND i.partner_item_number IS NOT NULL;

--- Joining dates with inventory data
CREATE OR REPLACE TEMP TABLE _inventory_log_by_day AS
SELECT item_id,
       partner_item_number,
       warehouse_id,
       d.date,
       warehouse_available_quantity,
       datetime_added,
       next_log_datetime,
       HASH(item_id, partner_item_number, warehouse_id, date, warehouse_available_quantity, datetime_added,
            next_log_datetime) AS meta_row_hash,
       $execution_start_time   AS meta_create_datetime,
       $execution_start_time   AS meta_update_datetime
FROM _dates d
         JOIN _all_inventory_data log
              ON d.date >= datetime_added::DATE AND d.date <= COALESCE(next_log_datetime::DATE, '9999-12-31')
QUALIFY ROW_NUMBER() OVER (PARTITION BY item_id, warehouse_id,date ORDER BY datetime_added DESC) = 1;


MERGE INTO reference.dropship_inventory_log t
    USING _inventory_log_by_day s
    ON EQUAL_NULL(t.item_id, s.item_id)
        AND EQUAL_NULL(t.warehouse_id, s.warehouse_id)
        AND EQUAL_NULL(t.date, s.date)
    WHEN NOT MATCHED THEN INSERT (
                                  item_id,
                                  partner_item_number,
                                  warehouse_id,
                                  date,
                                  warehouse_available_quantity,
                                  datetime_added,
                                  next_log_datetime,
                                  meta_row_hash, meta_create_datetime, meta_update_datetime
        )
        VALUES (item_id,
                partner_item_number,
                warehouse_id,
                date,
                warehouse_available_quantity,
                datetime_added,
                next_log_datetime,
                meta_row_hash, meta_create_datetime, meta_update_datetime)
    WHEN MATCHED AND t.meta_row_hash != s.meta_row_hash
        THEN
        UPDATE SET
            t.partner_item_number = s.partner_item_number,
            t.warehouse_available_quantity = s.warehouse_available_quantity,
            t.datetime_added = s.datetime_added,
            t.next_log_datetime = s.next_log_datetime,
            t.meta_row_hash = s.meta_row_hash,
            t.meta_update_datetime = s.meta_update_datetime;

UPDATE stg.meta_table_dependency_watermark
SET
    high_watermark_datetime = (
                                        SELECT
                                            MAX(meta_update_datetime)
                                        FROM reference.dropship_inventory_log
                                    )
                                ,
    meta_update_datetime = $execution_start_time
WHERE table_name = 'reference.dropship_inventory_log';
