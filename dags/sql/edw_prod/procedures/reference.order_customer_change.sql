SET target_table = 'reference.order_customer_change';
SET execution_start_time = CURRENT_TIMESTAMP :: TIMESTAMP_LTZ(3);
SET is_full_refresh = (SELECT IFF(stg.udf_get_watermark($target_table, NULL) = '1900-01-01'::TIMESTAMP_LTZ(3), TRUE, FALSE));

MERGE INTO stg.meta_table_dependency_watermark AS w
    USING (
        SELECT $target_table                                                   AS table_name,
               NULLIF(dependent_table_name, 'reference.order_customer_change') AS dependent_table_name,
               high_watermark_datetime                                         AS new_high_watermark_datetime
        FROM (
                 SELECT -- For self table
                        'reference.order_customer_change'                      AS dependent_table_name,
                        '1900-01-01'                                           AS high_watermark_datetime
                 UNION ALL

                 SELECT 'lake_consolidated.ultra_merchant_history."ORDER"'     AS dependent_table_name,
                        MAX(meta_update_datetime)                              AS high_watermark_datetime
                 FROM lake_consolidated.ultra_merchant_history."ORDER"

                 UNION ALL

                 SELECT 'lake_consolidated.ultra_merchant.gift_order'          AS dependent_table_name,
                        MAX(meta_update_datetime)                              AS high_watermark_datetime
                 FROM lake_consolidated.ultra_merchant.gift_order
             ) AS t
        ORDER BY COALESCE(t.dependent_table_name, '')
    ) AS s
    ON w.table_name = s.table_name
        AND EQUAL_NULL(w.dependent_table_name, s.dependent_table_name)
    WHEN NOT MATCHED THEN
        INSERT (
                table_name,
                dependent_table_name,
                high_watermark_datetime,
                new_high_watermark_datetime
            )
            VALUES (s.table_name,
                    s.dependent_table_name,
                    '1900-01-01', -- current high_watermark_datetime
                    s.new_high_watermark_datetime)
    WHEN MATCHED AND NOT EQUAL_NULL(w.new_high_watermark_datetime, s.new_high_watermark_datetime) THEN
        UPDATE
            SET w.new_high_watermark_datetime = s.new_high_watermark_datetime,
                w.meta_update_datetime = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);

SET wm_lh_um_order = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant_history."ORDER"'));
SET wm_lake_ultra_merchant_gift_order = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.gift_order'));

CREATE OR REPLACE TEMP TABLE _order_base (order_id INT);

INSERT INTO _order_base (order_id)
SELECT order_id
FROM lake_consolidated.ultra_merchant."ORDER"
WHERE $is_full_refresh = TRUE
ORDER BY order_id;

INSERT INTO _order_base (order_id)
SELECT DISTINCT order_id
FROM (
      SELECT order_id
      FROM lake_consolidated.ultra_merchant_history."ORDER" AS o
      WHERE meta_update_datetime > $wm_lh_um_order
      UNION ALL
      SELECT order_id
      FROM lake_consolidated.ultra_merchant.gift_order
      WHERE meta_update_datetime > $wm_lake_ultra_merchant_gift_order
    ) AS incr
WHERE $is_full_refresh = FALSE
ORDER BY order_id;

CREATE OR REPLACE TEMP TABLE _order_base__customer_change AS
SELECT
    ob.order_id
FROM _order_base AS ob
JOIN lake_consolidated.ultra_merchant_history."ORDER" AS o
    ON o.order_id = ob.order_id
GROUP BY ob.order_id
HAVING COUNT(DISTINCT o.customer_id) > 1;

CREATE OR REPLACE TEMP TABLE _order_customer_change_sequence AS
SELECT
    order_id,
    meta_original_order_id,
    customer_id,
    effective_start_datetime,
    ROW_NUMBER()OVER(PARTITION BY order_id ORDER BY effective_start_datetime) AS rn
FROM (
         SELECT DISTINCT obcc.order_id,
                         o.meta_original_order_id,
                         o.customer_id,
                         effective_start_datetime,
                         rank() OVER (PARTITION BY o.order_id,o.customer_id ORDER BY o.effective_start_datetime) AS rn
         FROM _order_base__customer_change obcc
                  LEFT JOIN lake_consolidated.ultra_merchant_history."ORDER" AS o
                            ON o.order_id = obcc.order_id
             QUALIFY rn = 1
     );

CREATE OR REPLACE TEMP TABLE _order_customer_change__stg AS
SELECT
    occs1.order_id,
    occs1.meta_original_order_id,
    occs1.customer_id AS original_customer_id,
    occs2.customer_id AS new_customer_id,
    occs1.rn AS customer_id_change_sequence,
    FALSE AS is_gift_order
FROM _order_customer_change_sequence AS occs1
LEFT JOIN _order_customer_change_sequence AS occs2 ON occs1.order_id = occs2.order_id AND occs2.rn = occs1.rn + 1
WHERE occs2.order_id IS NOT NULL;

UPDATE _order_customer_change__stg AS t
SET t.is_gift_order = TRUE
FROM (
        SELECT ob.order_id
        FROM _order_base AS ob
            JOIN lake_consolidated.ultra_merchant.gift_order as g
                ON g.order_id = ob.order_id
     ) AS s
WHERE s.order_id = t.order_id;

MERGE INTO reference.order_customer_change AS t
USING _order_customer_change__stg AS s ON
    s.order_id = t.order_id
    AND s.customer_id_change_sequence = t.customer_id_change_sequence
    WHEN MATCHED THEN UPDATE SET
        t.meta_original_order_id = s.meta_original_order_id,
        t.original_customer_id = s.original_customer_id,
        t.new_customer_id = s.new_customer_id,
        t.is_gift_order = s.is_gift_order,
        t.meta_update_datetime = $execution_start_time
    WHEN NOT MATCHED THEN INSERT
    (
        order_id,
        meta_original_order_id,
        original_customer_id,
        new_customer_id,
        is_gift_order,
        customer_id_change_sequence,
        meta_create_datetime,
        meta_update_datetime
    )
    VALUES
    (
        order_id,
        meta_original_order_id,
        original_customer_id,
        new_customer_id,
        is_gift_order,
        customer_id_change_sequence,
        $execution_start_time,
        $execution_start_time
    );

UPDATE stg.meta_table_dependency_watermark
SET high_watermark_datetime = IFF(dependent_table_name IS NOT NULL, new_high_watermark_datetime,
        (SELECT MAX(meta_update_datetime) AS new_high_watermark_datetime FROM reference.order_date_change)),
    meta_update_datetime = CURRENT_TIMESTAMP()
WHERE table_name = $target_table;
