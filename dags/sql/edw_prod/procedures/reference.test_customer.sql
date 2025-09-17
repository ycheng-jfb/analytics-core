SET target_table = 'reference.test_customer';
SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
SET is_full_refresh = (SELECT IFF(stg.udf_get_watermark($target_table, NULL) = '1900-01-01'::TIMESTAMP_LTZ(3), TRUE, FALSE));
ALTER SESSION SET QUERY_TAG = $target_table;

/*
-- Initial Load / Full Refresh
UPDATE stg.meta_table_dependency_watermark
SET high_watermark_datetime = '1900-01-01',
    meta_update_datetime = CURRENT_TIMESTAMP()
WHERE table_name = $target_table;
SET is_full_refresh = TRUE;
*/

MERGE INTO stg.meta_table_dependency_watermark AS w
USING (
    SELECT
        $target_table AS table_name,
        t.dependent_table_name,
        IFF(t.dependent_table_name IS NOT NULL, t.new_high_watermark_datetime, (
            SELECT MAX(meta_update_datetime) AS new_high_watermark_datetime
            FROM reference.test_customer
        )) AS new_high_watermark_datetime
    FROM (
        SELECT /* For self table */
            NULL AS dependent_table_name,
            NULL AS new_high_watermark_datetime
        UNION ALL
        SELECT
            'lake_consolidated.ultra_merchant.customer' AS dependent_table_name,
            MAX(meta_update_datetime) AS new_high_watermark_datetime
        FROM lake_consolidated.ultra_merchant.customer
        UNION ALL
         SELECT
            'lake_consolidated.ultra_merchant_history.customer' AS dependent_table_name,
            MAX(meta_update_datetime) AS new_high_watermark_datetime
        FROM lake_consolidated.ultra_merchant_history.customer
        ) AS t
    ORDER BY COALESCE(t.dependent_table_name, '')
    ) AS s
    ON w.table_name = s.table_name
    AND w.dependent_table_name IS NOT DISTINCT FROM s.dependent_table_name
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
        '1900-01-01', /* current high_watermark_datetime */
        s.new_high_watermark_datetime
        )
WHEN MATCHED AND w.new_high_watermark_datetime IS DISTINCT FROM s.new_high_watermark_datetime THEN
    UPDATE
    SET w.new_high_watermark_datetime = s.new_high_watermark_datetime,
        w.meta_update_datetime = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);

-- Use watermark variables for each dependent table to allow pruning of micro-partitions which doesn't happen with UDFs.
SET wm_self = (SELECT stg.udf_get_watermark($target_table, NULL));
SET wm_lake_ultra_merchant_customer = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.customer'));
SET wm_lake_ultra_merchant_history_customer = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant_history.customer'));

/*
SELECT
    $wm_self,
    $wm_lake_ultra_merchant_customer;
*/

-- Customer Base Table
CREATE OR REPLACE TEMP TABLE _test_customer__customer_base (customer_id INT);

-- Full Refresh
INSERT INTO _test_customer__customer_base (customer_id)
SELECT c.customer_id
FROM lake_consolidated.ultra_merchant.customer AS c
WHERE $is_full_refresh = TRUE
ORDER BY customer_id;


-- Incremental Refresh
INSERT INTO _test_customer__customer_base (customer_id)
SELECT DISTINCT incr.customer_id
FROM (
    /* updated rows from self table */
    SELECT customer_id
    FROM reference.test_customer
    WHERE meta_update_datetime > $wm_self
    UNION ALL
    /* new rows */
    SELECT customer_id
    FROM lake_consolidated.ultra_merchant.customer
    WHERE meta_update_datetime > $wm_lake_ultra_merchant_customer
    UNION ALL
    SELECT customer_id
    FROM lake_consolidated.ultra_merchant_history.customer
    WHERE meta_update_datetime > $wm_lake_ultra_merchant_history_customer
    ) AS incr
WHERE NOT $is_full_refresh
ORDER BY incr.customer_id;
-- SELECT COUNT(1) FROM _test_customer__customer_base;

CREATE OR REPLACE TEMP TABLE _test_customer__stg AS
SELECT DISTINCT customer_id,
                meta_original_customer_id
FROM (SELECT c.customer_id,
             c.meta_original_customer_id,
             c.email
      FROM _test_customer__customer_base AS base
               JOIN lake_consolidated.ultra_merchant.customer AS c
                    ON c.customer_id = base.customer_id
      UNION ALL
      SELECT ch.customer_id,
             ch.meta_original_customer_id,
             ch.email
      FROM _test_customer__customer_base AS base
               JOIN lake_consolidated.ultra_merchant_history.customer AS ch
                    ON ch.customer_id = base.customer_id)
WHERE email ILIKE ANY ('%%@test.com%%',
                       '%%@example.com%%',
                       '%%@testing.com%%',
                       '%%@fkqa.com%%',
                       '%%hastoo@mail.ru%%',
                       '%%zoloto0888@mail.ru%%',
                       '%%jessicaanakotta@yahoo.com%%',
                       '%%test.co.uk',
                       '%%@test.co.uk%%',
                       '%%test.de',
                       '%%@test.de%%',
                       '%%test.es',
                       '%%@test.es%%',
                       '%%test.fr',
                       '%%@test.fr%%',
                       '%%test.us',
                       '%%@test.us%%',
                       '%%micummings@techstyle.com%%',
                       '%%jia@justfab.com%%',
                       '%%mamunoz@justfab.com%%',
                       '%%lars@justfab.com%%',
                       '%%test%@email%%')
ORDER BY customer_id;
-- SELECT * FROM _test_customer__stg;
-- SELECT customer_id FROM _test_customer__stg GROUP BY 1 HAVING COUNT(1) > 1;



BEGIN TRANSACTION;

-- Remove orphan records
DELETE FROM reference.test_customer AS src
USING _test_customer__customer_base AS base
WHERE base.customer_id = src.customer_id
    AND NOT EXISTS ( /* Compare using all the fields that make the table row unique */
        SELECT TRUE AS is_exists
        FROM _test_customer__stg AS stg
        WHERE stg.customer_id = src.customer_id
        );
-- FINAL OUTPUT - Merge into reference.test_customer table
MERGE INTO reference.test_customer AS t
USING (
    SELECT
        customer_id,
        meta_original_customer_id,
        HASH (
            customer_id,
            meta_original_customer_id
            ) AS meta_row_hash,
        $execution_start_time AS meta_create_datetime,
        $execution_start_time AS meta_update_datetime,
        ROW_NUMBER() OVER (ORDER BY customer_id) AS sort_order
    FROM _test_customer__stg
    ) AS s
    ON s.customer_id = t.customer_id
WHEN NOT MATCHED THEN
    INSERT (
        customer_id,
        meta_original_customer_id,
        meta_row_hash,
        meta_create_datetime,
        meta_update_datetime
        )
    VALUES (
        customer_id,
        meta_original_customer_id,
        meta_row_hash,
        meta_create_datetime,
        meta_update_datetime
        )
WHEN MATCHED AND t.meta_row_hash != s.meta_row_hash THEN
    UPDATE
    SET
--      t.customer_id = s.customer_id,
        t.meta_original_customer_id = s.meta_original_customer_id,
        t.meta_row_hash = s.meta_row_hash,
--      t.meta_create_datetime = s.meta_create_datetime,
        t.meta_update_datetime = s.meta_update_datetime;
-- SELECT COUNT(1) FROM reference.test_customer;

COMMIT;

--  Success
UPDATE stg.meta_table_dependency_watermark
SET high_watermark_datetime = IFF(dependent_table_name IS NOT NULL, new_high_watermark_datetime,
        (SELECT MAX(meta_update_datetime) AS new_high_watermark_datetime FROM reference.test_customer)),
    meta_update_datetime = CURRENT_TIMESTAMP()
WHERE table_name = $target_table;
-- SELECT * FROM stg.meta_table_dependency_watermark WHERE table_name = $target_table;
