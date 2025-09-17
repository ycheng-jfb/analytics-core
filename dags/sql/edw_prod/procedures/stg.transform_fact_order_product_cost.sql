SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
SET target_table = 'stg.fact_order_product_cost';
SET is_full_refresh = (SELECT IFF(stg.udf_get_watermark($target_table, NULL) = '1900-01-01'::TIMESTAMP_LTZ(3), TRUE, FALSE));


SET wm_edw_stg_fact_order_line = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.fact_order_line'));
SET wm_edw_stg_fact_order = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.fact_order'));
SET wm_edw_stg_fact_order_line_product_cost = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.fact_order_line_product_cost'));


CREATE OR REPLACE TEMP TABLE _fact_order_product_cost__base(order_id INT, meta_original_order_id INT);

INSERT INTO _fact_order_product_cost__base (order_id, meta_original_order_id)
SELECT order_id, meta_original_order_id
FROM stg.fact_order
WHERE $is_full_refresh = TRUE
ORDER BY order_id;


INSERT INTO _fact_order_product_cost__base (order_id)
SELECT DISTINCT order_id
FROM (
      SELECT fol.order_id
      FROM stg.fact_order_line fol
      WHERE fol.meta_update_datetime > $wm_edw_stg_fact_order_line

      UNION ALL

      SELECT fo.order_id
      FROM stg.fact_order fo
      WHERE fo.meta_update_datetime > $wm_edw_stg_fact_order

      UNION ALL

      SELECT fol.order_id
      FROM stg.fact_order_line_product_cost fol
      WHERE fol.meta_update_datetime > $wm_edw_stg_fact_order_line_product_cost

      UNION ALL

      SELECT e.order_id
      FROM excp.fact_order_product_cost e
      WHERE e.meta_is_current_excp
        AND e.meta_data_quality = 'error'
      ) incr
WHERE $is_full_refresh = FALSE
ORDER BY incr.order_id;

UPDATE _fact_order_product_cost__base fopcb
SET fopcb.meta_original_order_id = fo.meta_original_order_id
FROM stg.fact_order fo
WHERE fopcb.order_id = fo.order_id;

CREATE OR REPLACE TEMP TABLE _fact_order_product_cost__stg AS
SELECT fol.order_id,
       o.meta_original_order_id,
       NVL(fol.store_id, -1)                                 AS store_id,
       NVL(fol.currency_key, -1)                             AS currency_key,
       SUM(NVL(fol.estimated_landed_cost_local_amount, 0))   AS estimated_landed_cost_local_amount,
       SUM(NVL(folpc.reporting_landed_cost_local_amount, 0)) AS reporting_landed_cost_local_amount,
       SUM(fol.oracle_cost_local_amount)                     AS oracle_cost_local_amount,
       SUM(fol.lpn_po_cost_local_amount)                     AS lpn_po_cost_local_amount,
       SUM(fol.po_cost_local_amount)                         AS po_cost_local_amount,
       SUM(NVL(fol.misc_cogs_local_amount, 0))               AS misc_cogs_local_amount,
       MIN(NVL(folpc.is_actual_landed_cost, 0))              AS is_actual_landed_cost
FROM _fact_order_product_cost__base o
         JOIN stg.fact_order_line fol
              ON o.order_id = fol.order_id
         LEFT JOIN stg.fact_order_line_product_cost folpc
                   ON folpc.order_line_id = fol.order_line_id
         JOIN stg.dim_order_line_status dols
              ON fol.order_line_status_key = dols.order_line_status_key
                  AND dols.order_line_status != 'Cancelled'
         LEFT JOIN stg.dim_product_type dpt
                   ON dpt.product_type_key = fol.product_type_key
WHERE NVL(dpt.is_free, 0) = 0
   OR LOWER(dpt.product_type_name) = 'membership reward points item'
GROUP BY fol.order_id,
         o.meta_original_order_id,
         fol.store_id,
         fol.currency_key;


-- Update reporting_landed_cost_local_amount,is_actual_landed_cost, estimated_landed_cost and misc_cogs_local_amount in fact_order
UPDATE stg.fact_order f
SET f.estimated_landed_cost_local_amount = s.estimated_landed_cost_local_amount,
    f.misc_cogs_local_amount             = s.misc_cogs_local_amount,
    f.reporting_landed_cost_local_amount = s.reporting_landed_cost_local_amount,
    f.is_actual_landed_cost              = s.is_actual_landed_cost
FROM _fact_order_product_cost__stg s
WHERE f.order_id = s.order_id
  AND (NOT EQUAL_NULL(f.estimated_landed_cost_local_amount, s.estimated_landed_cost_local_amount)
    OR NOT EQUAL_NULL(f.misc_cogs_local_amount, s.misc_cogs_local_amount)
    OR NOT EQUAL_NULL(f.reporting_landed_cost_local_amount, s.reporting_landed_cost_local_amount)
    OR NOT EQUAL_NULL(f.is_actual_landed_cost, s.is_actual_landed_cost));


INSERT INTO stg.fact_order_product_cost_stg
(
     order_id,
     meta_original_order_id,
     store_id,
     currency_key,
     estimated_landed_cost_local_amount,
     reporting_landed_cost_local_amount,
     is_actual_landed_cost,
     oracle_cost_local_amount,
     lpn_po_cost_local_amount,
     po_cost_local_amount,
     meta_create_datetime,
     meta_update_datetime,
     misc_cogs_local_amount
)
SELECT o.order_id,
       o.meta_original_order_id,
       COALESCE(pcs.store_id, -1)                                                                 AS store_id,
       COALESCE(pcs.currency_key, -1)                                                             AS currency_key,
       SUM(NVL(pcs.estimated_landed_cost_local_amount, 0))                                        AS estimated_landed_cost_local_amount,
       SUM(NVL(pcs.reporting_landed_cost_local_amount, 0))                                        AS reporting_landed_cost_local_amount,
       MIN(NVL(pcs.is_actual_landed_cost, 0))                                                     AS is_actual_landed_cost,
       SUM(IFF(pcs.oracle_cost_local_amount IS NULL, NULL, NVL(pcs.oracle_cost_local_amount, 0))) AS oracle_cost_local_amount,
       SUM(IFF(pcs.lpn_po_cost_local_amount IS NULL, NULL, NVL(pcs.lpn_po_cost_local_amount, 0))) AS lpn_po_cost_local_amount,
       SUM(IFF(pcs.po_cost_local_amount IS NULL, NULL,NVL(pcs.po_cost_local_amount, 0)))          AS po_cost_local_amount,
       $execution_start_time                                                                      AS meta_create_datetime,
       $execution_start_time                                                                      AS meta_update_datetime,
       SUM(NVL(pcs.misc_cogs_local_amount, 0))                                                    AS misc_cogs_local_amount
FROM _fact_order_product_cost__base o
         LEFT JOIN _fact_order_product_cost__stg pcs
                   ON o.order_id = pcs.order_id
GROUP BY o.order_id,
         o.meta_original_order_id,
         COALESCE(pcs.store_id, -1),
         COALESCE(pcs.currency_key, -1)
ORDER BY o.order_id;
