TRUNCATE TABLE validation.return_and_rma_duplicates;

SET execution_start_datetime = current_timestamp;

INSERT INTO validation.return_and_rma_duplicates
WITH dup_fact_return_line
         AS
         (SELECT rma_id,
                 SUM(CASE WHEN return_line_id != -1 THEN 1 ELSE 0 END) AS return_line_id_cnt,
                 SUM(CASE WHEN rma_product_id != -1 THEN 1 ELSE 0 END) AS rma_product_id_cnt
          FROM data_model.fact_return_line
          GROUP BY rma_id)
SELECT  rline.rma_id, rma_product_id, return_line_id,
        $execution_start_datetime AS meta_create_datetime,
        $execution_start_datetime AS meta_update_datetime
FROM data_model.fact_return_line rline
        JOIN dup_fact_return_line dup
                    ON rline.rma_id = dup.rma_id
WHERE return_line_id_cnt > 0
  AND rma_product_id_cnt > 0;