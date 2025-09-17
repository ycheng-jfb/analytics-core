TRUNCATE TABLE reference.fact_refund_rma_history;

INSERT INTO reference.fact_refund_rma_history(refund_id, meta_original_refund_id, return_id, rma_id,
                                              refund_request_local_datetime, meta_create_datetime, meta_update_datetime)
SELECT CONCAT(fr.refund_id, ds.company_id)                                                                        AS refund_id,
       fr.refund_id                                                                                               AS meta_original_refund_id,
       CASE
           WHEN fr.return_id = -1 THEN -1
           ELSE CAST(CONCAT(CAST(fr.return_id AS INT), ds.company_id) AS INT) END                                 AS return_id,
       CASE
           WHEN fr.rma_id = -1 THEN -1
           ELSE CAST(CONCAT(CAST(fr.rma_id AS INT), ds.company_id) AS INT) END                                    AS rma_id,
       refund_request_local_datetime,
       CURRENT_TIMESTAMP                                                                                          AS meta_create_datetime,
       CURRENT_TIMESTAMP                                                                                          AS meta_update_datetime
FROM stg.fact_refund fr
         JOIN lake_consolidated.reference.dim_store ds
              ON ds.store_id = fr.store_id;
