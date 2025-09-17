TRUNCATE TABLE reference.emarsys_history;

INSERT INTO reference.emarsys_history (meta_original_customer_id, effective_start_datetime, is_opt_out, prev_is_opt_out)
SELECT customer_id                                                                  AS meta_original_customer_id,
       effective_start_datetime,
       is_opt_out,
       LAG(is_opt_out) OVER (PARTITION BY customer_id ORDER BY effective_start_datetime) AS prev_is_opt_out
FROM stg.dim_customer_detail_history -- this should use edw_prod.
    QUALIFY CASE
                WHEN is_opt_out <> LAG(is_opt_out) OVER (PARTITION BY customer_id ORDER BY meta_event_datetime)
                    THEN TRUE
                ELSE FALSE END = TRUE;


UPDATE reference.emarsys_history eh
SET eh.customer_id = CONCAT(c.customer_id, ds.company_id)
FROM lake.ultra_merchant.customer c
JOIN lake_fl.reference.dim_store ds
ON c.store_id = ds.store_id
WHERE eh.meta_original_customer_id = c.customer_id;


-- validate
SELECT *
FROM reference.emarsys_history
WHERE customer_id IS NULL;
