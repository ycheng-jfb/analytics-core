CREATE OR REPLACE VIEW data_model.fact_customer_action
AS
SELECT
    fca.customer_action_key,
    fca.customer_id,
    fca.customer_action_type_key,
    fca.order_id,
    fca.store_id,
    fca.customer_action_local_datetime,
    fca.customer_action_period_date,
    fca.event_count,
    --meta_row_hash,
    fca.meta_create_datetime,
    fca.meta_update_datetime
FROM stg.fact_customer_action AS fca
    LEFT JOIN stg.dim_store AS ds
        ON fca.store_id = ds.store_id
WHERE ds.store_brand NOT IN ('Legacy')
    AND fca.customer_id NOT IN (SELECT customer_id FROM reference.test_customer);
