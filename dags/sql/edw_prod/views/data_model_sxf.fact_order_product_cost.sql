CREATE OR REPLACE VIEW data_model_sxf.fact_order_product_cost AS
SELECT
    stg.udf_unconcat_brand(fopc.order_id) AS order_id,
    fopc.store_id,
    fopc.currency_key,
    fopc.oracle_cost_local_amount,
    fopc.lpn_po_cost_local_amount,
    fopc.po_cost_local_amount,
    fopc.estimated_landed_cost_local_amount,
    fopc.reporting_landed_cost_local_amount,
    fopc.is_actual_landed_cost,
    fopc.meta_create_datetime,
    fopc.meta_update_datetime
FROM stg.fact_order_product_cost AS fopc
    LEFT JOIN stg.dim_store AS ds
        ON fopc.store_id = ds.store_id
    WHERE ds.store_brand NOT IN ('Legacy')
        AND (substring(fopc.order_id, -2) = '30' OR fopc.order_id = -1);
