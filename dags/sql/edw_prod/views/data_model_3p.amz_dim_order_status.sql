CREATE OR REPLACE VIEW data_model_3p.amz_dim_order_status(
    order_status_key,
    order_status,
    meta_create_datetime,
    meta_update_datetime
)
AS
(
SELECT order_status_key,
       order_status,
       meta_create_datetime,
       meta_update_datetime
FROM dbt_edw_prod.stg.amz_dim_order_status
);
