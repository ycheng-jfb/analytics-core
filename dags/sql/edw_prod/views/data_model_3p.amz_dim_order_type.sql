CREATE OR REPLACE VIEW data_model_3p.amz_dim_order_type(
    order_type_key,
    order_type,
    meta_create_datetime,
    meta_update_datetime
)
AS
(
SELECT order_type_key,
       order_type,
       meta_create_datetime,
       meta_update_datetime
FROM dbt_edw_prod.stg.amz_dim_order_type
);
