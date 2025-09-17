CREATE OR REPLACE VIEW data_model_3p.amz_dim_fulfillment_channel(
    order_fulfillment_channel_key,
    order_fulfillment_channel_name,
    meta_create_datetime,
    meta_update_datetime
)
AS
(
SELECT order_fulfillment_channel_key,
       order_fulfillment_channel_name,
       meta_create_datetime,
       meta_update_datetime
FROM dbt_edw_prod.stg.amz_dim_fulfillment_channel
);