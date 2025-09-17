CREATE OR REPLACE VIEW data_model_3p.amz_dim_marketplace_channel(
    order_marketplace_channel_key,
    order_marketplace_channel_id,
    order_marketplace_channel_name,
    order_marketplace_channel_country,
    time_zone,
    meta_create_datetime,
    meta_update_datetime
)
AS
(
SELECT order_marketplace_channel_key,
       order_marketplace_channel_id,
       order_marketplace_channel_name,
       order_marketplace_channel_country,
       time_zone,
       meta_create_datetime,
       meta_update_datetime
FROM dbt_edw_prod.stg.amz_dim_marketplace_channel
);
