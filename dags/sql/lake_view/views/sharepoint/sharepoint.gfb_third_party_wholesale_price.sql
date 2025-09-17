CREATE OR REPLACE VIEW lake_view.sharepoint.gfb_third_party_wholesale_price
        (
         product_sku,
         color,
         style,
         skc,
         sub_brand,
         wholesale_price,
        meta_update_datetime
            )
AS
SELECT product_sku,
       color,
       style,
       skc,
       sub_brand,
       wholesale_price,
       CONVERT_TIMEZONE('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.jfb_other_sharepoint_v1.gfb_thirdparty_wholesale_price_data;

