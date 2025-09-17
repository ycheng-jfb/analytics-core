CREATE OR REPLACE VIEW lake_view.amazon_selling_partner.inventory_feature
            (
                        feature_name,
                        seller_sku,
                        fn_sku,
                        asin,
                        sku_count,
                        overlapping_skus,
                        meta_update_datetime
            )
            AS
SELECT feature_name,
       seller_sku,
       fn_sku,
       asin,
       sku_count,
       overlapping_skus,
       convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.central_amazon_selling_partner_v1.inventory_feature;
