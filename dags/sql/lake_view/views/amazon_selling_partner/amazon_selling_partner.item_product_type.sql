CREATE OR REPLACE VIEW lake_view.amazon_selling_partner.item_product_type
            (
                        asin,
                        marketplace_id,
                        product_type,
                        meta_update_datetime
            )
            AS
SELECT asin,
       marketplace_id,
       product_type,
       convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.central_amazon_selling_partner_v1.item_product_type;
