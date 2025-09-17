CREATE OR REPLACE VIEW lake_view.amazon_selling_partner.item_image
            (
                        asin,
                        marketplace_id,
                        link,
                        VARIANT,
                        height,
                        width,
                        meta_update_datetime
            )
            AS
SELECT asin,
       marketplace_id,
       link,
       VARIANT,
       height,
       width,
       convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.central_amazon_selling_partner_v1.item_image;
