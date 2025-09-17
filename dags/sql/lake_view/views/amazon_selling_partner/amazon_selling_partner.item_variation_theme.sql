CREATE OR REPLACE VIEW lake_view.amazon_selling_partner.item_variation_theme
            (
                        asin,
                        theme,
                        attribute,
                        meta_update_datetime
            )
            AS
SELECT asin,
       theme,
       attribute,
       convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.central_amazon_selling_partner_v1.item_variation_theme;
