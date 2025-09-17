CREATE OR REPLACE VIEW lake_view.amazon_selling_partner.listed_item
            (
                        sku,
                        item_procurement_currency_code,
                        item_procurement_amount,
                        meta_update_datetime
            )
            AS
SELECT sku,
       item_procurement_currency_code,
       item_procurement_amount,
       convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.central_amazon_selling_partner_v1.listed_item;
