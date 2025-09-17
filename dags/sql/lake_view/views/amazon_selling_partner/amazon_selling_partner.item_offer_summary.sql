CREATE OR REPLACE VIEW lake_view.amazon_selling_partner.item_offer_summary
            (
                        asin,
                        total_offer_count,
                        list_price_currency_code,
                        list_price_amount,
                        threshold_currency_code,
                        threshold_amount,
                        suggested_lower_price_plus_shipping_currency_code,
                        suggested_lower_price_plus_shipping_amount,
                        offers_available_time,
                        meta_update_datetime
            )
            AS
SELECT asin,
       total_offer_count,
       list_price_currency_code,
       list_price_amount,
       threshold_currency_code,
       threshold_amount,
       suggested_lower_price_plus_shipping_currency_code,
       suggested_lower_price_plus_shipping_amount,
       offers_available_time,
       convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.central_amazon_selling_partner_v1.item_offer_summary;
