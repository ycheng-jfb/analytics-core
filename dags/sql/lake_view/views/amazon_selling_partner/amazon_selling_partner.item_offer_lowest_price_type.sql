CREATE OR REPLACE VIEW lake_view.amazon_selling_partner.item_offer_lowest_price_type
            (
                        asin,
                        CONDITION,
                        fulfillment_channel,
                        offer_type,
                        quantity_tier,
                        quantity_discount_type,
                        landed_price_currency_code,
                        landed_price_amount,
                        listing_price_currency_code,
                        listing_price_amount,
                        shipping_currency_code,
                        shipping_amount,
                        points_points_number,
                        pointspoints_monetary_value_currency_code,
                        pointspoints_monetary_value_amount,
                        meta_update_datetime
            )
            AS
SELECT asin,
       CONDITION,
       fulfillment_channel,
       offer_type,
       quantity_tier,
       quantity_discount_type,
       landed_price_currency_code,
       landed_price_amount,
       listing_price_currency_code,
       listing_price_amount,
       shipping_currency_code,
       shipping_amount,
       points_points_number,
       pointspoints_monetary_value_currency_code,
       pointspoints_monetary_value_amount,
       convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.central_amazon_selling_partner_v1.item_offer_lowest_price_type;
