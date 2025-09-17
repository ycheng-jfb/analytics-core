CREATE OR REPLACE VIEW lake_view.amazon_selling_partner.item_buy_box_price_type
            (
                        asin,
                        CONDITION,
                        offer_type,
                        quantity_tier,
                        quantity_discount_type,
                        landed_price_currency_code,
                        landed_price_amount,
                        listing_price_currency_code,
                        listing_price_amount,
                        shipping_price_currency_code,
                        shipping_price_amount,
                        points_points_number,
                        pointspoints_monetary_value_currency_code,
                        pointspoints_monetary_value_amount,
                        seller_id,
                        meta_update_datetime
            )
            AS
SELECT asin,
       CONDITION,
       offer_type,
       quantity_tier,
       quantity_discount_type,
       landed_price_currency_code,
       landed_price_amount,
       listing_price_currency_code,
       listing_price_amount,
       shipping_price_currency_code,
       shipping_price_amount,
       points_points_number,
       pointspoints_monetary_value_currency_code,
       pointspoints_monetary_value_amount,
       seller_id,
       convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.central_amazon_selling_partner_v1.item_buy_box_price_type;
