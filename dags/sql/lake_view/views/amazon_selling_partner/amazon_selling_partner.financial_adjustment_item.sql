CREATE OR REPLACE VIEW lake_view.amazon_selling_partner.financial_adjustment_item
            (
                        adjustment_event_id,
                        INDEX,
                        quantity,
                        per_unit_amount_currency_code,
                        per_unit_amount_currency_amount,
                        total_amount_currency_code,
                        total_amount_currency_amount,
                        seller_sku,
                        fn_sku,
                        product_description,
                        asin,
                        meta_update_datetime
            )
            AS
SELECT adjustment_event_id,
       INDEX,
       quantity,
       per_unit_amount_currency_code,
       per_unit_amount_currency_amount,
       total_amount_currency_code,
       total_amount_currency_amount,
       seller_sku,
       fn_sku,
       product_description,
       asin,
       convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.central_amazon_selling_partner_v1.financial_adjustment_item;
