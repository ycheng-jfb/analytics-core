CREATE OR REPLACE VIEW lake_view.amazon_selling_partner.financial_coupon_payment_event
            (
                        _fivetran_id,
                        financial_event_group_id,
                        posted_date,
                        coupon_id,
                        seller_coupon_description,
                        clip_or_redemption_count,
                        payment_event_id,
                        total_amount_currency_code,
                        total_amount_currency_amount,
                        meta_update_datetime
            )
            AS
SELECT _fivetran_id,
       financial_event_group_id,
       posted_date,
       coupon_id,
       seller_coupon_description,
       clip_or_redemption_count,
       payment_event_id,
       total_amount_currency_code,
       total_amount_currency_amount,
       convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.central_amazon_selling_partner_v1.financial_coupon_payment_event;
