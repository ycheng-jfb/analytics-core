CREATE OR REPLACE VIEW lake_view.amazon_selling_partner.financial_seller_deal_payment_event
            (
                        _fivetran_id,
                        financial_event_group_id,
                        posted_date,
                        deal_id,
                        deal_description,
                        event_type,
                        fee_type,
                        fee_amount_currency_code,
                        fee_amount_currency_amount,
                        tax_amount_currency_code,
                        tax_amount_currency_amount,
                        total_amount_currency_code,
                        total_amount_currency_amount,
                        meta_update_datetime
            )
            AS
SELECT _fivetran_id,
       financial_event_group_id,
       posted_date,
       deal_id,
       deal_description,
       event_type,
       fee_type,
       fee_amount_currency_code,
       fee_amount_currency_amount,
       tax_amount_currency_code,
       tax_amount_currency_amount,
       total_amount_currency_code,
       total_amount_currency_amount,
       convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.central_amazon_selling_partner_v1.financial_seller_deal_payment_event;
