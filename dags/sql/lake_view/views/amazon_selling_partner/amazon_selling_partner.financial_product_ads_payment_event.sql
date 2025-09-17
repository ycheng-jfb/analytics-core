CREATE OR REPLACE VIEW lake_view.amazon_selling_partner.financial_product_ads_payment_event
            (
                        _fivetran_id,
                        financial_event_group_id,
                        posted_date,
                        transaction_type,
                        invoice_id,
                        base_value_currency_code,
                        base_value_currency_amount,
                        tax_value_currency_code,
                        tax_value_currency_amount,
                        transaction_value_currency_code,
                        transaction_value_currency_amount,
                        meta_update_datetime
            )
            AS
SELECT _fivetran_id,
       financial_event_group_id,
       posted_date,
       transaction_type,
       invoice_id,
       base_value_currency_code,
       base_value_currency_amount,
       tax_value_currency_code,
       tax_value_currency_amount,
       transaction_value_currency_code,
       transaction_value_currency_amount,
       convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.central_amazon_selling_partner_v1.financial_product_ads_payment_event;
