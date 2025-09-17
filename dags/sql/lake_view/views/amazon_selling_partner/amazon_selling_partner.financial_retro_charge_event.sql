CREATE OR REPLACE VIEW lake_view.amazon_selling_partner.financial_retro_charge_event
            (
                        _fivetran_id,
                        retrocharge_event_type,
                        amazon_order_id,
                        financial_event_group_id,
                        posted_date,
                        base_tax_currency_code,
                        base_tax_currency_amount,
                        shipping_tax_currency_code,
                        shipping_tax_currency_amount,
                        marketplace_name,
                        meta_update_datetime
            )
            AS
SELECT _fivetran_id,
       retrocharge_event_type,
       amazon_order_id,
       financial_event_group_id,
       posted_date,
       base_tax_currency_code,
       base_tax_currency_amount,
       shipping_tax_currency_code,
       shipping_tax_currency_amount,
       marketplace_name,
       convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.central_amazon_selling_partner_v1.financial_retro_charge_event;
