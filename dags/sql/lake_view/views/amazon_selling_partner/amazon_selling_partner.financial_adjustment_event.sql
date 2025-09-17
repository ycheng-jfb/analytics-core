CREATE OR REPLACE VIEW lake_view.amazon_selling_partner.financial_adjustment_event
            (
                        _fivetran_id,
                        financial_event_group_id,
                        adjustment_type,
                        posted_date,
                        adjustment_amount_currency_code,
                        adjustment_amount_currency_amount,
                        meta_update_datetime
            )
            AS
SELECT _fivetran_id,
       financial_event_group_id,
       adjustment_type,
       posted_date,
       adjustment_amount_currency_code,
       adjustment_amount_currency_amount,
       convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.central_amazon_selling_partner_v1.financial_adjustment_event;
