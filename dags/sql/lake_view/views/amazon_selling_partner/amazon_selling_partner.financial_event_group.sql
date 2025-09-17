CREATE OR REPLACE VIEW lake_view.amazon_selling_partner.financial_event_group
            (
                        financial_event_group_id,
                        processing_status,
                        fund_transfer_status,
                        original_total_currency_code,
                        original_total_currency_amount,
                        converted_total_currency_code,
                        converted_total_currency_amount,
                        fund_transfer_date,
                        trace_id,
                        account_tail,
                        beginning_balance_currency_code,
                        beginning_balance_currency_amount,
                        financial_event_group_start,
                        financial_event_group_end,
                        meta_update_datetime
            )
            AS
SELECT financial_event_group_id,
       processing_status,
       fund_transfer_status,
       original_total_currency_code,
       original_total_currency_amount,
       converted_total_currency_code,
       converted_total_currency_amount,
       fund_transfer_date,
       trace_id,
       account_tail,
       beginning_balance_currency_code,
       beginning_balance_currency_amount,
       financial_event_group_start,
       financial_event_group_end,
       convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.central_amazon_selling_partner_v1.financial_event_group;
