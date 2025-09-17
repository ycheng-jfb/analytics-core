CREATE OR REPLACE VIEW lake_view.amazon_selling_partner.financial_removal_shipment_item
            (
                        removal_shipment_item_id,
                        removal_shipment_event_id,
                        tax_collection_model,
                        fulfillment_network_sku,
                        quantity,
                        revenue_currency_code,
                        revenue_currency_amount,
                        fee_amount_currency_code,
                        fee_amount_currency_amount,
                        tax_amount_currency_code,
                        tax_amount_currency_amount,
                        tax_with_held_currency_code,
                        tax_with_held_currency_amount,
                        meta_update_datetime
            )
            AS
SELECT removal_shipment_item_id,
       removal_shipment_event_id,
       tax_collection_model,
       fulfillment_network_sku,
       quantity,
       revenue_currency_code,
       revenue_currency_amount,
       fee_amount_currency_code,
       fee_amount_currency_amount,
       tax_amount_currency_code,
       tax_amount_currency_amount,
       tax_with_held_currency_code,
       tax_with_held_currency_amount,
       convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.central_amazon_selling_partner_v1.financial_removal_shipment_item;
