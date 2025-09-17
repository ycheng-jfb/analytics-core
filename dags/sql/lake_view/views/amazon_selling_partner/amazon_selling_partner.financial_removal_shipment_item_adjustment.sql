CREATE OR REPLACE VIEW lake_view.amazon_selling_partner.financial_removal_shipment_item_adjustment
            (
                        removal_shipment_item_id,
                        removal_shipment_adjustment_event_id,
                        tax_collection_model,
                        fulfillment_network_sku,
                        adjusted_quantity,
                        revenue_adjustment_currency_code,
                        revenue_adjustment_currency_amount,
                        tax_amount_adjustment_currency_code,
                        tax_amount_adjustment_currency_amount,
                        tax_withheld_adjustment_currency_code,
                        tax_withheld_adjustment_currency_amount,
                        meta_update_datetime
            )
            AS
SELECT removal_shipment_item_id,
       removal_shipment_adjustment_event_id,
       tax_collection_model,
       fulfillment_network_sku,
       adjusted_quantity,
       revenue_adjustment_currency_code,
       revenue_adjustment_currency_amount,
       tax_amount_adjustment_currency_code,
       tax_amount_adjustment_currency_amount,
       tax_withheld_adjustment_currency_code,
       tax_withheld_adjustment_currency_amount,
       convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.central_amazon_selling_partner_v1.financial_removal_shipment_item_adjustment;
