CREATE OR REPLACE VIEW lake_view.amazon_selling_partner.financial_removal_shipment_event
            (
                        _fivetran_id,
                        financial_event_group_id,
                        posted_date,
                        merchant_order_id,
                        order_id,
                        transaction_type,
                        meta_update_datetime
            )
            AS
SELECT _fivetran_id,
       financial_event_group_id,
       posted_date,
       merchant_order_id,
       order_id,
       transaction_type,
       convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.central_amazon_selling_partner_v1.financial_removal_shipment_event;
