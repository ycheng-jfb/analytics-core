CREATE OR REPLACE VIEW lake_view.amazon_selling_partner.financial_shipment_event_item
            (
                        _fivetran_id,
                        financial_shipment_event_id,
                        item_kind,
                        INDEX,
                        seller_sku,
                        order_item_id,
                        order_adjustment_item_id,
                        quantity_shipped,
                        cost_of_points_granted_currency_code,
                        cost_of_points_granted_currency_amount,
                        cost_of_points_returned_currency_code,
                        cost_of_points_returned_currency_amount,
                        meta_update_datetime
            )
            AS
SELECT _fivetran_id,
       financial_shipment_event_id,
       item_kind,
       INDEX,
       seller_sku,
       order_item_id,
       order_adjustment_item_id,
       quantity_shipped,
       cost_of_points_granted_currency_code,
       cost_of_points_granted_currency_amount,
       cost_of_points_returned_currency_code,
       cost_of_points_returned_currency_amount,
       convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.central_amazon_selling_partner_v1.financial_shipment_event_item;
