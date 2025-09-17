CREATE OR REPLACE VIEW lake_view.amazon_selling_partner.financial_shipment_event_item_promotion
            (
                        financial_shipment_event_item_id,
                        INDEX,
                        TYPE,
                        amount_currency_code,
                        amount_currency_amount,
                        promotion_kind,
                        id,
                        meta_update_datetime
            )
            AS
SELECT financial_shipment_event_item_id,
       INDEX,
       TYPE,
       amount_currency_code,
       amount_currency_amount,
       promotion_kind,
       id,
       convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.central_amazon_selling_partner_v1.financial_shipment_event_item_promotion;
