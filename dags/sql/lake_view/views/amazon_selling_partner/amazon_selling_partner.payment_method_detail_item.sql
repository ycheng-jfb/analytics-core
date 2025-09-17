CREATE OR REPLACE VIEW lake_view.amazon_selling_partner.payment_method_detail_item
            (
                        amazon_order_id,
                        method,
                        meta_update_datetime
            )
            AS
SELECT amazon_order_id,
       method,
       convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.central_amazon_selling_partner_v1.payment_method_detail_item;
