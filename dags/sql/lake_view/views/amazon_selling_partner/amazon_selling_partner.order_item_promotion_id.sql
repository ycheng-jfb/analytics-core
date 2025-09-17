CREATE OR REPLACE VIEW lake_view.amazon_selling_partner.order_item_promotion_id
            (
                        order_item_id,
                        promotion_id,
                        amazon_order_id,
                        meta_update_datetime
            )
            AS
SELECT order_item_id,
       promotion_id,
       amazon_order_id,
       convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.central_amazon_sp_orders_v1.order_item_promotion_id;
