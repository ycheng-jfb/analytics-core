CREATE OR REPLACE VIEW lake_view.amazon_selling_partner.item_sales_rank_type
            (
                        asin,
                        product_category_id,
                        RANK,
                        meta_update_datetime
            )
            AS
SELECT asin,
       product_category_id,
       RANK,
       convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.central_amazon_selling_partner_v1.item_sales_rank_type;
