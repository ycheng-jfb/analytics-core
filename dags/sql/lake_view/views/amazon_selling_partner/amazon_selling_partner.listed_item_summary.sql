CREATE OR REPLACE VIEW lake_view.amazon_selling_partner.listed_item_summary
            (
                        asin,
                        marketplace_id,
                        sku,
                        product_type,
                        condition_type,
                        status,
                        fn_sku,
                        item_name,
                        created_date,
                        last_updated_date,
                        link,
                        height,
                        width,
                        meta_update_datetime
            )
            AS
SELECT asin,
       marketplace_id,
       sku,
       product_type,
       condition_type,
       status,
       fn_sku,
       item_name,
       created_date,
       last_updated_date,
       link,
       height,
       width,
       convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.central_amazon_selling_partner_v1.listed_item_summary;
