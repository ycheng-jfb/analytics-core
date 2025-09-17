CREATE OR REPLACE VIEW lake_view.amazon_selling_partner.item_classification_sales_rank
            (
                        asin,
                        classification_id,
                        title,
                        link,
                        RANK,
                        meta_update_datetime
            )
            AS
SELECT asin,
       classification_id,
       title,
       link,
       RANK,
       convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.central_amazon_selling_partner_v1.item_classification_sales_rank;
