CREATE OR REPLACE VIEW lake_view.amazon_selling_partner.item_display_group_sales_rank
            (
                        asin,
                        website_display_group,
                        title,
                        link,
                        RANK,
                        meta_update_datetime
            )
            AS
SELECT asin,
       website_display_group,
       title,
       link,
       RANK,
       convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.central_amazon_selling_partner_v1.item_display_group_sales_rank;
