CREATE OR REPLACE VIEW lake_view.amazon_selling_partner.item_relationship
            (
                        child_asin,
                        parent_asin,
                        TYPE,
                        meta_update_datetime
            )
            AS
SELECT child_asin,
       parent_asin,
       TYPE,
       convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.central_amazon_selling_partner_v1.item_relationship;
