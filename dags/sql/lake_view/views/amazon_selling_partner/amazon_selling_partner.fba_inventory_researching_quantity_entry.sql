CREATE OR REPLACE VIEW lake_view.amazon_selling_partner.fba_inventory_researching_quantity_entry
            (
                        inventory_summary_id,
                        name,
                        granularity_type,
                        granularity_id,
                        quantity,
                        meta_update_datetime
            )
            AS
SELECT inventory_summary_id,
       name,
       granularity_type,
       granularity_id,
       quantity,
       convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.central_amazon_selling_partner_v1.fba_inventory_researching_quantity_entry;
