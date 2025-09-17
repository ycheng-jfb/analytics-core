CREATE OR REPLACE VIEW lake_view.amazon_selling_partner.content_meta_data_record
            (
                        content_reference_key,
                        marketplace_id,
                        name,
                        status,
                        update_time,
                        meta_update_datetime
            )
            AS
SELECT content_reference_key,
       marketplace_id,
       name,
       status,
       update_time,
       convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.central_amazon_selling_partner_v1.content_meta_data_record;
