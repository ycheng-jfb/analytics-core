CREATE OR REPLACE VIEW lake_view.amazon_selling_partner.content_badge_set
            (
                        content_reference_key,
                        content_badge,
                        meta_update_datetime
            )
            AS
SELECT content_reference_key,
       content_badge,
       convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.central_amazon_selling_partner_v1.content_badge_set;
