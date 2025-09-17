CREATE OR REPLACE VIEW lake_view.amazon_selling_partner.outbound_seller_feature
            (
                        feature_name,
                        feature_description,
                        seller_eligible,
                        meta_update_datetime
            )
            AS
SELECT feature_name,
       feature_description,
       seller_eligible,
       convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.central_amazon_selling_partner_v1.outbound_seller_feature;
