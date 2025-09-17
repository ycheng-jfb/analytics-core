CREATE OR REPLACE VIEW lake_view.amazon_selling_partner.item_offer_count_type
            (
                        asin,
                        TYPE,
                        CONDITION,
                        fullfillment_channel,
                        offer_count,
                        meta_update_datetime
            )
            AS
SELECT asin,
       TYPE,
       CONDITION,
       fullfillment_channel,
       offer_count,
       convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.central_amazon_selling_partner_v1.item_offer_count_type;
