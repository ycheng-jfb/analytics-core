CREATE OR REPLACE VIEW lake_view.amazon_selling_partner.financial_charge_component
            (
                        charge_type,
                        charge_kind,
                        INDEX,
                        linked_to,
                        linked_to_id,
                        currency_code,
                        currency_amount,
                        meta_update_datetime
            )
            AS
SELECT charge_type,
       charge_kind,
       INDEX,
       linked_to,
       linked_to_id,
       currency_code,
       currency_amount,
       convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.central_amazon_selling_partner_v1.financial_charge_component;
