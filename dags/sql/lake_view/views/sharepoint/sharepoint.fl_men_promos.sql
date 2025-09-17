CREATE OR REPLACE VIEW lake_view.sharepoint.fl_men_promos (
    promo_code,
    promo_name,
    meta_create_datetime,
    meta_update_datetime
) AS
SELECT
    promo_code,
    promo_name,
    _fivetran_synced::timestamp as meta_create_datetime,
    _fivetran_synced::timestamp as meta_update_datetime
FROM lake_fivetran.fabletics_sharepoint_v1.fl_men_s_gateways_and_promos_promos;
