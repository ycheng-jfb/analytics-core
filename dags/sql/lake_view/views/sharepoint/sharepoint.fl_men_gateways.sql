create or replace view lake_view.sharepoint.fl_men_gateways (
    GATEWAY,
    OFFER,
    META_CREATE_DATETIME,
    META_UPDATE_DATETIME
) as
SELECT
    gateway,
    offer,
    _fivetran_synced::timestamp as meta_create_datetime,
    _fivetran_synced::timestamp as meta_update_datetime
FROM lake_fivetran.fabletics_sharepoint_v1.fl_men_s_gateways_and_promos_gateways;
