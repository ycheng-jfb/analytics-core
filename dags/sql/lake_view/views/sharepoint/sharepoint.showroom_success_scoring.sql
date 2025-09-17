create or replace view lake_view.sharepoint.showroom_success_scoring (
    PRODUCT_SKU,
    SHOWROOM_MONTH,
    PERFORMED_AS,
    STYLE_SCORE,
    RECOMMEND_BUYING_AGAIN,
    NOTES,
    META_CREATE_DATETIME,
    META_UPDATE_DATETIME
) as
SELECT
    product_sku,
    showroom_month::timestamp as showroom_month,
    performed_as,
    style_score,
    recommend_buying_again,
    Notes,
    _fivetran_synced::timestamp as meta_create_datetime,
    _fivetran_synced::timestamp as meta_update_datetime
FROM lake_fivetran.med_inbound_sharepoint_v1.showroom_success_scoring_master;
