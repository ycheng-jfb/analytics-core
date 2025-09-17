CREATE OR REPLACE VIEW lake_view.sharepoint.dynamic_display_card_offer AS
SELECT start_date_include,
       offer_promo,
       end_date_exclude,
       channel,
       country,
       store_id,
       ad_id,
       store_country,
       store_brand,
    _fivetran_synced:: TIMESTAMP AS META_CREATE_DATETIME
FROM (
    select *,row_number() OVER ( PARTITION BY start_date_include,offer_promo,end_date_exclude,channel,country,store_id,ad_id,store_country,store_brand ORDER BY _fivetran_synced ) AS rn
    FROM lake_fivetran.med_sharepoint_acquisition_v1.display_card_tagging_generator
) a
WHERE a._line>1 and ad_id is not null and a.rn = 1;
