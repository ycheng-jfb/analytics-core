CREATE OR REPLACE VIEW LAKE_VIEW.PINTEREST.PRODUCT_GROUPS_METADATA(
    ad_group_id
    ,bid_in_micro_currency
    ,catalog_product_group_id
    ,catalog_product_group_name
    ,collections_hero_destination_url
    ,collections_hero_pin_id
    ,creative_type
    ,definition
    ,id
    ,included
    ,parent_id
    ,relative_definition
    ,slideshow_collections_description
    ,slideshow_collections_title
    ,status
    ,tracking_url
    ,type
    ,META_CREATE_DATETIME
    ,META_UPDATE_DATETIME
    ) AS
select ad_group_id
    ,bid_in_micro_currency
    ,catalog_product_group_id
    ,catalog_product_group_name
    ,collections_hero_destination_url
    ,collections_hero_pin_id
    ,creative_type
    ,definition
    ,id
    ,included
    ,parent_id
    ,relative_definition
    ,slideshow_collections_description
    ,slideshow_collections_title
    ,status
    ,tracking_url
    ,'productgroup' as type
    ,convert_timezone('America/Los_Angeles',_FIVETRAN_SYNCED) AS META_CREATE_DATETIME
    ,convert_timezone('America/Los_Angeles',_FIVETRAN_SYNCED) as META_UPDATE_DATETIME
from  LAKE_FIVETRAN.MED_PINTEREST_AD30_V1.product_group_history
;
