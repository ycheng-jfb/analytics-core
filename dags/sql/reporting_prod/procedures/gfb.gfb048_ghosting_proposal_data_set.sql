create or replace transient table reporting_prod.GFB.gfb048_ghosting_proposal_data_set as
select distinct
    pds.BUSINESS_UNIT
    ,pds.REGION
    ,pds.WAREHOUSE
    ,pds.SKU
    ,pds.DESCRIPTION
    ,pds.PO_NUMBER
    ,pds.PO_QTY
    ,pds.PO_QTY/2 as ghosting_qty
    ,pds."fnd/eta"
    ,i.ITEM_ID
    ,ycd.TRAILER_ID as shipment_id
    ,pds.PRODUCT_SKU
from reporting_prod.GFB.GFB_PO_DATA_SET pds
join LAKE.ULTRA_WAREHOUSE.ITEM i
    on i.ITEM_NUMBER = pds.SKU
join REPORTING_BASE_PROD.GSC.YARD_CONTAINER_DATA ycd
    on ycd.ITEM_ID = i.ITEM_ID
    and ycd.PO_NUMBER = pds.PO_NUMBER
where
    pds.REGION = 'NA'
    and pds."fnd/eta" >= current_date()
    and pds."fnd/eta" < dateadd(day, 5, current_date());
