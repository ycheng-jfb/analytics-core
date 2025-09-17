create or replace temporary table _qty_received as
select
    pds.BUSINESS_UNIT as BUSINESS_UNIT
    ,pds.REGION as region
    ,pds.COUNTRY as country
    ,pds.PRODUCT_SKU as product_sku
    ,cast(pds.DATE_RECEIVED as date) as date_received
    ,mdp.DEPARTMENT_DETAIL
    ,mdp.SUBCATEGORY
    ,mdp.STYLE_NAME
    ,mdp.COLOR
    ,mdp.SHARED

    ,sum(case
            when mdp.shared = mdp.BUSINESS_UNIT or mdp.shared is null
            then pds.QTY_RECEIPT else 0 end) as qty_receipt
from reporting_prod.gfb.GFB_PO_DATA_SET pds
join reporting_prod.GFB.MERCH_DIM_PRODUCT mdp
    on lower(mdp.BUSINESS_UNIT) = lower(pds.BUSINESS_UNIT)
    and lower(mdp.REGION) = lower(pds.REGION)
    and lower(mdp.COUNTRY) = lower(pds.COUNTRY)
    and mdp.PRODUCT_SKU = pds.PRODUCT_SKU
where
    upper(pds.BUSINESS_UNIT) in ('JUSTFAB', 'SHOEDAZZLE')
group by
    pds.BUSINESS_UNIT
    ,pds.REGION
    ,pds.COUNTRY
    ,pds.PRODUCT_SKU
    ,cast(pds.DATE_RECEIVED as date)
    ,mdp.DEPARTMENT_DETAIL
    ,mdp.SUBCATEGORY
    ,mdp.STYLE_NAME
    ,mdp.COLOR
    ,mdp.SHARED;


create or replace transient table reporting_prod.GFB.dos_114_ladder_plan as
select
    coalesce(md.BUSINESS_UNIT, qr.BUSINESS_UNIT) as BUSINESS_UNIT
    ,coalesce(md.REGION, qr.region) as REGION
    ,coalesce(md.COUNTRY, qr.country) as COUNTRY
    ,coalesce(md.PRODUCT_SKU, qr.product_sku) as PRODUCT_SKU
    ,coalesce(md.DEPARTMENT_DETAIL, qr.DEPARTMENT_DETAIL) as DEPARTMENT_DETAIL
    ,coalesce(md.SUBCATEGORY, qr.SUBCATEGORY) as SUBCATEGORY
    ,coalesce(md.STYLE_NAME, qr.STYLE_NAME) as STYLE_NAME
    ,coalesce(md.COLOR, qr.COLOR) as COLOR
    ,coalesce(md.DATE, qr.date_received) as DATE
    ,coalesce(md.SHARED, qr.SHARED) as SHARED

    ,coalesce(md.TOTAL_QTY_SOLD, 0) as TOTAL_QTY_SOLD
    ,coalesce(md.QTY_ONHAND, 0) as QTY_ONHAND
    ,coalesce(qr.qty_receipt, 0) as qty_receipt
    ,coalesce(md.QTY_AVAILABLE_TO_SELL, 0) as QTY_AVAILABLE_TO_SELL
from reporting_prod.GFB.DOS_107_MERCH_DATA_SET_BY_PLACE_DATE md
full join _qty_received qr
    on lower(qr.BUSINESS_UNIT) = lower(md.BUSINESS_UNIT)
    and lower(qr.region) = lower(md.REGION)
    and lower(qr.country) = lower(md.COUNTRY)
    and qr.product_sku = md.PRODUCT_SKU
    and qr.date_received = md.DATE
where
    (
        coalesce(md.TOTAL_QTY_SOLD, 0) > 0
        or
        coalesce(md.QTY_ONHAND, 0) > 0
        or
        coalesce(qr.qty_receipt, 0) > 0
        or
        coalesce(md.QTY_AVAILABLE_TO_SELL, 0) > 0
    );
