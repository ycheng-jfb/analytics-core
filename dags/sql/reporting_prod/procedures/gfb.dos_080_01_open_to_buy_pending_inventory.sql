CREATE OR REPLACE TRANSIENT TABLE reporting_prod.GFB.DOS_080_01_OPEN_TO_BUY_PENDING_INVENTORY as
select
    mfd.BUSINESS_UNIT
    ,mfd.REGION
    ,mfd.DEPARTMENT
    ,mfd.SUBCATEGORY
    ,mfd.vendor
    ,mfd.country
    ,mfd.LATEST_LAUNCH_DATE as showroom_date
    ,(case
        when mfd.SHARED = mfd.BUSINESS_UNIT then 'Not Shared'
        else 'Shared' end) as is_shared
    ,mfd.IS_PLUSSIZE
    ,received.month_received as expecting_month_received
    ,mfd.PRODUCT_SKU

    --received inventory
    ,sum(case
            when received.month_received > dateadd(month, -6, current_date()) and (mfd.REGION = 'NA' or (mfd.REGION = 'EU' and mfd.COUNTRY = 'FR'))
            then coalesce(received.qty_pending, 0) else 0 end) as qty_pending
    ,sum(case
            when received.month_received > dateadd(month, -6, current_date()) and (mfd.REGION = 'NA' or (mfd.REGION = 'EU' and mfd.COUNTRY = 'FR'))
            then coalesce(received.landed_cost_pending, 0) else 0 end) as landed_cost_pending
    ,sum(case
            when received.month_received > dateadd(month, -6, current_date()) and (mfd.REGION = 'NA' or (mfd.REGION = 'EU' and mfd.COUNTRY = 'FR'))
            then coalesce(received.pending_retail, 0) else 0 end) as pending_retail
from
(
    select
        *
    from reporting_prod.GFB.MERCH_DIM_PRODUCT
    ) mfd
left join
(
    select
        upper(jpe.BRAND) as BUSINESS_UNIT
        ,(case
            when upper(jpe.REGION_ID) in ('US', 'CA') then 'NA'
            else 'EU' end) as region
        ,upper(jpe.REGION_ID) as country
        ,split(jpe.SKU, '-')[0] || '-' || split(jpe.SKU, '-')[1] as color_sku
        ,dd.MONTH_DATE as month_received
        ,sum(case
                when upper(jpe.PO_STATUS_TEXT) != 'RECEIVED' then jpe.QTY
            else 0 end) as qty_pending
        ,sum(case
                when upper(jpe.PO_STATUS_TEXT) != 'RECEIVED' then jpe.reporting_landed_cost * jpe.QTY
            else 0 end) as landed_cost_pending
        ,sum(case
                when upper(jpe.PO_STATUS_TEXT) != 'RECEIVED' then coalesce(mfd_retail.OG_RETAIL, mfd_retail.CURRENT_RETAIL) * jpe.QTY
            else 0 end) as pending_retail
    from REPORTING_PROD.GSC.PO_DETAIL_DATASET jpe
    join EDW_PROD.DATA_MODEL_JFB.dim_date dd
        on dd.FULL_DATE = jpe.FC_DELIVERY
    join
    (
        select distinct
            mfd.BUSINESS_UNIT
            ,mfd.REGION
            ,mfd.PRODUCT_SKU
            ,mfd.OG_RETAIL
            ,mfd.CURRENT_VIP_RETAIL as CURRENT_RETAIL
        from reporting_prod.GFB.MERCH_DIM_PRODUCT mfd
        ) mfd_retail on mfd_retail.PRODUCT_SKU = split(jpe.SKU, '-')[0] || '-' || split(jpe.SKU, '-')[1]
            and lower(mfd_retail.BUSINESS_UNIT) = lower(jpe.BRAND)
            and lower(mfd_retail.REGION) = lower((case
                                            when upper(jpe.REGION_ID) in ('US', 'CA') then 'NA'
                                            else 'EU' end))
    where
        jpe.STYLE_NAME != 'TBD'
        and jpe.PO_STATUS_ID in (4,3,8,12)
        and upper(jpe.LINE_STATUS) != 'CANCELLED'
        and upper(jpe.REGION_ID) in ('US', 'CA', 'EU')
        and upper(jpe.BRAND) in ('JUSTFAB', 'SHOEDAZZLE')
        and dd.MONTH_DATE >= dateadd(month, -25, date_trunc(month, current_date))
    group by
        upper(jpe.BRAND)
        ,(case
            when upper(jpe.REGION_ID) in ('US', 'CA') then 'NA'
            else 'EU' end)
        ,upper(jpe.REGION_ID)
        ,split(jpe.SKU, '-')[0] || '-' || split(jpe.SKU, '-')[1]
        ,dd.MONTH_DATE
    ) received on received.color_sku = mfd.PRODUCT_SKU
            and lower(received.BUSINESS_UNIT) = lower(mfd.BUSINESS_UNIT)
            and lower(received.region) = lower(mfd.region)
            and lower(received.country) = lower(case
                                                    when mfd.COUNTRY in ('US', 'CA') then mfd.COUNTRY
                                                    else mfd.REGION end)
group by
    mfd.BUSINESS_UNIT
    ,mfd.REGION
    ,mfd.DEPARTMENT
    ,mfd.SUBCATEGORY
    ,mfd.vendor
    ,mfd.country
    ,mfd.LATEST_LAUNCH_DATE
    ,(case
        when mfd.SHARED = mfd.BUSINESS_UNIT then 'Not Shared'
        else 'Shared' end)
    ,mfd.IS_PLUSSIZE
    ,received.month_received
    ,mfd.PRODUCT_SKU
having
    sum(case
            when received.month_received > dateadd(month, -6, current_date())
            then coalesce(received.qty_pending, 0) else 0 end) > 0
