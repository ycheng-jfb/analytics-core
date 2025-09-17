use reporting_prod;

set c_date = current_date()::date;

CREATE OR REPLACE TEMP TABLE _returns_shipped as
select di.ITEM                                                                                    as SKU,
       di.PRODUCT_SKU                                                                             as CC,
       CASE
           WHEN fole.SALES_CHANNEL in ('Online', 'Mobile App', 'Group Order') and
                fole.MEMBERSHIP_ORDER_TYPE_L1 = 'Activating VIP'
               THEN 'ECOM_ACT'
           WHEN fole.SALES_CHANNEL in ('Online', 'Mobile App', 'Group Order') and
                fole.MEMBERSHIP_ORDER_TYPE_L1 = 'NonActivating'
               THEN 'ECOM_NON'
           WHEN fole.SALES_CHANNEL = 'Retail' THEN 'RETAIL'
           END
                                                                                                  as DMDGROUP,
       dl.LOCATION                                                                                as LOCATION,
       rl.RETURN_RECEIPT_LOCAL_DATETIME:: DATE                                                    as STARTDATE,
       'SHIP'                                                                                     as HISTSTREAM,
       1440                                                                                       as DUR,
       0                                                                                          as UNITS_NONACT,
       0                                                                                          as UNITS_ACT,
       0                                                                                          as COST_NONACT,
       0                                                                                          as COST_ACT,
       0                                                                                          as RETAIL_NONACT,
       0                                                                                          as RETAIL_ACT,
       0                                                                                          as DISC_NONACT,
       0                                                                                          as DISC_ACT,
       sum(rl.RETURN_ITEM_QUANTITY)                                                               AS RET_UNITS,
       sum(rl.ESTIMATED_RETURNED_PRODUCT_LOCAL_COST * rl.RETURN_RECEIPT_DATE_USD_CONVERSION_RATE) AS RET_COST,
       0                                                                                          as DMD_FLAG,
       1                                                                                          as ALLOC_FLAG,
       1                                                                                          as EP_FLAG,
       1                                                                                          as SS_FLAG
from edw_prod.data_model_fl.fact_return_line rl
         join EDW_PROD.DATA_MODEL_FL.DIM_PRODUCT dp on dp.PRODUCT_ID = rl.PRODUCT_ID
         left join REPORTING_BASE_PROD.BLUE_YONDER.DIM_ITEM di on dp.SKU = di.ITEM
    join REPORTING_BASE_PROD.FABLETICS.FACT_ORDER_LINE_EXTENDED fole on fole.ORDER_LINE_ID = rl.ORDER_LINE_ID
         left join REPORTING_BASE_PROD.BLUE_YONDER.DIM_LOCATION dl on dl.STORE_ID = fole.STORE_ID
where rl.RETURN_STATUS_KEY = 2
  and fole.SALES_CHANNEL != 'Retail' --Christina added 8.6.24 to have only Retail POS data
  and rl.RETURN_RECEIPT_LOCAL_DATETIME::DATE >= DATEADD('YEAR', -2, $c_date)
    -- can be reduced to -5 weeks using filter condition for Sunday and Non-Sunday runs
group by 1, 2, 3, 4, 5, 6, 7;

CREATE OR REPLACE TEMP TABLE _history_shipped AS
SELECT DISTINCT di.ITEM                            as SKU,
                di.PRODUCT_SKU                     as CC,
                CASE
                    WHEN fole.SALES_CHANNEL in ('Online', 'Mobile App', 'Group Order') and
                         fole.MEMBERSHIP_ORDER_TYPE_L1 = 'Activating VIP'
                        THEN 'ECOM_ACT'
                    WHEN fole.SALES_CHANNEL in ('Online', 'Mobile App', 'Group Order') and
                         fole.MEMBERSHIP_ORDER_TYPE_L1 = 'NonActivating'
                        THEN 'ECOM_NON'
                    WHEN fole.SALES_CHANNEL = 'Retail' THEN 'RETAIL'
                    END
                                                                                                        as DMDGROUP,
                dl.LOCATION                                                                             as LOCATION,
                fole.SHIPPED_LOCAL_DATETIME:: DATE as STARTDATE,
                'SHIP'                                                                                  as HISTSTREAM,
                1440                                                                                    as DUR,
                SUM(IFF(fole.MEMBERSHIP_ORDER_TYPE_L1 = 'NonActivating', fole.ITEM_QUANTITY,
                        0))                                                                             as UNITS_NONACT,
                SUM(IFF(fole.MEMBERSHIP_ORDER_TYPE_L1 = 'Activating VIP', fole.ITEM_QUANTITY,
                        0))                                                                             as UNITS_ACT,
                SUM(IFF(fole.MEMBERSHIP_ORDER_TYPE_L1 = 'NonActivating', fole.REPORTING_LANDED_COST_AMOUNT,
                        0))                                                                             as COST_NONACT,
                SUM(IFF(fole.MEMBERSHIP_ORDER_TYPE_L1 = 'Activating VIP', fole.REPORTING_LANDED_COST_AMOUNT,
                        0))                                                                             as COST_ACT,
                SUM(IFF(fole.MEMBERSHIP_ORDER_TYPE_L1 = 'NonActivating',
                        fole.PRODUCT_GROSS_REVENUE_EXCL_SHIPPING_AMOUNT,
                        0))                        as RETAIL_NONACT,
                SUM(IFF(fole.MEMBERSHIP_ORDER_TYPE_L1 = 'Activating VIP',
                        fole.PRODUCT_GROSS_REVENUE_EXCL_SHIPPING_AMOUNT,
                        0))                        as RETAIL_ACT,
                SUM(IFF(fole.MEMBERSHIP_ORDER_TYPE_L1 = 'NonActivating', fole.BLENDED_DISCOUNT,
                        0))                        as DISC_NONACT,
                SUM(IFF(fole.MEMBERSHIP_ORDER_TYPE_L1 = 'Activating VIP', fole.BLENDED_DISCOUNT,
                        0))                        as DISC_ACT,
                0                                  AS RET_UNITS,
                0                                  AS RET_COST,
                0                                  as DMD_FLAG,
                1                                  as ALLOC_FLAG,
                1                                  as EP_FLAG,
                1                                  as SS_FLAG
from REPORTING_BASE_PROD.FABLETICS.FACT_ORDER_LINE_EXTENDED fole
         left join REPORTING_BASE_PROD.BLUE_YONDER.DIM_ITEM di
                   on di.ITEM = fole.SKU
         left join REPORTING_BASE_PROD.BLUE_YONDER.DIM_LOCATION dl on dl.STORE_ID = fole.STORE_ID
where 1 = 1
  and fole.ORDER_DATE >= DATEADD('YEAR', -2, $c_date)
  and fole.SALES_CHANNEL != 'Retail' --Christina added 8.6.24 to have only Retail POS data
  and fole.PRODUCT_TYPE_NAME in ('BYO', 'Normal', 'Bundle Component')
group by 1, 2, 3, 4, 5, 6, 7
;

create or replace temp table _shipped_combined as
select *
from _returns_shipped
UNION ALL
select *
from _history_shipped;

create or replace temp table _final_shipped as
select SKU,
       CC,
       DMDGROUP,
       LOCATION,
       STARTDATE,
       HISTSTREAM,
       DUR,
       COALESCE(SUM(UNITS_NONACT), 0)  AS UNITS_NONACT,
       COALESCE(SUM(UNITS_ACT), 0)     AS UNITS_ACT,
       COALESCE(SUM(COST_NONACT), 0)   AS COST_NONACT,
       COALESCE(SUM(COST_ACT), 0)      AS COST_ACT,
       COALESCE(SUM(RETAIL_NONACT), 0) AS RETAIL_NONACT,
       COALESCE(SUM(RETAIL_ACT), 0)    AS RETAIL_ACT,
       COALESCE(SUM(DISC_NONACT), 0)   AS DISC_NONACT,
       COALESCE(SUM(DISC_ACT), 0)      AS DISC_ACT,
       COALESCE(SUM(RET_UNITS), 0)     AS RET_UNITS,
       COALESCE(SUM(RET_COST), 0)      AS RET_COST,
       DMD_FLAG,
       ALLOC_FLAG,
       EP_FLAG,
       SS_FLAG
from _shipped_combined
GROUP BY 1, 2, 3, 4, 5, 6, 7, 18, 19, 20, 21
;

CREATE OR REPLACE TEMP TABLE _returns_placed as
select di.ITEM                                                                                    as SKU,
       di.PRODUCT_SKU                                                                             as CC,
       CASE
           WHEN fole.SALES_CHANNEL in ('Online', 'Mobile App', 'Group Order') and
                fole.MEMBERSHIP_ORDER_TYPE_L1 = 'Activating VIP'
               THEN 'ECOM_ACT'
           WHEN fole.SALES_CHANNEL in ('Online', 'Mobile App', 'Group Order') and
                fole.MEMBERSHIP_ORDER_TYPE_L1 = 'NonActivating'
               THEN 'ECOM_NON'
           WHEN fole.SALES_CHANNEL = 'Retail' THEN 'RETAIL'
           END
                                                                                                  as DMDGROUP,
       dl.LOCATION                                                                                as LOCATION,
       rl.RETURN_RECEIPT_LOCAL_DATETIME:: DATE                                                    as STARTDATE,
       CASE
           WHEN fole.SALES_CHANNEL in ('Online', 'Mobile App', 'Group Order') THEN 'OP'
           WHEN fole.SALES_CHANNEL = 'Retail' then 'POS' END                                           as HISTSTREAM,
       1440                                                                                       as DUR,
       0                                                                                          as UNITS_NONACT,
       0                                                                                          as UNITS_ACT,
       0                                                                                          as COST_NONACT,
       0                                                                                          as COST_ACT,
       0                                                                                          as RETAIL_NONACT,
       0                                                                                          as RETAIL_ACT,
       0                                                                                          as DISC_NONACT,
       0                                                                                          as DISC_ACT,
       sum(rl.RETURN_ITEM_QUANTITY)                                                               AS RET_UNITS,
       sum(rl.ESTIMATED_RETURNED_PRODUCT_LOCAL_COST * rl.RETURN_RECEIPT_DATE_USD_CONVERSION_RATE) AS RET_COST,
       CASE
           WHEN fole.SALES_CHANNEL = 'Retail' THEN 1
           WHEN di.IS_LADDER = TRUE and fole.SALES_CHANNEL in ('Online', 'Mobile App', 'Group Order') THEN 1
           ELSE 0 END
                                                                                                  as DMD_FLAG,
       iff(fole.SALES_CHANNEL = 'Retail', 1, 0)                                                        as ALLOC_FLAG,
       iff(fole.SALES_CHANNEL = 'Retail', 1, 0)                                                        as EP_FLAG,
       iff(fole.SALES_CHANNEL = 'Retail', 1, 0)                                                        as SS_FLAG
from edw_prod.data_model_fl.fact_return_line rl
         join EDW_PROD.DATA_MODEL_FL.DIM_PRODUCT dp on dp.PRODUCT_ID = rl.PRODUCT_ID
         left join REPORTING_BASE_PROD.BLUE_YONDER.DIM_ITEM di on dp.SKU = di.ITEM
    join REPORTING_BASE_PROD.FABLETICS.FACT_ORDER_LINE_EXTENDED fole on fole.ORDER_LINE_ID = rl.ORDER_LINE_ID
         left join REPORTING_BASE_PROD.BLUE_YONDER.DIM_LOCATION dl on dl.STORE_ID = fole.STORE_ID
where rl.RETURN_STATUS_KEY = 2
  and rl.RETURN_RECEIPT_LOCAL_DATETIME::DATE >= DATEADD('YEAR', -2, $c_date)
group by 1, 2, 3, 4, 5, 6, 7, 18, 19, 20, 21
HAVING SUM(CASE
               WHEN fole.SALES_CHANNEL = 'Retail' THEN 1
               WHEN di.IS_LADDER = true AND fole.SALES_CHANNEL IN ('Online', 'Mobile App', 'Group Order') THEN 1
               ELSE 0 END) != 0
    OR SUM(IFF(fole.SALES_CHANNEL = 'Retail', 1, 0)) != 0
    OR SUM(IFF(fole.SALES_CHANNEL = 'Retail', 1, 0)) != 0
    OR SUM(IFF(fole.SALES_CHANNEL = 'Retail', 1, 0)) != 0;

CREATE OR REPLACE TEMP TABLE _history_placed AS
SELECT DISTINCT di.ITEM                                               as SKU,
                di.PRODUCT_SKU                                        as CC,
                CASE
                    WHEN fole.SALES_CHANNEL in ('Online', 'Mobile App', 'Group Order') and
                         fole.MEMBERSHIP_ORDER_TYPE_L1 = 'Activating VIP'
                        THEN 'ECOM_ACT'
                    WHEN fole.SALES_CHANNEL in ('Online', 'Mobile App', 'Group Order') and
                         fole.MEMBERSHIP_ORDER_TYPE_L1 = 'NonActivating'
                        THEN 'ECOM_NON'
                    WHEN fole.SALES_CHANNEL = 'Retail' THEN 'RETAIL'
                    END
                                                                      as DMDGROUP,
                dl.LOCATION                                           as LOCATION,
                fole.ORDER_DATE                                       as STARTDATE,
                CASE
                    WHEN fole.sales_channel in ('Online', 'Mobile App', 'Group Order') THEN 'OP'
                    WHEN fole.sales_channel = 'Retail' then 'POS' END as HISTSTREAM,
                1440                                                  as DUR,
                SUM(IFF(fole.MEMBERSHIP_ORDER_TYPE_L1 = 'NonActivating', fole.ITEM_QUANTITY,
                        0))                                           as UNITS_NONACT,
                SUM(IFF(fole.MEMBERSHIP_ORDER_TYPE_L1 = 'Activating VIP', fole.ITEM_QUANTITY,
                        0))                                           as UNITS_ACT,
                SUM(IFF(fole.MEMBERSHIP_ORDER_TYPE_L1 = 'NonActivating', fole.REPORTING_LANDED_COST_AMOUNT,
                        0))                                           as COST_NONACT,
                SUM(IFF(fole.MEMBERSHIP_ORDER_TYPE_L1 = 'Activating VIP', fole.REPORTING_LANDED_COST_AMOUNT,
                        0))                                           as COST_ACT,
                SUM(IFF(fole.MEMBERSHIP_ORDER_TYPE_L1 = 'NonActivating',
                        fole.PRODUCT_GROSS_REVENUE_EXCL_SHIPPING_AMOUNT,
                        0))                                           as RETAIL_NONACT,
                SUM(IFF(fole.MEMBERSHIP_ORDER_TYPE_L1 = 'Activating VIP',
                        fole.PRODUCT_GROSS_REVENUE_EXCL_SHIPPING_AMOUNT,
                        0))                                           as RETAIL_ACT,
                SUM(IFF(fole.MEMBERSHIP_ORDER_TYPE_L1 = 'NonActivating', fole.BLENDED_DISCOUNT,
                        0))                                           as DISC_NONACT,
                SUM(IFF(fole.MEMBERSHIP_ORDER_TYPE_L1 = 'Activating VIP', fole.BLENDED_DISCOUNT,
                        0))                                           as DISC_ACT,
                0                                                     AS RET_UNITS,
                0                                                     AS RET_COST,
                CASE
                    WHEN fole.sales_channel = 'Retail' THEN 1
                    WHEN di.IS_LADDER = TRUE and fole.sales_channel in ('Online', 'Mobile App', 'Group Order')
                        THEN 1
                    ELSE 0 END
                                                                      as DMD_FLAG,
                iff(fole.sales_channel = 'Retail', 1, 0)              as ALLOC_FLAG,
                iff(fole.sales_channel = 'Retail', 1, 0)              as EP_FLAG,
                iff(fole.sales_channel = 'Retail', 1, 0)              as SS_FLAG
from REPORTING_BASE_PROD.FABLETICS.FACT_ORDER_LINE_EXTENDED fole
         left join REPORTING_BASE_PROD.BLUE_YONDER.DIM_ITEM di
                   on di.ITEM = fole.SKU
         left join REPORTING_BASE_PROD.BLUE_YONDER.DIM_LOCATION dl on dl.STORE_ID = fole.STORE_ID
where 1 = 1
  and fole.ORDER_DATE >= DATEADD('YEAR', -2, $c_date)
  and fole.PRODUCT_TYPE_NAME in ('BYO', 'Normal', 'Bundle Component')
group by 1, 2, 3, 4, 5, 6, 7, 18, 19, 20, 21
HAVING SUM(CASE
               WHEN fole.SALES_CHANNEL = 'Retail' THEN 1
               WHEN di.IS_LADDER = true AND fole.SALES_CHANNEL IN ('Online', 'Mobile App', 'Group Order') THEN 1
               ELSE 0 END) != 0
    OR SUM(IFF(fole.SALES_CHANNEL = 'Retail', 1, 0)) != 0
    OR SUM(IFF(fole.SALES_CHANNEL = 'Retail', 1, 0)) != 0
    OR SUM(IFF(fole.SALES_CHANNEL = 'Retail', 1, 0)) != 0
;

create or replace temp table _placed_combined as
select *
from _returns_placed
UNION ALL
select *
from _history_placed;

create or replace table _final_placed as
select SKU,
       CC,
       DMDGROUP,
       LOCATION,
       STARTDATE,
       HISTSTREAM,
       DUR,
       COALESCE(SUM(UNITS_NONACT), 0)  AS UNITS_NONACT,
       COALESCE(SUM(UNITS_ACT), 0)     AS UNITS_ACT,
       COALESCE(SUM(COST_NONACT), 0)   AS COST_NONACT,
       COALESCE(SUM(COST_ACT), 0)      AS COST_ACT,
       COALESCE(SUM(RETAIL_NONACT), 0) AS RETAIL_NONACT,
       COALESCE(SUM(RETAIL_ACT), 0)    AS RETAIL_ACT,
       COALESCE(SUM(DISC_NONACT), 0)   AS DISC_NONACT,
       COALESCE(SUM(DISC_ACT), 0)      AS DISC_ACT,
       COALESCE(SUM(RET_UNITS), 0)     AS RET_UNITS,
       COALESCE(SUM(RET_COST), 0)      AS RET_COST,
       DMD_FLAG,
       ALLOC_FLAG,
       EP_FLAG,
       SS_FLAG
from _placed_combined
GROUP BY 1, 2, 3, 4, 5, 6, 7, 18, 19, 20, 21
;

create or replace temp table _final_history as
select *
from _final_shipped
UNION ALL
select *
from _final_placed;


truncate table reporting_prod.blue_yonder.export_by_history_interface_stg;

insert into reporting_prod.blue_yonder.export_by_history_interface_stg
(
SKU,
CC,
DMDGROUP,
LOC,
STARTDATE,
HISTSTREAM,
DUR,
UNITS_NONACT,
UNITS_ACT,
COST_NONACT,
COST_ACT,
RETAIL_NONACT,
RETAIL_ACT,
DISC_NONACT,
DISC_ACT,
RET_UNITS,
RET_COST,
DMD_FLAG,
ALLOC_FLAG,
EP_FLAG,
SS_FLAG,
DATE_ADDED
)
select SKU,
    CC,
    DMDGROUP,
    LOCATION,
    STARTDATE,
    HISTSTREAM,
    DUR,
    UNITS_NONACT,
    UNITS_ACT,
    COST_NONACT,
    COST_ACT,
    RETAIL_NONACT,
    RETAIL_ACT,
    DISC_NONACT,
    DISC_ACT,
    RET_UNITS,
    RET_COST,
    DMD_FLAG,
    ALLOC_FLAG,
    EP_FLAG,
    SS_FLAG,
    current_timestamp
from _final_history
where STARTDATE >= dateadd('week', -5, $c_date)
and cc not in (
    with _all_new_ccs as
    (
        select item
        from reporting_prod.blue_yonder.export_by_item_interface_stg -- today's feed
        where item not in (
                select item
                from reporting_prod.blue_yonder.export_by_item_interface -- snapshot table
                where date_added::date = DATE_TRUNC('WEEK', $c_date::date)-1
                and item NOT LIKE '%-%-%')
            )
    select distinct nc.item
    from _all_new_ccs nc
    join REPORTING_BASE_PROD.BLUE_YONDER.DIM_ITEM di on di.PRODUCT_SKU = nc.item
    where IS_LADDER = true
    and dayofweek($c_date::date) = 0
)
;


insert into reporting_prod.blue_yonder.export_by_history_interface_stg
(
SKU,
CC,
DMDGROUP,
LOC,
STARTDATE,
HISTSTREAM,
DUR,
UNITS_NONACT,
UNITS_ACT,
COST_NONACT,
COST_ACT,
RETAIL_NONACT,
RETAIL_ACT,
DISC_NONACT,
DISC_ACT,
RET_UNITS,
RET_COST,
DMD_FLAG,
ALLOC_FLAG,
EP_FLAG,
SS_FLAG,
DATE_ADDED
)
select SKU,
    CC,
    DMDGROUP,
    LOCATION,
    STARTDATE,
    HISTSTREAM,
    DUR,
    UNITS_NONACT,
    UNITS_ACT,
    COST_NONACT,
    COST_ACT,
    RETAIL_NONACT,
    RETAIL_ACT,
    DISC_NONACT,
    DISC_ACT,
    RET_UNITS,
    RET_COST,
    DMD_FLAG,
    ALLOC_FLAG,
    EP_FLAG,
    SS_FLAG,
    current_timestamp
from _final_history
where STARTDATE >= DATEADD('YEAR', -2, $c_date)
and cc in (
    with _all_new_ccs as
    (
        select item
        from reporting_prod.blue_yonder.export_by_item_interface_stg -- today's feed
        where item not in (
                select item
                from reporting_prod.blue_yonder.export_by_item_interface -- snapshot table
                where date_added::date = DATE_TRUNC('WEEK', $c_date::date)-1
                and item NOT LIKE '%-%-%')
            )
    select distinct nc.item
    from _all_new_ccs nc
    join REPORTING_BASE_PROD.BLUE_YONDER.DIM_ITEM di on di.PRODUCT_SKU = nc.item
    where IS_LADDER = true
    and dayofweek($c_date::date) = 0
)
;

delete from reporting_prod.blue_yonder.export_by_history_interface_stg
where SKU is null
or CC is null
or LOC is null;

update reporting_prod.blue_yonder.export_by_HISTORY_interface_stg
set
SKU = reporting_prod.blue_yonder.udf_cleanup_field(SKU),
CC = reporting_prod.blue_yonder.udf_cleanup_field(CC),
DMDGROUP = reporting_prod.blue_yonder.udf_cleanup_field(DMDGROUP),
LOC = reporting_prod.blue_yonder.udf_cleanup_field(LOC),
HISTSTREAM = reporting_prod.blue_yonder.udf_cleanup_field(HISTSTREAM)
;
