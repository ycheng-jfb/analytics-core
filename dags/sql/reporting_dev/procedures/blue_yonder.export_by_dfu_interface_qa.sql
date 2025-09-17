use reporting_dev;
CREATE OR REPLACE TEMP TABLE _items AS --prefiltering for core items
select distinct PRODUCT_SKU AS CC
from REPORTING_BASE_PROD.BLUE_YONDER.DIM_ITEM
where IS_LADDER = TRUE --updated 8.26.24 to use is ladder instead of core
;

CREATE OR REPLACE TEMP TABLE _sku_stores AS
SELECT DISTINCT di.CC                                                        as DMDUNIT,
                dl.LOCATION                                                  as LOC,
                'TOT_FAB'                                                    as MARKET,
                CASE
                    WHEN startswith(dl.location, 'R') = TRUE then 'RETAIL'
                    WHEN startswith(dl.location, 'S') = TRUE then 'ECOM' END as BUS_SEGMENT,
                CASE
                    WHEN startswith(dl.location, 'R') = TRUE then 'POS'
                    WHEN startswith(dl.location, 'S') = TRUE then 'OP' END   as HIST_STREAM
from _items di
         cross join REPORTING_BASE_PROD.BLUE_YONDER.DIM_LOCATION dl
where 1 = 1
  and (startswith(dl.LOCATION, 'S') or startswith(dl.LOCATION, 'R')) --stores and virtual sales stores
  and dl.DMD_FLAG = 1                                                --Christina added 7.10.24 to check for Core demand location
  and dl.STORE_ID is not null;

--get lead only tags:
create or replace temporary table _lead_only AS
select lower(tg.label)                                as tag_name,
       p.STORE_GROUP_ID                               as store_group_id,
       CASE
        WHEN p.STORE_GROUP_ID IN (16, 29) THEN 'NA'
        WHEN p.STORE_GROUP_ID IN (22, 24, 25, 26, 27, 28) THEN 'EU'
        WHEN p.STORE_GROUP_ID = 23 THEN 'UK'
    END AS REGION,
       edw_prod.stg.udf_unconcat_brand(pt.product_id) as master_product_id
from LAKE_CONSOLIDATED.ultra_merchant_history.product_tag pt
         join LAKE_CONSOLIDATED.ultra_merchant.product p on pt.product_id = p.product_id
         join LAKE_CONSOLIDATED.ultra_merchant.tag tg on tg.tag_id = pt.tag_id
where lower(tg.label) = 'lead only'
  and pt.hvr_change_op = 1
  and pt.meta_company_id = 20
  and pt.EFFECTIVE_START_DATETIME >= '2023-07-01'
  and pt.effective_end_datetime >= current_date; -- only need currently tagged products


--filter for only active products
CREATE OR REPLACE TEMP TABLE _resolved_master_products AS
SELECT
    dp.PRODUCT_SKU,
    IFF(dp.master_product_id = -1, dp.product_id, dp.master_product_id) AS master_product_id
FROM EDW_PROD.DATA_MODEL_FL.DIM_PRODUCT dp
WHERE dp.IS_ACTIVE = TRUE;

--add lead only tag to sku location data
CREATE OR REPLACE TEMP TABLE _sku_stores_lead_only AS
SELECT
    ss.*,
    IFF(lo.master_product_id IS NOT NULL AND
        CASE
            WHEN LOC IN ('S52', 'S151', 'S79', 'S241', 'S24101') THEN 'NA'
            WHEN LOC IN ('S73', 'S77', 'S75', 'S152', 'S65', 'S154', 'S69', 'S71', 'S155') THEN 'EU'
            WHEN LOC IN ('S67', 'S153') THEN 'UK'
        END IN (SELECT REGION FROM _lead_only WHERE master_product_id = lo.master_product_id),
        TRUE, FALSE) AS LEAD_ONLY
FROM _sku_stores ss
LEFT JOIN _resolved_master_products rmp
    ON ss.DMDUNIT = rmp.PRODUCT_SKU
LEFT JOIN _lead_only lo
    ON lo.master_product_id = rmp.master_product_id;

-- if lead only for one region store_id, lead only for all of that region's store_ids
CREATE OR REPLACE TEMP TABLE _location_lead_only_group AS
SELECT
    DMDUNIT,
    LOC,
    MAX(LEAD_ONLY) OVER (
        PARTITION BY DMDUNIT,
                     CASE
                         WHEN LOC IN ('S52', 'S151', 'S79', 'S241', 'S24101') THEN 'NA'
                         WHEN LOC IN ('S73', 'S77', 'S75', 'S152', 'S65', 'S154', 'S69', 'S71', 'S155') THEN 'EU'
                         WHEN LOC IN ('S67', 'S153') THEN 'UK'
                     END
    ) AS LEAD_ONLY_GROUP
FROM _sku_stores_lead_only;

CREATE OR REPLACE TEMP TABLE _adjusted_sku_stores AS
SELECT
    ss.*,
    COALESCE(llg.LEAD_ONLY_GROUP, ss.LEAD_ONLY) AS LEAD_ONLY_FINAL
FROM _sku_stores_lead_only ss
LEFT JOIN _location_lead_only_group llg
    ON ss.DMDUNIT = llg.DMDUNIT
    AND ss.LOC = llg.LOC;

CREATE OR REPLACE TEMP TABLE _final AS
SELECT distinct
    DMDUNIT,
    'ECOM_ACT' AS DMDGROUP,
    LOC,
    MARKET,
    BUS_SEGMENT,
    HIST_STREAM,
    0 AS LEAD_ONLY
FROM _adjusted_sku_stores
WHERE STARTSWITH(LOC, 'S') = TRUE
UNION ALL
SELECT distinct
    DMDUNIT,
    'ECOM_NON' AS DMDGROUP,
    LOC,
    MARKET,
    BUS_SEGMENT,
    HIST_STREAM,
    IFF(LEAD_ONLY_FINAL,1,0) AS LEAD_ONLY
FROM _adjusted_sku_stores
WHERE STARTSWITH(LOC, 'S') = TRUE
UNION ALL
SELECT distinct
    DMDUNIT,
    'RETAIL' AS DMDGROUP,
    LOC,
    MARKET,
    BUS_SEGMENT,
    HIST_STREAM,
    0 AS LEAD_ONLY
FROM _adjusted_sku_stores
WHERE STARTSWITH(LOC, 'R') = TRUE;

truncate table reporting_dev.blue_yonder.export_by_dfu_interface_stg;

insert into reporting_dev.blue_yonder.export_by_dfu_interface_stg
(
DMDUNIT,
DMDGROUP,
LOC,
MARKET,
BUS_SEGMENT,
HIST_STREAM,
LEAD_ONLY
)
select
DMDUNIT,
DMDGROUP,
LOC,
MARKET,
BUS_SEGMENT,
HIST_STREAM,
LEAD_ONLY
from _final;

update reporting_dev.blue_yonder.export_by_DFU_interface_stg
set
DMDUNIT = reporting_prod.blue_yonder.udf_cleanup_field(DMDUNIT),
DMDGROUP = reporting_prod.blue_yonder.udf_cleanup_field(DMDGROUP),
LOC = reporting_prod.blue_yonder.udf_cleanup_field(LOC),
MARKET = reporting_prod.blue_yonder.udf_cleanup_field(MARKET),
BUS_SEGMENT = reporting_prod.blue_yonder.udf_cleanup_field(BUS_SEGMENT),
HIST_STREAM = reporting_prod.blue_yonder.udf_cleanup_field(HIST_STREAM)
;
