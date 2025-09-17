use reporting_dev;
CREATE OR REPLACE TEMP TABLE _sku_FCs AS
SELECT DISTINCT di.ITEM     as ITEM,
                dl.LOCATION as LOCATION,
                2           as REPLENTYPE
from REPORTING_BASE_PROD.BLUE_YONDER.DIM_ITEM di
         cross join REPORTING_BASE_PROD.BLUE_YONDER.DIM_LOCATION dl
where 1 = 1
  and di.IS_LADDER = TRUE --updated 8.26.24 to use is ladder instead of core
  and (startswith(dl.LOCATION, 'E') or startswith(dl.LOCATION, 'R'))
  and dl.FF_FLAG = 1 --Christina added 7.10.24 to check for Core demand location
  and dl.STORE_ID is null;

CREATE OR REPLACE TEMP TABLE _sku_stores AS
SELECT DISTINCT di.ITEM     as ITEM,
                dl.LOCATION as LOCATION,
                1           as REPLENTYPE
from REPORTING_BASE_PROD.BLUE_YONDER.DIM_ITEM di
         cross join REPORTING_BASE_PROD.BLUE_YONDER.DIM_LOCATION dl
where 1 = 1
  and di.IS_LADDER = TRUE --updated 8.26.24 to use is ladder instead of core
  and (startswith(dl.LOCATION, 'S') or startswith(dl.LOCATION, 'R'))
  and dl.FF_FLAG = 1 --Christina added 7.10.24 to check for Core demand location
  and dl.STORE_ID is not null;

CREATE OR REPLACE TEMP TABLE _sku_final AS
select *
from _sku_FCs
UNION ALL
select *
from _sku_stores;

truncate table reporting_dev.blue_yonder.export_by_sku_interface_stg;

insert into reporting_dev.blue_yonder.export_by_sku_interface_stg(
ITEM,
LOC,
REPLENTYPE
)
select
    ITEM,
    LOCATION,
    REPLENTYPE
from _sku_final;


update reporting_dev.blue_yonder.export_by_SKU_interface_stg
set
ITEM = reporting_prod.blue_yonder.udf_cleanup_field(ITEM),
LOC = reporting_prod.blue_yonder.udf_cleanup_field(LOC)
;
