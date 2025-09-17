use reporting_dev;
truncate table reporting_dev.blue_yonder.export_by_loc_interface_stg;

insert into reporting_dev.blue_yonder.export_by_loc_interface_stg(
	LOCATION,
	DESCR,
	TYPE,
	TOTAL_COMPANY,
	TOTAL_COMPANY_DESC,
	COMPANY,
	COMPANY_DESC,
	BUS_SEG,
	LOC_H_LVL_1,
	LOC_H_LVL_1_DSC,
	LOC_H_LVL_2,
	LOC_H_LVL_2_DSC,
	CITY,
	STATE,
	STATE_NAME,
	CHANNEL,
	CHANNEL_NAME,
	REGION,
	REGION_NAME,
	DISTRICT,
	DISTRICT_NAME,
	ADDRESS_1,
	ADDRESS_2,
	ZIP_CODE,
	COUNTRY_ID,
	COUNTRY_NAME,
	TELEPHONE,
	ACTIVE_YN,
	DATE_OPENED,
	DATE_CLOSED,
	FORMAT,
	REMODEL_DATE,
	ANNUAL_SALES,
	RETAIL_SQUARE_FEET,
	WHSE_FLAG,
	OUTLET_FLAG,
    STORE_NUMBER,
    STORE_ID,
    DMD_FLAG,
    FF_FLAG,
    ALLOC_FLAG,
    EP_FLAG,
    SS_FLAG
)
select distinct
dl.location,
left(w.name, 50),
dl.type,
dl.total_company,
dl.total_company_desc,
dl.company,
dl.company_desc,
case
    when w.warehouse_id in (154,231) and left(dl.location,1) = 'R'
    then 'RETAIL'
    when contains(UPPER(label), 'AMZ')
    then 'AMAZON'
    when w.airport_code is not null
    then 'ECOM'
end,
case
    when dl.type = 3 and left(dl.location,1) = 'E' then 'E_' || CASE WHEN a.country_code in ('UK','GB') THEN 'UK' ELSE dl.region END
    when dl.type = 3 then ' '
    when w.warehouse_id in (154,231) and left(dl.location,1) = 'R' then a.country_code
    else ' '
/*    when w.warehouse_id in (154,231)
    then 'NA'
    when contains(UPPER(label), 'AMZ')
    then 'US'
    when w.airport_code is not null
    then 'E_' || CASE WHEN a.country_code in ('UK','GB') THEN 'UK' ELSE dl.region END*/
end,
case
    when dl.type = 3 and left(dl.location,1) = 'E' then 'ECOM_' || CASE WHEN a.country_code in ('UK','GB') THEN 'UK' ELSE dl.region END
    when dl.type = 3 then ' '
    when w.warehouse_id in (154,231) and left(dl.location,1) = 'R' then a.country_code
    else ' '
 /*   when w.warehouse_id in (154,231)
    then 'NA'
    when contains(UPPER(label), 'AMZ')
    then 'US'
    when w.airport_code is not null
    then 'ECOM ' || CASE WHEN a.country_code in ('UK','GB') THEN 'UK' ELSE dl.region END*/
end,
' ',
/*case
    when dl.type = 3
    then ''
    when w.warehouse_id in (154,231)
    then a.country_code
    when contains(UPPER(label), 'AMZ') or w.airport_code is not null
    then ' '
end,*/
' ',
/*case
    when dl.type = 3
    then ''
    when w.warehouse_id in (154,231)
    then a.country_code
    when contains(UPPER(label), 'AMZ') or w.airport_code is not null
    then ' '
end,*/
left (a.city, 50),
a.state,
'' as state_name, -- back fill
dl.channel,
dl.channel_name,
dl.region,
dl.region_name,
'' as DISTRICT,
'' as DISTRICT_NAME,
left(a.address1,50),
left(a.address2,100),
a.zip,
CASE WHEN a.country_code in ('UK','GB') THEN 826 ELSE i.numeric_code END as country_id,
CASE WHEN a.country_code in ('UK','GB') THEN 'UK' ELSE i.short_name END as country_name,
a.phone as telephone,
iff(w.status_code_id = 4, 'Y', 'N') as active_YN,
'' as DATE_OPENED,
'' as DATE_CLOSED,
'' as FORMAT,
'' as REMODEL_DATE,
0 as ANNUAL_SALES,
0 as RETAIL_SQUARE_FEET,
iff(w.airport_code is not null, 'Y', 'N') WHSE_FLAG,
'N' as  OUTLET_FLAG,
dl.retail_location_code,
dl.store_id,
DMD_FLAG,
FF_FLAG,
ALLOC_FLAG,
EP_FLAG,
SS_FLAG
from lake_view.ultra_warehouse.warehouse w
left join lake_view.ultra_warehouse.address a
    on w.address_id = a.address_id
left join reporting_base_prod.public.iso_3166_1 i
    on a.country_code = i.alpha_2_code
left join reporting_base_prod.blue_yonder.dim_location dl
    on w.warehouse_id = dl.warehouse_id
where w.type_code_id in (54, 55)
and w.status_code_id = 4
and w.region_id not in (8,9) --remove consignment and retail stores
and w.airport_code is not null
and w.warehouse_id in (
                select distinct warehouse_id
                from reporting_base_prod.gsc.po_skus_data
                where show_room = to_varchar(year(current_date)) ||  iff(month(current_date) < 10, '-0', '-') || to_varchar(month(current_date))
                AND division_id IN ('FABLETICS', 'YITTY')
                )
and
dl.location is not null
/*case
    when w.warehouse_id in (154,231)
    then 'RETAIL'
    when contains(UPPER(label), 'AMZ')
    then 'AMAZON'
    when w.airport_code is not null
    then 'ECOM'
    when left(w.label, 3) in ('RTL', 'SXF')
    then 'RETAIL'
    else 'POP'
    end != 'POP'*/

union

SELECT distinct
dl.location,
left(ds.store_full_name, 50) as descr,
dl.type,
dl.total_company,
dl.total_company_desc,
dl.company,
dl.company_desc,
case when store_type in ('Online','Mobile App','Group Order') THEN 'ECOM' ELSE 'RETAIL' END,
case
    when dl.type = 3 then ' '
    when store_type in ('Online','Mobile App','Group Order')
    then 'E_' || CASE WHEN ds.store_country = 'UK' THEN 'UK' ELSE dl.region END
    when ds.store_country = 'UK' THEN 'UK'
    when ds.store_region = 'EU' then 'EU'
    else rl.country_code
end,
case
    when dl.type = 3 then ' '
    when store_type in ('Online','Mobile App','Group Order')
    then 'ECOM ' || CASE WHEN ds.store_country = 'UK' THEN 'UK' ELSE dl.region END
    when ds.store_country = 'UK' THEN 'UK'
    when ds.store_region = 'EU' THEN 'EU'
    else rl.country_code
end,
' ',
/*case
    when dl.type = 3
    then ' '
    when store_type in ('Online','Mobile App')
    then ' '
    else CASE WHEN ds.store_region = 'EU' THEN 'EU' ELSE rl.country_code END
end,*/
' ',
/*case
    when dl.type = 3
    then ' '
    when store_type in ('Online','Mobile App')
    then ' '
    else CASE WHEN ds.store_region = 'EU' THEN 'EU' ELSE rl.country_code END
end,*/
store_retail_city as city,
store_retail_state as state,
'' as state_name,
dl.channel,
dl.channel_name,
dl.region,
dl.region_name,
left(retail_region, 50) as DISTRICT,
left(retail_region, 50) as DISTRICT_NAME,
w.address1,
w.address2,
store_retail_zip_code as zip,
CASE WHEN ds.store_country = 'UK' THEN 826 ELSE i.numeric_code END as country_id,
CASE WHEN ds.store_country = 'UK' THEN 'UK' ELSE i.short_name END as country_name,
'' as telephone,
CASE WHEN ifnull(upper(ds.store_retail_status),'') like '%CLOSED%' THEN 'N' ELSE 'Y' END as active_YN,
CASE WHEN CAST(rl.open_date as varchar(15)) < '2020' THEN '' ELSE CAST(rl.open_date as varchar(15)) END as DATE_OPENED,
'' as DATE_CLOSED,
'' as FORMAT,
'' as REMODEL_DATE,
0 as ANNUAL_SALES,
rl.selling_sq_ft as RETAIL_SQUARE_FEET,
'N' as  WHSE_FLAG,
iff(upper(ds.retail_region) = 'OUTLET','Y','N'),
dl.retail_location_code,
dl.store_id,
DMD_FLAG,
FF_FLAG,
ALLOC_FLAG,
EP_FLAG,
SS_FLAG
FROM REPORTING_BASE_PROD.FABLETICS.DIM_STORE_EXTENDED ds
left join edw_prod.data_model_fl.dim_warehouse w
    on  ds.store_retail_location_code = replace(warehouse_code, 'FLRT','') and w.warehouse_id != 526
left join lake_view.ultra_warehouse.retail_location rl
    on ds.store_id = rl.store_id
left join reporting_base_prod.public.iso_3166_1 i
    on ds.store_country = i.alpha_2_code
left join reporting_base_prod.blue_yonder.dim_location dl
    on ds.store_id = dl.store_id
WHERE STORE_TYPE in ('Retail','Online','Mobile App','Group Order')
and store_brand_abbr in ('FL','YTY')
and dl.location is not null
--and w.is_active = TRUE
--and w.is_retail = TRUE

order by type
;

update reporting_dev.blue_yonder.export_by_Loc_interface_stg
set
LOCATION = reporting_prod.blue_yonder.udf_cleanup_field(LOCATION),
DESCR = reporting_prod.blue_yonder.udf_cleanup_field(DESCR),
TOTAL_COMPANY = reporting_prod.blue_yonder.udf_cleanup_field(TOTAL_COMPANY),
TOTAL_COMPANY_DESC = reporting_prod.blue_yonder.udf_cleanup_field(TOTAL_COMPANY_DESC),
COMPANY = reporting_prod.blue_yonder.udf_cleanup_field(COMPANY),
COMPANY_DESC = reporting_prod.blue_yonder.udf_cleanup_field(COMPANY_DESC),
BUS_SEG = reporting_prod.blue_yonder.udf_cleanup_field(BUS_SEG),
LOC_H_LVL_1 = reporting_prod.blue_yonder.udf_cleanup_field(LOC_H_LVL_1),
LOC_H_LVL_1_DSC = reporting_prod.blue_yonder.udf_cleanup_field(LOC_H_LVL_1_DSC),
LOC_H_LVL_2 = reporting_prod.blue_yonder.udf_cleanup_field(LOC_H_LVL_2),
LOC_H_LVL_2_DSC = reporting_prod.blue_yonder.udf_cleanup_field(LOC_H_LVL_2_DSC),
CITY = reporting_prod.blue_yonder.udf_cleanup_field(CITY),
STATE = reporting_prod.blue_yonder.udf_cleanup_field(STATE),
STATE_NAME = reporting_prod.blue_yonder.udf_cleanup_field(STATE_NAME),
CHANNEL = reporting_prod.blue_yonder.udf_cleanup_field(CHANNEL),
CHANNEL_NAME = reporting_prod.blue_yonder.udf_cleanup_field(CHANNEL_NAME),
REGION = reporting_prod.blue_yonder.udf_cleanup_field(REGION),
REGION_NAME = reporting_prod.blue_yonder.udf_cleanup_field(REGION_NAME),
DISTRICT = reporting_prod.blue_yonder.udf_cleanup_field(DISTRICT),
DISTRICT_NAME = reporting_prod.blue_yonder.udf_cleanup_field(DISTRICT_NAME),
ADDRESS_1 = reporting_prod.blue_yonder.udf_cleanup_field(ADDRESS_1),
ADDRESS_2 = reporting_prod.blue_yonder.udf_cleanup_field(ADDRESS_2),
ZIP_CODE = reporting_prod.blue_yonder.udf_cleanup_field(ZIP_CODE),
COUNTRY_NAME = reporting_prod.blue_yonder.udf_cleanup_field(COUNTRY_NAME),
TELEPHONE = reporting_prod.blue_yonder.udf_cleanup_field(TELEPHONE),
ACTIVE_YN = reporting_prod.blue_yonder.udf_cleanup_field(ACTIVE_YN),
DATE_OPENED = reporting_prod.blue_yonder.udf_cleanup_field(DATE_OPENED),
DATE_CLOSED = reporting_prod.blue_yonder.udf_cleanup_field(DATE_CLOSED),
FORMAT = reporting_prod.blue_yonder.udf_cleanup_field(FORMAT),
REMODEL_DATE = reporting_prod.blue_yonder.udf_cleanup_field(REMODEL_DATE),
WHSE_FLAG = reporting_prod.blue_yonder.udf_cleanup_field(WHSE_FLAG),
OUTLET_FLAG = reporting_prod.blue_yonder.udf_cleanup_field(OUTLET_FLAG)
;
