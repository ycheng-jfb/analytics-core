create or replace temp table _view_SM_Preorder_Expire_Date AS
select * from REPORTING_PROD.sxf.view_SM_Preorder_Expire_Date;

create or replace temp table _sm_po_detail AS
select * from REPORTING_PROD.sxf.view_sm_po_detail;

create or replace temp table _sm_plm_master AS
select distinct color_sku, size_scale, size_scale_production, collection, fabric_merch_name, fabric_plm, gender, style_size_type, style_dept_name, style_subdept_name, class, subclass, 'Y' as is_centric
from REPORTING_PROD.sxf.view_sm_centric_plm
UNION
select distinct color_sku, size_scale, size_scale_production, collection, fabric_merch_name, fabric_plm, gender, style_size_type, style_dept_name, style_subdept_name, class, subclass, 'N' as is_centric
from REPORTING_PROD.sxf.view_sm_plm
WHERE color_sku not in (select color_sku from REPORTING_PROD.sxf.view_sm_centric_plm);

create or replace temp table _PO_master_v2 AS
select * from REPORTING_PROD.sxf.view_PO_master_v2 ;

create or replace temp table _sm_reorder_first_last AS
select * from REPORTING_PROD.sxf.view_sm_reorder_first_last;

create or replace temp table _sm_color_family AS
select * from REPORTING_PROD.sxf.view_sm_color_family;

create or replace temp table _SM_Receipt_first_last AS
select * from REPORTING_PROD.sxf.view_SM_Receipt_first_last;


create or replace temp table _sm_dim_product AS
select distinct PRODUCT_SKU,SITE_COLOR,STYLE_NAME,LATEST_VIP_PRICE,LATEST_RETAIL_PRICE,IS_ACTIVE
    ,IS_FREE,LATEST_UPDATE,IMAGE_URL
from REPORTING_PROD.sxf.view_sm_dim_product;

create or replace temp table _sm_bundle AS
select * from REPORTING_PROD.sxf.view_sm_bundle;

create or replace temp table _sm_tags AS
select * from REPORTING_PROD.sxf.view_sm_tags;

create or replace temp table _sm_setsboxes AS
select * from REPORTING_PROD.sxf.view_sm_sets_boxes;

create or replace temp table _sm_release_date AS
select * from REPORTING_PROD.sxf.view_sm_release_date;

create or replace temp table _inventory_first_ready AS
select * from REPORTING_PROD.sxf.view_inventory_first_ready;

create or replace temp table _style_master_merch_final_input AS
select * from REPORTING_PROD.sxf.view_sm_merch_final_input;

create or replace temp table _first_sales_date AS
select * from REPORTING_PROD.sxf.view_first_sales_date;

create or replace temp table _savage_showroom AS
select distinct COLOR_SKU,SAVAGE_SHOWROOM
from REPORTING_PROD.sxf.view_savage_showroom;

create or replace temp table _sale_section AS
select * from REPORTING_PROD.sxf.view_sale_section;

create or replace temp table _sxf_core_basic_key AS
select * from lake_view.sharepoint.med_sxf_core_basic_key;

create or replace temp table _sxf_category_type_key AS
select * from lake_view.sharepoint.med_sxf_category_type_key;

create or replace temp table _bs as
select
    p.product_sku,
    date_trunc('day',convert_timezone('America/Los_Angeles' , fol.order_local_datetime)) ORDER_HQ_DATE,
    COALESCE(fol.ESTIMATED_LANDED_COST_local_amount,0)*COALESCE(fol.order_date_usd_conversion_rate,1) ESTIMATED_LANDED_COST,
    COALESCE(fol.reporting_landed_cost_local_amount,0)*COALESCE(fol.order_date_usd_conversion_rate,1) REPORTING_LANDED_COST,
    rank() over (partition by product_sku order by order_hq_date desc) as order_hq_latest
from EDW_PROD.DATA_MODEL_SXF.FACT_ORDER fo
JOIN EDW_PROD.DATA_MODEL_SXF.FACT_ORDER_LINE fol ON (fol.ORDER_ID=fo.ORDER_ID)
--JOIN EDW_PROD.DATA_MODEL_SXF.FACT_ORDER_LINE_PRODUCT_COST pc ON (fol.ORDER_LINE_ID = pc.ORDER_LINE_ID) – doesn't exist in NEW WH, its in FOL
JOIN EDW_PROD.DATA_MODEL_SXF.DIM_PRODUCT p ON (p.PRODUCT_ID = fol.PRODUCT_ID)
JOIN EDW_PROD.DATA_MODEL_SXF.DIM_PRODUCT_TYPE pt on (pt.PRODUCT_TYPE_KEY=fol.PRODUCT_TYPE_KEY)
JOIN EDW_PROD.DATA_MODEL_SXF.DIM_STORE st ON (st.STORE_ID = fo.STORE_ID)
JOIN EDW_PROD.DATA_MODEL_SXF.DIM_ORDER_SALES_CHANNEL osc ON (fo.order_sales_channel_key=osc.order_sales_channel_key)
JOIN EDW_PROD.DATA_MODEL_SXF.DIM_ORDER_PROCESSING_STATUS dops ON (dops.ORDER_PROCESSING_STATUS_KEY=fo.ORDER_PROCESSING_STATUS_KEY)
WHERE
    1=1
    AND st.STORE_BRAND='Savage X'
    AND st.STORE_NAME not like '%(DM)%'
    AND osc.ORDER_CLASSIFICATION_L1 = 'Product Order'
    AND pt.IS_FREE='FALSE'
    AND dops.ORDER_PROCESSING_STATUS_CODE != 2130
    and st.store_region='NA';

CREATE OR REPLACE TEMP TABLE _cc AS
select distinct collection from lake_view.sharepoint.med_sxf_core_basic_key;

create or replace temp table _reorder_brand_showroom AS
select *
from REPORTING_PROD.sxf.view_savage_showroom
unpivot (
    reorder_brand_showroom for reorder_number in (
        reorder_1_brand_showroom,
        reorder_2_brand_showroom,
        reorder_3_brand_showroom,
        reorder_4_brand_showroom,
        reorder_5_brand_showroom,
        reorder_6_brand_showroom
    )
)
order by color_sku;

CREATE OR REPLACE TEMP TABLE _style_master_stg AS
SELECT distinct
	upper(po.color_sku) as color_sku_po,
	upper(po.style_number_po) as style_number_po,
	upper(ro.last_stylename) as style_name,
	upper(dp.style_name) as site_name,
	upper(ro.last_po_color) as po_color,
	upper(dp.site_color) as site_color,
	dp.image_url,
	upper(ifnull(cf.color_family,smg.color_family)) as color_family,
	upper(ifnull(plm.class, ro.last_category)) as final_category,
	upper(ifnull(plm.subclass,ro.last_subcategory)) as final_subcategory,
        ifnull(tb.category_type,iff(lower(final_category) IN ('accessories', 'collectibles'),'N/A',
        iff(lower(final_category) IN ('bra', 'bralette') or lower(final_subcategory) like '%cami%'
            or lower(final_subcategory) IN ('bustier', 'corset', 'tank') or lower(final_subcategory) like '%top%','Top',
        iff(lower(final_category)='undie' or lower(final_subcategory) like '%legging%'
            or lower(final_subcategory) IN ('skirt', 'short', 'pant', 'garter')
            or lower(final_subcategory) like '%bottom%','Bottom',
        iff(lower(final_subcategory) like '%robe%' or lower(final_subcategory) like '%jumpsuit%'
            or lower(final_subcategory) IN ('babydoll', 'slip', 'catsuit', 'romper', 'teddy', 'playsuit', 'bodysuit', 'bodystocking', 'duster', 'chemise', 'set')
            ,'All Over','New - Merch, add to Top/Bottom Key'))))) as final_category_type,
	upper(ifnull(plm.size_scale, smg.size_scale)) as size_scale,
    upper(coalesce(ro.last_po_size_scale, plm.size_scale, smg.size_scale)) as size_scale_production,
	upper(smg.size_range) as gsheet_size_range,
	upper(case when plm.style_size_type IS NOT NULL and plm.style_size_type <> '' THEN plm.style_size_type
    when upper(ifnull(plm.size_scale, smg.size_scale)) like '%MENS%' then 'MENS'
    when upper(ifnull(plm.size_scale, smg.size_scale)) like '%ALL GENDER%' then 'YoUniversal'
    when (upper(ifnull(plm.size_scale, smg.size_scale)) not like '%STRAPLESS%' or (upper(ifnull(plm.size_scale, smg.size_scale)) like '%PLBRA STRAPLESS%'))
    and (upper(ifnull(plm.size_scale, smg.size_scale)) like '%CURVY%' or upper(ifnull(plm.size_scale, smg.size_scale)) like '%PL%')
    and (upper(ifnull(plm.size_scale, smg.size_scale)) like '%CURVY%' or upper(ifnull(plm.size_scale, smg.size_scale)) not like '%SPLIT%') then 'CURVY'
    when upper(ifnull(plm.size_scale, smg.size_scale)) like '%MISSY%' or upper(ifnull(plm.size_scale, smg.size_scale)) like '%EXTENDED SPLIT%' then 'MISSY'
    else 'MISSY' end)
    as size_range,
	upper(smi.buy_rank) as buy_rank,
	iff(po.color_sku=cb.color_sku,'CORE BASIC',iff(ifnull(plm.collection,smg.collection)=cc.collection,'CORE FASHION','FASHION')) as core_fashion,
	--ifnull(upper(t.ecat_fabric),upper(smg.fabric)) as ecat_fabric,
	--plm.fabric_plm,
	upper(coalesce(iff(plm.fabric_merch_name in ('','|','||','|||','||||',' '),NULL,plm.fabric_merch_name),iff(plm.fabric_plm in ('','|','||','|||','||||',' '),NULL,plm.fabric_plm),smg.fabric)) as fabric, -- Merchandized Fabric name based on Design tagged Fabric in PLM or Ecat Fabric from historical gsheet
	ifnull(upper(smi.persona),upper(smg.persona)) as persona,
	upper(ifnull(plm.collection,smg.collection)) as collection,
	case when upper(ifnull(plm.gender,smg.gender)) = 'FEMALE' then 'WOMEN' else upper(ifnull(plm.gender,smg.gender))  end as gender,
	ifnull(smi.planned_vip_price,smg.Vip_price) as vip_price,
	ifnull(smi.euro_vip,smg.euro_vip) as euro_vip,
	ifnull(smi.gbp_vip,smg.gbp_vip) as gbp_vip,
	ifnull(smi.msrp,smg.msrp) as msrp,
	ifnull(smi.euro_msrp,smg.euro_msrp) as euro_msrp,
	ifnull(smi.gbp_msrp,smg.gbp_msrp) as gbp_msrp,
	ro.first_showroom,
	ro.last_showroom as latest_showroom,
    ifnull(max(sxs.savage_showroom),ro.first_showroom) as savage_showroom,
	ro.first_vendor,
	ro.last_vendor as latest_vendor,
	ro.first_est_landedcost as first_est_landed_cost,
	ro.last_est_landedcost as latest_est_landed_cost,
	ro.last_qty as latest_qty,
	upper(b.promotion_name) as active_bundle_programs,
	upper(case when active_bundle_programs like '%3 FOR $30%' then 30/3
	when active_bundle_programs like '%2 FOR $44%' then 44/2
	when active_bundle_programs like '%2 FOR $39%' then 39/2
	when active_bundle_programs like '%10 FOR $35%' then 35/10
	when active_bundle_programs like '%2 FOR $34%' then 34/2
	when active_bundle_programs like '%7 FOR $35%' then 35/7
	when active_bundle_programs like '%3 FOR $20%' then 20/3
	when active_bundle_programs like '%7 FOR $37%' then 37/7
	when active_bundle_programs like '%5 FOR $35%' then 35/5
	when active_bundle_programs like '%2 FOR $20%' then 20/2
	when active_bundle_programs like '%10 FOR $30%' then 30/10 end)    as bundle_retail,
	upper(sb.sets) as active_sets,
	ifnull(upper(sb.vip_box), upper(smi.vip_box)) as vip_box,
    date_trunc('day',fs.first_sales_datetime::datetime) as first_sales_date,
	date_trunc('day',lr.first_datetime_received) as first_received_date,
	date_trunc('day',lr.latest_datetime_received) as last_received_date,
    date_trunc('day',ir.first_inventory_ready_date) as first_inventory_ready_date,
    iff(lr.first_datetime_received is null and ir.first_inventory_ready_date is null, null,
        iff(lr.first_datetime_received>=ir.first_inventory_ready_date or lr.first_datetime_received is null, date_trunc('day',ir.first_inventory_ready_date),
        iff(ir.first_inventory_ready_date>lr.first_datetime_received or ir.first_inventory_ready_date is null
            , date_trunc('day',lr.first_datetime_received), null ))) as first_inventory_occurrence_date,
    iff(first_inventory_occurrence_date is null and first_sales_date is null, null,
        iff(first_inventory_occurrence_date<=first_sales_date or first_sales_date is null,first_inventory_occurrence_date,
        iff(first_sales_date<first_inventory_occurrence_date or first_inventory_occurrence_date is null, first_sales_date,null))) as first_occurrence_date,
	datediff('day',last_received_date,current_date()) as inv_aged_days,
	datediff('month',last_received_date,current_date()) as inv_aged_months,

	case when datediff('month',last_received_date,current_date()) is null then 'Future'
			  when datediff('month',last_received_date,current_date()) between 0 and 9 then 'Current'
			  when datediff('month',last_received_date,current_date()) between 10 and 15 then 'Tier 1'
			  when datediff('month',last_received_date,current_date())  between 16 and 24 then 'Tier 2'
			  when datediff('month',last_received_date,current_date())  > 24 then 'Tier 3' else '?' end as inv_aged_status,

	date_trunc('day',rd.date_expected) as planned_site_release_date,
	--case when lower(po.po_status) like '%cancel%' then 'Cancelled'
			  --when lower(po.po_status) like '%error%' then 'Error' else 'Success' end as po_status,
	 case when inv_aged_status = 'Current' then 1
			 when inv_aged_status = 'Future' then 2
			 when inv_aged_status = 'Tier 1' then 3
			 when inv_aged_status = 'Tier 2' then 4
			 when inv_aged_status = 'Tier 3' then 5 else null end as inv_aged_rank,
	dense_rank() over (order by inv_aged_rank asc, final_category, final_subcategory, latest_qty desc) as rank,
	--sl.code as sale_section_code_current,
	--sl.label as sale_section_label_current,
	--sl.description as sale_section_description,
	--sl.calculation_method as sale_section_calculation_method,
	sl.rate as sale_section_price_current,
	--sl.date_added_latest as sale_section_date_added_latest,
	sl.date_added_first as sale_section_date_added_first,
	pm.sr_weighted_est_freight_us,
	pm.sr_weighted_est_duty_us,
	pm.sr_weighted_est_cmt_us,
	pm.sr_weighted_est_cost_us,
	pm.sr_weighted_est_primary_tariff_us,
	pm.sr_weighted_est_uk_tariff_us,
	pm.sr_weighted_est_us_tariff_us,
	pm.sr_weighted_est_landed_cost_us,
	pm.sr_weighted_est_freight_EU,
	pm.sr_weighted_est_duty_EU,
	pm.sr_weighted_est_cmt_EU,
	pm.sr_weighted_est_cost_EU,
	pm.sr_weighted_est_primary_tariff_EU,
	pm.sr_weighted_est_uk_tariff_EU,
	pm.sr_weighted_est_us_tariff_EU,
	pm.sr_weighted_est_landed_cost_EU,
	AVG(bs.ESTIMATED_LANDED_COST) as ESTIMATED_LANDED_COST,
	AVG(bs.REPORTING_LANDED_COST) as REPORTING_LANDED_COST,
    current_timestamp()::timestamp_ltz(3) as load_datetime,
    sa.bright,
    sa.heat,
    sa.color_roll_up,
    sa.color_value,
    sa.color_tone,
    sa.coverage,
    sa.fabric_grouping,
    sa.opacity,
    sa.pattern,
    date_trunc('day',pre.datetime_preorder_expires) as preorder_expires_date,
    plm.style_dept_name,
    ifnull(plm.style_subdept_name, smg.sub_department) as style_subdept_name,
    smi.launch_collection,
    plm.is_centric,
    IFF(
        po.color_sku=cb.color_sku and upper(cb.status)='DISCONTINUED CORE',
        'CORE DISCONTINUED',
        IFF(
            UPPER(tb.category)='COLLECTIBLES' OR upper(ifnull(plm.collection,smg.collection))='COLLECTIBLES',
            'COLLECTIBLES',
            IFF(
                upper(
                    IFF(
                        po.color_sku=cb.color_sku,
                        'CORE BASIC',
                        IFF(
                            ifnull(plm.collection,smg.collection)=cc.collection,
                            'CORE FASHION',
                            'FASHION'
                        )
                    )
                ) in ('CORE FASHION', 'FASHION'),
                'FASHION',
                IFF(
                    po.color_sku=cb.color_sku,
                    'CORE BASIC',
                    IFF(
                        ifnull(plm.collection,smg.collection)=cc.collection,
                        'CORE FASHION',
                        'FASHION'
                    )
                )
            )
        )
    ) as planning_reserve_type_na,
    dp.latest_vip_price as current_vip_price_on_site_na,
    dp.latest_retail_price as current_retail_price_on_site_na,
    ifnull(max(sxsr.reorder_brand_showroom), ro.last_showroom) as latest_brand_showroom
FROM _sm_po_detail as po
left join _sm_plm_master as plm on (po.color_sku = plm.color_sku)
left join _PO_master_v2 as pm on (po.color_sku=pm.color_sku)
left join _sm_reorder_first_last as ro on (ro.color_sku = po.color_sku)
left join _sm_color_family as cf on (cf.color_name = ro.last_po_color)
left join _SM_Receipt_first_last as lr on (ro.color_sku = lr.color_sku)
left join _sm_dim_product as dp on (dp.product_sku = po.color_sku)
left join _sm_bundle as b on (b.product_sku = dp.product_sku)
left join _sm_tags as t on (t.product_sku = dp.product_sku)
left join _sm_setsboxes as sb on (sb.product_sku = dp.product_sku)
left join _sm_release_date as rd on (rd.product_sku = dp.product_sku)
left join REPORTING_PROD.sxf.SM_gsheet as smg on (po.color_sku=smg.product_sku)
left join _inventory_first_ready ir on (ir.product_sku=po.color_sku)
left join _sxf_category_type_key tb on lower(tb.category)=lower(ifnull(plm.class, ro.last_category)) and lower(tb.subcategory)=lower(ifnull(plm.subclass,ro.last_subcategory))
left join _style_master_merch_final_input as smi on (smi.color_sku=po.color_sku)
left join _sxf_core_basic_key as cb on (cb.color_sku=po.color_sku)
left join _first_sales_date fs on (fs.product_sku=po.color_sku)
left join _savage_showroom sxs on (sxs.color_sku=po.color_sku)
left join REPORTING_PROD.sxf.refined_style_anatomy sa on (po.color_sku=sa.color_sku)
left join _cc as cc on (cc.collection=coalesce(plm.collection,smg.collection))
left join _sale_section as sl on (sl.product_sku=po.color_sku)
left join _bs as bs on (bs.product_sku=po.color_sku) and order_hq_latest=1
left join _view_SM_Preorder_Expire_Date pre on (pre.product_sku=po.color_sku)
left join _reorder_brand_showroom sxsr on (sxsr.color_sku=po.color_sku)
where lower(po.po_status) not like '%cancel%' and lower(po.po_status) not like '%error%'
group by smi.buy_rank,
    smi.persona,
    smi.vip_box,
    smi.planned_vip_price,
    smi.euro_vip,
    smi.gbp_vip,
    smi.msrp,
    smi.euro_msrp,
    smi.gbp_msrp,
    plm.fabric_merch_name,
    po.color_sku,
    po.style_number_po,
    ro.last_stylename,
    site_name,
    ro.last_po_color,
    dp.site_color,
    dp.image_url,
    cf.color_family,
    smg.color_family,
    final_category,
    final_subcategory,
    final_category_type,
    plm.size_scale,
    plm.size_scale_production,
    plm.style_size_type,
    t.ecat_fabric,
    plm.fabric_plm,
    plm.collection,
    plm.is_centric,
    cc.collection,
    core_fashion,
    plm.gender,
    ro.first_showroom,
    ro.last_showroom,
    ro.first_vendor,
    ro.last_vendor,
    ro.first_est_landedcost,
    ro.last_est_landedcost,
    ro.last_qty,
    active_bundle_programs,
    bundle_retail,
    sb.sets,
    sb.vip_box,
    first_sales_date,
    first_received_date,
    last_received_date,
    first_inventory_ready_date,
    first_inventory_occurrence_date,
    first_occurrence_date,
    inv_aged_days,
    inv_aged_months,
    planned_site_release_date,
    ro.last_po_size_scale,
    smg.size_range,
    smg.size_scale,
    smg.persona,
    smg.collection,

    smg.gender,
    smg.Vip_price,
    smg.euro_vip,
    smg.gbp_vip,
    smg.msrp,
    smg.euro_msrp,
    smg.gbp_msrp,
    smg.fabric,
    po_status,
    --sl.code,
    --sl.label,
    --sl.description,
    --sl.calculation_method,
    sl.rate,
    --sl.date_added_latest,
    sl.date_added_first,
    pm.sr_weighted_est_freight_us,
    pm.sr_weighted_est_duty_us,
    pm.sr_weighted_est_cmt_us,
    pm.sr_weighted_est_cost_us,
    pm.sr_weighted_est_primary_tariff_us,
    pm.sr_weighted_est_uk_tariff_us,
    pm.sr_weighted_est_us_tariff_us,
    pm.sr_weighted_est_landed_cost_us,
    pm.sr_weighted_est_freight_EU,
    pm.sr_weighted_est_duty_EU,
    pm.sr_weighted_est_cmt_EU,
    pm.sr_weighted_est_cost_EU,
    pm.sr_weighted_est_primary_tariff_EU,
    pm.sr_weighted_est_uk_tariff_EU,
    pm.sr_weighted_est_us_tariff_EU,
    pm.sr_weighted_est_landed_cost_EU,
    cb.color_sku,
    cb.status,
    sa.bright,
    sa.heat,
    sa.color_roll_up,
    sa.color_value,
    sa.color_tone,
    sa.coverage,
    sa.fabric_grouping,
    sa.opacity,
    sa.pattern,
    pre.datetime_preorder_expires,
    plm.style_dept_name,
    ifnull(plm.style_subdept_name, smg.sub_department),
    smi.launch_collection,
    tb.category,
    dp.latest_vip_price,
    dp.latest_retail_price
qualify row_number() over (partition by po.color_sku order by po.style_number_po) = 1
order by rank;

MERGE INTO work.dbo.style_master t
USING _style_master_stg s ON equal_null(t.color_sku_po, s.color_sku_po)
WHEN NOT MATCHED THEN INSERT (
    COLOR_SKU_PO, STYLE_NUMBER_PO, STYLE_NAME, SITE_NAME, PO_COLOR, SITE_COLOR, IMAGE_URL, COLOR_FAMILY, CATEGORY, SUBCATEGORY, CATEGORY_TYPE, SIZE_SCALE, SIZE_SCALE_PRODUCTION, GSHEET_SIZE_RANGE, SIZE_RANGE, BUY_RANK, CORE_FASHION, FABRIC, PERSONA, COLLECTION, GENDER, VIP_PRICE, EURO_VIP, GBP_VIP, MSRP, EURO_MSRP, GBP_MSRP, FIRST_SHOWROOM, LATEST_SHOWROOM, SAVAGE_SHOWROOM, FIRST_VENDOR, LATEST_VENDOR, FIRST_EST_LANDED_COST, LATEST_EST_LANDED_COST, LATEST_QTY, ACTIVE_BUNDLE_PROGRAMS, BUNDLE_RETAIL, ACTIVE_SETS, VIP_BOX, FIRST_SALES_DATE, FIRST_RECEIVED_DATE, LAST_RECEIVED_DATE, FIRST_INVENTORY_READY_DATE, FIRST_INVENTORY_OCCURRENCE_DATE, FIRST_OCCURRENCE_DATE, INV_AGED_DAYS, INV_AGED_MONTHS, INV_AGED_STATUS, PLANNED_SITE_RELEASE_DATE, INV_AGED_RANK, RANK, SALE_SECTION_PRICE_CURRENT, SALE_SECTION_DATE_ADDED_FIRST, SR_WEIGHTED_EST_FREIGHT_US, SR_WEIGHTED_EST_DUTY_US, SR_WEIGHTED_EST_CMT_US, SR_WEIGHTED_EST_COST_US, SR_WEIGHTED_EST_PRIMARY_TARIFF_US, SR_WEIGHTED_EST_UK_TARIFF_US, SR_WEIGHTED_EST_US_TARIFF_US, SR_WEIGHTED_EST_LANDED_COST_US, SR_WEIGHTED_EST_FREIGHT_EU, SR_WEIGHTED_EST_DUTY_EU, SR_WEIGHTED_EST_CMT_EU, SR_WEIGHTED_EST_COST_EU, SR_WEIGHTED_EST_PRIMARY_TARIFF_EU, SR_WEIGHTED_EST_UK_TARIFF_EU, SR_WEIGHTED_EST_US_TARIFF_EU, SR_WEIGHTED_EST_LANDED_COST_EU, ESTIMATED_LANDED_COST, REPORTING_LANDED_COST, LOAD_DATETIME, BRIGHT, HEAT, COLOR_ROLL_UP, COLOR_VALUE, COLOR_TONE, COVERAGE, FABRIC_GROUPING, OPACITY, PATTERN, PREORDER_EXPIRES_DATE, DEPARTMENT, SUB_DEPARTMENT, LAUNCH_COLLECTION, IS_CENTRIC, PLANNING_RESERVE_TYPE_NA, CURRENT_VIP_PRICE_ON_SITE_NA, CURRENT_RETAIL_PRICE_ON_SITE_NA, LATEST_BRAND_SHOWROOM
)
VALUES (
    COLOR_SKU_PO, STYLE_NUMBER_PO, STYLE_NAME, SITE_NAME, PO_COLOR, SITE_COLOR, IMAGE_URL, COLOR_FAMILY, final_category, final_subcategory, final_category_type, SIZE_SCALE, SIZE_SCALE_PRODUCTION, GSHEET_SIZE_RANGE, SIZE_RANGE, BUY_RANK, CORE_FASHION, FABRIC, PERSONA, COLLECTION, GENDER, VIP_PRICE, EURO_VIP, GBP_VIP, MSRP, EURO_MSRP, GBP_MSRP, FIRST_SHOWROOM, LATEST_SHOWROOM, SAVAGE_SHOWROOM, FIRST_VENDOR, LATEST_VENDOR, FIRST_EST_LANDED_COST, LATEST_EST_LANDED_COST, LATEST_QTY, ACTIVE_BUNDLE_PROGRAMS, BUNDLE_RETAIL, ACTIVE_SETS, VIP_BOX, FIRST_SALES_DATE, FIRST_RECEIVED_DATE, LAST_RECEIVED_DATE, FIRST_INVENTORY_READY_DATE, FIRST_INVENTORY_OCCURRENCE_DATE, FIRST_OCCURRENCE_DATE, INV_AGED_DAYS, INV_AGED_MONTHS, INV_AGED_STATUS, PLANNED_SITE_RELEASE_DATE, INV_AGED_RANK, RANK, SALE_SECTION_PRICE_CURRENT, SALE_SECTION_DATE_ADDED_FIRST, SR_WEIGHTED_EST_FREIGHT_US, SR_WEIGHTED_EST_DUTY_US, SR_WEIGHTED_EST_CMT_US, SR_WEIGHTED_EST_COST_US, SR_WEIGHTED_EST_PRIMARY_TARIFF_US, SR_WEIGHTED_EST_UK_TARIFF_US, SR_WEIGHTED_EST_US_TARIFF_US, SR_WEIGHTED_EST_LANDED_COST_US, SR_WEIGHTED_EST_FREIGHT_EU, SR_WEIGHTED_EST_DUTY_EU, SR_WEIGHTED_EST_CMT_EU, SR_WEIGHTED_EST_COST_EU, SR_WEIGHTED_EST_PRIMARY_TARIFF_EU, SR_WEIGHTED_EST_UK_TARIFF_EU, SR_WEIGHTED_EST_US_TARIFF_EU, SR_WEIGHTED_EST_LANDED_COST_EU, ESTIMATED_LANDED_COST, REPORTING_LANDED_COST, LOAD_DATETIME, BRIGHT, HEAT, COLOR_ROLL_UP, COLOR_VALUE, COLOR_TONE, COVERAGE, FABRIC_GROUPING, OPACITY, PATTERN, PREORDER_EXPIRES_DATE, STYLE_DEPT_NAME, STYLE_SUBDEPT_NAME, LAUNCH_COLLECTION, IS_CENTRIC, PLANNING_RESERVE_TYPE_NA, CURRENT_VIP_PRICE_ON_SITE_NA, CURRENT_RETAIL_PRICE_ON_SITE_NA, LATEST_BRAND_SHOWROOM
)
WHEN MATCHED
THEN
UPDATE SET
  t.STYLE_NUMBER_PO = s.STYLE_NUMBER_PO,
  t.STYLE_NAME = s.STYLE_NAME,
  t.SITE_NAME = s.SITE_NAME,
  t.PO_COLOR = s.PO_COLOR,
  t.SITE_COLOR = s.SITE_COLOR,
  t.IMAGE_URL = s.IMAGE_URL,
  t.COLOR_FAMILY = s.COLOR_FAMILY,
  t.CATEGORY = s.final_category,
  t.SUBCATEGORY = s.final_subcategory,
  t.CATEGORY_TYPE = s.final_category_type,
  t.SIZE_SCALE = s.SIZE_SCALE,
  t.SIZE_SCALE_PRODUCTION = s.SIZE_SCALE_PRODUCTION,
  t.GSHEET_SIZE_RANGE = s.GSHEET_SIZE_RANGE,
  t.SIZE_RANGE = s.SIZE_RANGE,
  t.BUY_RANK = s.BUY_RANK,
  t.CORE_FASHION = s.CORE_FASHION,
  t.FABRIC = s.FABRIC,
  t.PERSONA = s.PERSONA,
  t.COLLECTION = s.COLLECTION,
  t.GENDER = s.GENDER,
  t.VIP_PRICE = s.VIP_PRICE,
  t.EURO_VIP = s.EURO_VIP,
  t.GBP_VIP = s.GBP_VIP,
  t.MSRP = s.MSRP,
  t.EURO_MSRP = s.EURO_MSRP,
  t.GBP_MSRP = s.GBP_MSRP,
  t.FIRST_SHOWROOM = s.FIRST_SHOWROOM,
  t.LATEST_SHOWROOM = s.LATEST_SHOWROOM,
  t.SAVAGE_SHOWROOM = s.SAVAGE_SHOWROOM,
  t.FIRST_VENDOR = s.FIRST_VENDOR,
  t.LATEST_VENDOR = s.LATEST_VENDOR,
  t.FIRST_EST_LANDED_COST = s.FIRST_EST_LANDED_COST,
  t.LATEST_EST_LANDED_COST = s.LATEST_EST_LANDED_COST,
  t.LATEST_QTY = s.LATEST_QTY,
  t.ACTIVE_BUNDLE_PROGRAMS = s.ACTIVE_BUNDLE_PROGRAMS,
  t.BUNDLE_RETAIL = s.BUNDLE_RETAIL,
  t.ACTIVE_SETS = s.ACTIVE_SETS,
  t.VIP_BOX = s.VIP_BOX,
  t.FIRST_SALES_DATE = s.FIRST_SALES_DATE,
  t.FIRST_RECEIVED_DATE = s.FIRST_RECEIVED_DATE,
  t.LAST_RECEIVED_DATE = s.LAST_RECEIVED_DATE,
  t.FIRST_INVENTORY_READY_DATE = s.FIRST_INVENTORY_READY_DATE,
  t.FIRST_INVENTORY_OCCURRENCE_DATE = s.FIRST_INVENTORY_OCCURRENCE_DATE,
  t.FIRST_OCCURRENCE_DATE = s.FIRST_OCCURRENCE_DATE,
  t.INV_AGED_DAYS = s.INV_AGED_DAYS,
  t.INV_AGED_MONTHS = s.INV_AGED_MONTHS,
  t.INV_AGED_STATUS = s.INV_AGED_STATUS,
  t.PLANNED_SITE_RELEASE_DATE = s.PLANNED_SITE_RELEASE_DATE,
  t.INV_AGED_RANK = s.INV_AGED_RANK,
  t.RANK = s.RANK,
  t.SALE_SECTION_PRICE_CURRENT = s.SALE_SECTION_PRICE_CURRENT,
  t.SALE_SECTION_DATE_ADDED_FIRST = s.SALE_SECTION_DATE_ADDED_FIRST,
  t.SR_WEIGHTED_EST_FREIGHT_US = s.SR_WEIGHTED_EST_FREIGHT_US,
  t.SR_WEIGHTED_EST_DUTY_US = s.SR_WEIGHTED_EST_DUTY_US,
  t.SR_WEIGHTED_EST_CMT_US = s.SR_WEIGHTED_EST_CMT_US,
  t.SR_WEIGHTED_EST_COST_US = s.SR_WEIGHTED_EST_COST_US,
  t.SR_WEIGHTED_EST_PRIMARY_TARIFF_US = s.SR_WEIGHTED_EST_PRIMARY_TARIFF_US,
  t.SR_WEIGHTED_EST_UK_TARIFF_US = s.SR_WEIGHTED_EST_UK_TARIFF_US,
  t.SR_WEIGHTED_EST_US_TARIFF_US = s.SR_WEIGHTED_EST_US_TARIFF_US,
  t.SR_WEIGHTED_EST_LANDED_COST_US = s.SR_WEIGHTED_EST_LANDED_COST_US,
  t.SR_WEIGHTED_EST_FREIGHT_EU = s.SR_WEIGHTED_EST_FREIGHT_EU,
  t.SR_WEIGHTED_EST_DUTY_EU = s.SR_WEIGHTED_EST_DUTY_EU,
  t.SR_WEIGHTED_EST_CMT_EU = s.SR_WEIGHTED_EST_CMT_EU,
  t.SR_WEIGHTED_EST_COST_EU = s.SR_WEIGHTED_EST_COST_EU,
  t.SR_WEIGHTED_EST_PRIMARY_TARIFF_EU = s.SR_WEIGHTED_EST_PRIMARY_TARIFF_EU,
  t.SR_WEIGHTED_EST_UK_TARIFF_EU = s.SR_WEIGHTED_EST_UK_TARIFF_EU,
  t.SR_WEIGHTED_EST_US_TARIFF_EU = s.SR_WEIGHTED_EST_US_TARIFF_EU,
  t.SR_WEIGHTED_EST_LANDED_COST_EU = s.SR_WEIGHTED_EST_LANDED_COST_EU,
  t.ESTIMATED_LANDED_COST = s.ESTIMATED_LANDED_COST,
  t.REPORTING_LANDED_COST = s.REPORTING_LANDED_COST,
  t.LOAD_DATETIME = s.LOAD_DATETIME,
  t.BRIGHT = s.BRIGHT,
  t.HEAT = s.HEAT,
  t.COLOR_ROLL_UP = s.COLOR_ROLL_UP,
  t.COLOR_VALUE = s.COLOR_VALUE,
  t.COLOR_TONE = s.COLOR_TONE,
  t.COVERAGE = s.COVERAGE,
  t.FABRIC_GROUPING = s.FABRIC_GROUPING,
  t.OPACITY = s.OPACITY,
  t.PATTERN = s.PATTERN,
  t.PREORDER_EXPIRES_DATE = s.PREORDER_EXPIRES_DATE,
  t.DEPARTMENT = s.STYLE_DEPT_NAME,
  t.SUB_DEPARTMENT = s.STYLE_SUBDEPT_NAME,
  t.LAUNCH_COLLECTION = s.LAUNCH_COLLECTION,
  t.IS_CENTRIC = s.IS_CENTRIC,
  t.PLANNING_RESERVE_TYPE_NA = s.PLANNING_RESERVE_TYPE_NA,
  t.CURRENT_VIP_PRICE_ON_SITE_NA = s.CURRENT_VIP_PRICE_ON_SITE_NA,
  t.CURRENT_RETAIL_PRICE_ON_SITE_NA = s.CURRENT_RETAIL_PRICE_ON_SITE_NA,
  t.LATEST_BRAND_SHOWROOM = s.LATEST_BRAND_SHOWROOM;

CREATE OR REPLACE TEMP TABLE _deleted_sku AS
-- Cancelled color_skus
SELECT DISTINCT color_sku AS color_sku_po
FROM REPORTING_PROD.sxf.view_sm_po_detail
WHERE (po_status ILIKE '%cancel%'
    OR po_status ILIKE '%error%')
    --Exclude all color_skus where a single row has a valid value
    AND color_sku NOT IN (
        SELECT DISTINCT color_sku
        FROM REPORTING_PROD.sxf.view_sm_po_detail
        WHERE po_status NOT ILIKE '%cancel%'
            AND po_status NOT ILIKE '%error%'
    )

UNION ALL
-- Deleted color_skus
SELECT DISTINCT color_sku_po
FROM work.dbo.style_master
WHERE color_sku_po NOT IN (
    SELECT color_sku
    FROM REPORTING_PROD.sxf.view_sm_po_detail
    )
;

DELETE FROM work.dbo.style_master
WHERE color_sku_po IN (SELECT DISTINCT color_sku_po FROM _deleted_sku);
