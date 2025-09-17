CREATE OR REPLACE DYNAMIC TABLE REPORTING_PROD.SXF.VIEW_STYLE_MASTER_SIZE
    TARGET_LAG = '10 minutes'
    WAREHOUSE = DA_WH_ANALYTICS
AS (
SELECT
    po.sku as sku,
    po.size_name as size,
    sm.color_sku_po,
    sm.style_number_po,
    sm.style_name,
    sm.site_name,
    sm.po_color,
    sm.site_color,
    sm.color_family,
    sm.category,
    sm.subcategory,
    sm.category_type,
    sm.size_scale,
    sm.gsheet_size_range,
    sm.size_range,
    sm.core_fashion,
    sm.fabric,
    sm.persona,
    sm.collection,
    sm.gender,
    sm.Vip_price,
    sm.euro_vip,
    sm.gbp_vip,
    sm.msrp,
    sm.euro_msrp,
    sm.gbp_msrp,
    sm.first_showroom,
    sm.latest_showroom,
    sm.first_vendor,
    sm.latest_vendor,
    sm.savage_showroom,
    sm.first_est_landed_cost,
    sm.latest_est_landed_cost,
    --sm.latest_qty, Not accurate at the size level. Need to pull in once PO Master is complete
    sm.active_bundle_programs,
    sm.bundle_retail,
    sm.active_sets,
    sm.vip_box,
    sm.first_received_date,
    sm.last_received_date,
    sm.inv_aged_days,
    sm.inv_aged_months,
    sm.inv_aged_status,
    sm.planned_site_release_date,
    sm.inv_aged_rank,
    sm.rank,
    sm.image_url,
    sm.first_inventory_occurrence_date,
    sm.first_inventory_ready_date,
    sm.first_sales_date,
    sm.first_occurrence_date,
    sm.bright,
    sm.heat,
    sm.color_roll_up,
    sm.color_value,
    sm.color_tone,
    sm.coverage,
    sm.fabric_grouping,
    sm.opacity,
    sm.pattern,
    sm.sub_department,
    sm.department,
    sm.LATEST_BRAND_SHOWROOM
FROM REPORTING_PROD.SXF.STYLE_MASTER as sm
LEFT JOIN REPORTING_PROD.SXF.VIEW_SM_PO_DETAIL_SIZE_LEVEL as po
    on (sm.COLOR_SKU_PO = left(po.SKU,position('-',po.SKU,position('-',po.SKU)+1)-1))
    AND po.LATEST_UPDATE=1
    and po.SHOW_ROOM_LATEST=1
    )
;

ALTER DYNAMIC TABLE REPORTING_PROD.SXF.VIEW_STYLE_MASTER_SIZE REFRESH;

GRANT SELECT ON REPORTING_PROD.SXF.VIEW_STYLE_MASTER_SIZE TO ROLE __REPORTING_PROD_SXF_R;
GRANT SELECT ON REPORTING_PROD.SXF.VIEW_STYLE_MASTER_SIZE TO ROLE __REPORTING_PROD_SXF_RW;
GRANT SELECT ON REPORTING_PROD.SXF.VIEW_STYLE_MASTER_SIZE TO ROLE __REPORTING_PROD_SXF_RWC;
GRANT OWNERSHIP ON REPORTING_PROD.SXF.VIEW_STYLE_MASTER_SIZE TO ROLE SYSADMIN COPY CURRENT GRANTS;
