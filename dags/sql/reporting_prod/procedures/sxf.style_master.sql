CREATE
OR REPLACE TEMP TABLE _sm_preorder_expire_date AS
SELECT product_sku,
       datetime_preorder_expires
FROM reporting_prod.sxf.view_sm_preorder_expire_date;

CREATE
OR REPLACE TEMP TABLE _sm_po_detail AS
SELECT color_sku,
       po_color,
       style_number_po,
       category_po,
       subcategory_po,
       style_name_po,
       po_status,
       region_id,
       CASE WHEN product_status = 'Inactive' then 'Deactivated'
       WHEN product_status is null then 'Active' end as Product_Status
FROM reporting_prod.sxf.view_sm_po_detail po
LEFT JOIN reporting_prod.sxf.view_deactivated_skus ds on po.color_sku = ds.product_sku;


CREATE
OR REPLACE TEMP TABLE _sm_plm_master AS
SELECT DISTINCT color_sku,
                color_family,
                size_scale,
                size_scale_production,
                collection,
                fabric_merch_name,
                fabric_plm,
                gender,
                style_size_type,
                style_dept_name,
                style_subdept_name,
                class,
                subclass,
                'Y' AS is_centric
FROM reporting_prod.sxf.view_sm_centric_plm
UNION
SELECT DISTINCT color_sku,
                ''  AS color_family,
                size_scale,
                size_scale_production,
                collection,
                fabric_merch_name,
                fabric_plm,
                gender,
                style_size_type,
                style_dept_name,
                style_subdept_name,
                class,
                subclass,
                'N' AS is_centric
FROM reporting_prod.sxf.view_sm_plm
WHERE color_sku NOT IN (SELECT color_sku FROM reporting_prod.sxf.view_sm_centric_plm);

CREATE
OR REPLACE TEMP TABLE _po_master_v2 AS
SELECT color_sku,
       sr_weighted_est_freight_us,
       sr_weighted_est_duty_us,
       sr_weighted_est_cmt_us,
       sr_weighted_est_cost_us,
       sr_weighted_est_primary_tariff_us,
       sr_weighted_est_uk_tariff_us,
       sr_weighted_est_us_tariff_us,
       sr_weighted_est_landed_cost_us,
       sr_weighted_est_freight_eu,
       sr_weighted_est_duty_eu,
       sr_weighted_est_cmt_eu,
       sr_weighted_est_cost_eu,
       sr_weighted_est_primary_tariff_eu,
       sr_weighted_est_uk_tariff_eu,
       sr_weighted_est_us_tariff_eu,
       sr_weighted_est_landed_cost_eu
FROM reporting_prod.sxf.view_po_master_v2;

CREATE
OR REPLACE TEMP TABLE _sm_reorder_first_last AS
SELECT color_sku,
       first_showroom,
       first_stylename,
       first_vendor,
       first_category,
       first_subcategory,
       first_po_color,
       first_po_size_scale,
       first_qty,
       first_avg_cost,
       first_avg_duty,
       first_avg_freight,
       first_est_landedcost,
       last_showroom,
       last_stylename,
       last_vendor,
       last_category,
       last_subcategory,
       last_po_color,
       last_po_size_scale,
       last_qty,
       last_avg_cost,
       last_avg_duty,
       last_avg_freight,
       last_est_landedcost
FROM reporting_prod.sxf.view_sm_reorder_first_last;


CREATE
OR REPLACE TEMP TABLE _merlin_color_family AS
SELECT color_name,
       color_family
FROM reporting_prod.sxf.view_sm_color_family;


CREATE
OR REPLACE TEMP TABLE _sm_receipt_first_last AS
SELECT color_sku,
       first_datetime_received,
       latest_datetime_received
FROM reporting_prod.sxf.view_sm_receipt_first_last;

CREATE
OR REPLACE TEMP TABLE _sm_dim_product AS
SELECT DISTINCT product_sku,
                site_color,
                style_name,
                latest_vip_price,
                latest_retail_price,
                is_active,
                is_free,
                latest_update,
                image_url
FROM reporting_prod.sxf.view_sm_dim_product;

CREATE
OR REPLACE TEMP TABLE _sm_bundle AS
SELECT product_sku,
       LISTAGG(promotion_name, ' | ') WITHIN GROUP (ORDER BY promotion_name) as promotion_name
FROM reporting_prod.sxf.view_sm_bundle
group by product_sku;

CREATE
OR REPLACE TEMP TABLE _sm_tags AS
SELECT product_sku,
       ecat_fabric
FROM reporting_prod.sxf.view_sm_tags;

CREATE
OR REPLACE TEMP TABLE _sm_setsboxes AS
SELECT product_sku,
       vip_box,
       sets
FROM reporting_prod.sxf.view_sm_sets_boxes;

CREATE
OR REPLACE TEMP TABLE _sm_release_date AS
SELECT product_sku,
       date_expected
FROM reporting_prod.sxf.view_sm_release_date;

CREATE
OR REPLACE TEMP TABLE _inventory_first_ready AS
SELECT product_sku,
       first_inventory_ready_date
FROM reporting_prod.sxf.view_inventory_first_ready;

CREATE
OR REPLACE TEMP TABLE _style_master_merch_final_input AS
SELECT color_sku,
       buy_rank,
       persona,
       vip_box,
       planned_vip_price,
       launch_collection,
       meta_create_datetime,
       meta_update_datetime,
       euro_vip,
       gbp_vip,
       msrp,
       euro_msrp,
       gbp_msrp,
       us_discount,
       euro_discount,
       gbp_discount
FROM reporting_prod.sxf.view_sm_merch_final_input;

CREATE
OR REPLACE TEMP TABLE _po_detail_prices AS
SELECT DISTINCT LEFT(po.sku, POSITION('-', po.sku, POSITION('-', po.sku) + 1) - 1) AS color_sku,
    member_price,
    non_member_price,
    msrp_us
FROM reporting_prod.gsc.po_detail_dataset po
WHERE po.division_id ILIKE 'LINGERIE'
QUALIFY ROW_NUMBER() OVER (PARTITION BY color_sku ORDER BY meta_update_datetime DESC) = 1;

CREATE
OR REPLACE TEMP TABLE _first_sales_date AS
SELECT product_sku,
       first_sales_datetime
FROM reporting_prod.sxf.view_first_sales_date;

CREATE
OR REPLACE TEMP TABLE _savage_showroom AS
SELECT DISTINCT color_sku,
                savage_showroom
FROM reporting_prod.sxf.view_savage_showroom;

CREATE
OR REPLACE TEMP TABLE _sale_section AS
SELECT featured_product_location_id,
       product_id,
       product_sku,
       promo_id,
       promo_type_id,
       code,
       label,
       description,
       calculation_method,
       calculation_description,
       rate,
       date_added_latest,
       date_added_first
FROM reporting_prod.sxf.view_sale_section;

CREATE
OR REPLACE TEMP TABLE _sxf_core_basic_key AS
SELECT collection,
       color_sku,
       core_basic,
       status,
       meta_create_datetime,
       meta_update_datetime
FROM lake_view.sharepoint.med_sxf_core_basic_key;

CREATE
OR REPLACE TEMP TABLE _sxf_category_type_key AS
SELECT category,
       subcategory,
       category_type,
       meta_create_datetime,
       meta_update_datetime
FROM lake_view.sharepoint.med_sxf_category_type_key;

CREATE
OR REPLACE TEMP TABLE _bs AS
SELECT p.product_sku,
       DATE_TRUNC('day', CONVERT_TIMEZONE('America/Los_Angeles', fol.order_local_datetime)) order_hq_date,
       COALESCE(fol.estimated_landed_cost_local_amount, 0) *
       COALESCE(fol.order_date_usd_conversion_rate, 1)                                      estimated_landed_cost,
       COALESCE(fol.reporting_landed_cost_local_amount, 0) *
       COALESCE(fol.order_date_usd_conversion_rate, 1)                                      reporting_landed_cost,
       RANK()                                                                               OVER (PARTITION BY product_sku ORDER BY order_hq_date DESC) AS                order_hq_latest
FROM edw_prod.data_model_sxf.fact_order fo
         JOIN edw_prod.data_model_sxf.fact_order_line fol ON (fol.order_id = fo.order_id)
         JOIN edw_prod.data_model_sxf.dim_product p ON (p.product_id = fol.product_id)
         JOIN edw_prod.data_model_sxf.dim_product_type pt ON (pt.product_type_key = fol.product_type_key)
         JOIN edw_prod.data_model_sxf.dim_store st ON (st.store_id = fo.store_id)
         JOIN edw_prod.data_model_sxf.dim_order_sales_channel osc
              ON (fo.order_sales_channel_key = osc.order_sales_channel_key)
         JOIN edw_prod.data_model_sxf.dim_order_processing_status dops
              ON (dops.order_processing_status_key = fo.order_processing_status_key)
WHERE 1 = 1
  AND st.store_brand = 'Savage X'
  AND st.store_name NOT LIKE '%(DM)%'
  AND osc.order_classification_l1 = 'Product Order'
  AND pt.is_free = 'FALSE'
  AND dops.order_processing_status_code != 2130
  AND st.store_region = 'NA';

CREATE
OR REPLACE TEMP TABLE _cc AS
SELECT DISTINCT collection
FROM lake_view.sharepoint.med_sxf_core_basic_key;

CREATE
OR REPLACE TEMP TABLE _reorder_brand_showroom AS
SELECT color_sku,
       savage_showroom,
       reorder_number,
       reorder_brand_showroom
FROM reporting_prod.sxf.view_savage_showroom UNPIVOT (
                                                      reorder_brand_showroom FOR reorder_number IN (
        reorder_1_brand_showroom,
        reorder_2_brand_showroom,
        reorder_3_brand_showroom,
        reorder_4_brand_showroom,
        reorder_5_brand_showroom,
        reorder_6_brand_showroom
        )
    )
ORDER BY color_sku;

CREATE
OR REPLACE TEMP TABLE _style_master_stg AS
SELECT DISTINCT UPPER(po.color_sku)                                                                       AS color_sku_po,
                UPPER(po.style_number_po)                                                                 AS style_number_po,
                UPPER(ro.last_stylename)                                                                  AS style_name,
                UPPER(coalesce(dp.style_name,
                               case
                                   when ro.last_stylename like 'C %'
                                       then
                                       right(ro.last_stylename, len(ro.last_stylename) - 2)
                                   when ro.last_stylename like '% C'
                                       then left(ro.last_stylename, len(ro.last_stylename) - 2)
                                   when ro.last_stylename like '% C %'
                                       then concat(left(ro.last_stylename, (position(' C ', ro.last_stylename))), right(ro.last_stylename, (position(' C ', ro.last_stylename))+2))
                                   else ro.last_stylename
                                   end))                                                                  AS site_name,
                UPPER(ro.last_po_color)                                                                   AS po_color,
                UPPER(dp.site_color)                                                                      AS site_color,
                dp.image_url,
                UPPER(IFF(plm.color_family IS NULL OR plm.color_family = '', cf.color_family,
                          plm.color_family))                                                              AS color_family,
                UPPER(IFNULL(plm.class, ro.last_category))                                                AS actual_final_category,
                CASE WHEN IFNULL(plm.style_subdept_name, smg.sub_department) IN ('MEN\'S FOOTWEAR','WOMEN\'S FOOTWEAR') THEN 'ACCESSORIES'
                     ELSE UPPER(IFNULL(plm.class, ro.last_category))
                END AS final_category,
                UPPER(IFF(bf.bra_frame IS NULL, IFNULL(plm.subclass, ro.last_subcategory),
                          bf.bra_frame))                                                                  AS final_subcategory,
                IFNULL(tb.category_type, IFF(LOWER(final_category) IN ('accessories', 'collectibles'), 'N/A',
                                             IFF(LOWER(final_category) IN ('bra', 'bralette') OR
                                                 LOWER(final_subcategory) LIKE '%cami%'
                                                     OR LOWER(final_subcategory) IN ('bustier', 'corset', 'tank') OR
                                                 LOWER(final_subcategory) LIKE '%top%', 'Top',
                                                 IFF(LOWER(final_category) = 'undie' OR
                                                     LOWER(final_subcategory) LIKE '%legging%'
                                                         OR
                                                     LOWER(final_subcategory) IN ('skirt', 'short', 'pant', 'garter')
                                                         OR LOWER(final_subcategory) LIKE '%bottom%', 'Bottom',
                                                     IFF(LOWER(final_subcategory) LIKE '%robe%' OR
                                                         LOWER(final_subcategory) LIKE '%jumpsuit%'
                                                             OR LOWER(final_subcategory) IN
                                                                ('babydoll', 'slip', 'catsuit', 'romper', 'teddy',
                                                                 'playsuit', 'bodysuit', 'bodystocking', 'duster',
                                                                 'chemise', 'set')
                                                         , 'All Over',
                                                         'New - Merch, add to Top/Bottom Key')))))        AS final_category_type,
                UPPER(IFNULL(plm.size_scale, smg.size_scale))                                             AS size_scale,
                UPPER(COALESCE(ro.last_po_size_scale, plm.size_scale, smg.size_scale))                    AS size_scale_production,
                UPPER(smg.size_range)                                                                     AS gsheet_size_range,
               UPPER(CASE
                          WHEN IFNULL(plm.style_subdept_name, smg.sub_department) ILIKE 'MEN\'S %' THEN 'MENS'
                          WHEN UPPER(IFNULL(plm.size_scale, smg.size_scale)) LIKE '%ALL GENDER%' THEN 'YoUniversal'
                          WHEN (UPPER(IFNULL(plm.size_scale, smg.size_scale)) NOT LIKE '%STRAPLESS%' OR
                                (UPPER(IFNULL(plm.size_scale, smg.size_scale)) LIKE '%PLBRA STRAPLESS%'))
                              AND (UPPER(IFNULL(plm.size_scale, smg.size_scale)) LIKE '%CURVY%' OR
                                   UPPER(IFNULL(plm.size_scale, smg.size_scale)) LIKE '%PL%')
                              AND (UPPER(IFNULL(plm.size_scale, smg.size_scale)) LIKE '%CURVY%' OR
                                   UPPER(IFNULL(plm.size_scale, smg.size_scale)) NOT LIKE '%SPLIT%') THEN 'CURVY'
                          WHEN UPPER(IFNULL(plm.size_scale, smg.size_scale)) LIKE '%MISSY%' OR
                               UPPER(IFNULL(plm.size_scale, smg.size_scale)) LIKE '%EXTENDED SPLIT%' THEN 'MISSY'
                          ELSE 'MISSY' END)
                                                                                                          AS size_range,
                UPPER(smi.buy_rank)                                                                       AS buy_rank,
                IFF(po.color_sku = UPPER(cb.color_sku), 'CORE BASIC',
                    IFF(IFNULL(plm.collection, smg.collection) = cc.collection, 'CORE FASHION',
                        'FASHION'))                                                                       AS core_fashion,
                UPPER(ncf.new_core_fashion)                                                               AS new_core_fashion,
                UPPER(COALESCE(
                    IFF(plm.fabric_merch_name IN ('', '|', '||', '|||', '||||', ' '), NULL, plm.fabric_merch_name),
                    IFF(plm.fabric_plm IN ('', '|', '||', '|||', '||||', ' '), NULL, plm.fabric_plm),
                    smg.fabric))                                                                          AS fabric, -- Merchandized Fabric name based on Design tagged Fabric in PLM or Ecat Fabric from historical gsheet
                IFNULL(UPPER(smi.persona), UPPER(smg.persona))                                            AS persona,
                UPPER(IFNULL(plm.collection, smg.collection))                                             AS collection,
                CASE
                    WHEN UPPER(IFNULL(plm.gender, smg.gender)) = 'FEMALE' THEN 'WOMEN'
                    ELSE UPPER(IFNULL(plm.gender, smg.gender)) END                                        AS gender,
                COALESCE(IFF(
                            COALESCE(pdp.member_price,0) <> 0,
                            pdp.member_price,
                            IFF(COALESCE(smi.planned_vip_price,0) <> 0,smi.planned_vip_price, smg.vip_price))
                            ,0)                          AS vip_price,
                COALESCE(IFF(
                            COALESCE(pdp.member_price,0) <> 0,
                            'PO VIP Price',
                            IFF(COALESCE(smi.planned_vip_price,0) <> 0,'Planned VIP Price', 'GSheet VIP Price'))
                            ,NULL)                       AS vip_price_type,

                IFNULL(smi.euro_vip, smg.euro_vip)                                                        AS euro_vip,
                IFNULL(smi.gbp_vip, smg.gbp_vip)                                                          AS gbp_vip,
                COALESCE(IFF(
                            COALESCE(pdp.non_member_price,0) <> 0,
                            pdp.non_member_price,
                            IFF(COALESCE(pdp.msrp_us,0) <> 0,pdp.msrp_us,IFF(COALESCE(smi.msrp,0) <> 0,smi.msrp,smg.msrp))
                            ),
                        0)                           AS msrp,
                COALESCE(IFF(
                            COALESCE(pdp.non_member_price,0) <> 0,
                            'PO MSRP Price',
                            IFF(COALESCE(pdp.msrp_us,0) <> 0,'PO MSRP Price','GSheet MSRP Price')
                            ),
                        NULL)                        AS msrp_price_type,
                IFNULL(smi.euro_msrp, smg.euro_msrp)                                                      AS euro_msrp,
                IFNULL(smi.gbp_msrp, smg.gbp_msrp)                                                        AS gbp_msrp,
                ro.first_showroom,
                ro.last_showroom                                                                          AS latest_showroom,
                IFNULL(MAX(sxs.savage_showroom), ro.first_showroom)                                       AS savage_showroom,
                ro.first_vendor,
                ro.last_vendor                                                                            AS latest_vendor,
                ro.first_est_landedcost                                                                   AS first_est_landed_cost,
                ro.last_est_landedcost                                                                    AS latest_est_landed_cost,
                ro.last_qty                                                                               AS latest_qty,
                UPPER(b.promotion_name)                                                                   AS active_bundle_programs,
                UPPER(CASE
                          WHEN active_bundle_programs LIKE '%3 FOR $30%' THEN 30 / 3
                          WHEN active_bundle_programs LIKE '%2 FOR $44%' THEN 44 / 2
                          WHEN active_bundle_programs LIKE '%2 FOR $39%' THEN 39 / 2
                          WHEN active_bundle_programs LIKE '%10 FOR $35%' THEN 35 / 10
                          WHEN active_bundle_programs LIKE '%2 FOR $34%' THEN 34 / 2
                          WHEN active_bundle_programs LIKE '%7 FOR $35%' THEN 35 / 7
                          WHEN active_bundle_programs LIKE '%3 FOR $20%' THEN 20 / 3
                          WHEN active_bundle_programs LIKE '%7 FOR $37%' THEN 37 / 7
                          WHEN active_bundle_programs LIKE '%5 FOR $35%' THEN 35 / 5
                          WHEN active_bundle_programs LIKE '%2 FOR $20%' THEN 20 / 2
                          WHEN active_bundle_programs LIKE '%10 FOR $30%'
                              THEN 30 / 10 END)                                                           AS bundle_retail,
                UPPER(sb.sets)                                                                            AS active_sets,
                IFNULL(UPPER(sb.vip_box), UPPER(smi.vip_box))                                             AS vip_box,
                DATE_TRUNC('day', fs.first_sales_datetime::DATETIME)                                      AS first_sales_date,
                DATE_TRUNC('day', lr.first_datetime_received)                                             AS first_received_date,
                DATE_TRUNC('day', lr.latest_datetime_received)                                            AS last_received_date,
                DATE_TRUNC('day', ir.first_inventory_ready_date)                                          AS first_inventory_ready_date,
                IFF(lr.first_datetime_received IS NULL AND ir.first_inventory_ready_date IS NULL, NULL,
                    IFF(lr.first_datetime_received >= ir.first_inventory_ready_date OR
                        lr.first_datetime_received IS NULL, DATE_TRUNC('day', ir.first_inventory_ready_date),
                        IFF(ir.first_inventory_ready_date > lr.first_datetime_received OR
                            ir.first_inventory_ready_date IS NULL
                            , DATE_TRUNC('day', lr.first_datetime_received),
                            NULL)))                                                                       AS first_inventory_occurrence_date,
                IFF(first_inventory_occurrence_date IS NULL AND first_sales_date IS NULL, NULL,
                    IFF(first_inventory_occurrence_date <= first_sales_date OR first_sales_date IS NULL,
                        first_inventory_occurrence_date,
                        IFF(first_sales_date < first_inventory_occurrence_date OR
                            first_inventory_occurrence_date IS NULL, first_sales_date,
                            NULL)))                                                                       AS first_occurrence_date,
                DATEDIFF('day', last_received_date, CURRENT_DATE())                                       AS inv_aged_days,
                DATEDIFF('month', last_received_date, CURRENT_DATE())                                     AS inv_aged_months,

                CASE
                    WHEN DATEDIFF('month', last_received_date, CURRENT_DATE()) IS NULL THEN 'Future'
                    WHEN DATEDIFF('month', last_received_date, CURRENT_DATE()) BETWEEN 0 AND 9 THEN 'Current'
                    WHEN DATEDIFF('month', last_received_date, CURRENT_DATE()) BETWEEN 10 AND 15 THEN 'Tier 1'
                    WHEN DATEDIFF('month', last_received_date, CURRENT_DATE()) BETWEEN 16 AND 24 THEN 'Tier 2'
                    WHEN DATEDIFF('month', last_received_date, CURRENT_DATE()) > 24 THEN 'Tier 3'
                    ELSE '?' END                                                                          AS inv_aged_status,

                DATE_TRUNC('day', rd.date_expected)                                                       AS planned_site_release_date,
                CASE
                    WHEN inv_aged_status = 'Current' THEN 1
                    WHEN inv_aged_status = 'Future' THEN 2
                    WHEN inv_aged_status = 'Tier 1' THEN 3
                    WHEN inv_aged_status = 'Tier 2' THEN 4
                    WHEN inv_aged_status = 'Tier 3' THEN 5
                    ELSE NULL END                                                                         AS inv_aged_rank,
                DENSE_RANK()                                                                                 OVER (ORDER BY inv_aged_rank ASC, actual_final_category, final_subcategory, latest_qty DESC) AS rank, sl.rate AS sale_section_price_current,
                sl.date_added_first                                                                       AS sale_section_date_added_first,
                pm.sr_weighted_est_freight_us,
                pm.sr_weighted_est_duty_us,
                pm.sr_weighted_est_cmt_us,
                pm.sr_weighted_est_cost_us,
                pm.sr_weighted_est_primary_tariff_us,
                pm.sr_weighted_est_uk_tariff_us,
                pm.sr_weighted_est_us_tariff_us,
                pm.sr_weighted_est_landed_cost_us,
                pm.sr_weighted_est_freight_eu,
                pm.sr_weighted_est_duty_eu,
                pm.sr_weighted_est_cmt_eu,
                pm.sr_weighted_est_cost_eu,
                pm.sr_weighted_est_primary_tariff_eu,
                pm.sr_weighted_est_uk_tariff_eu,
                pm.sr_weighted_est_us_tariff_eu,
                pm.sr_weighted_est_landed_cost_eu,
                AVG(bs.estimated_landed_cost)                                                             AS estimated_landed_cost,
                AVG(bs.reporting_landed_cost)                                                             AS reporting_landed_cost,
                CURRENT_TIMESTAMP()::TIMESTAMP_LTZ(3)                                                     AS load_datetime,
                                UPPER(CASE
                          WHEN UPPER(IFF(plm.color_family IS NULL OR plm.color_family = '', cf.color_family,
                                         plm.color_family)) = 'NAVY' THEN 'BLUE'
                          WHEN UPPER(IFF(plm.color_family IS NULL OR plm.color_family = '', cf.color_family,
                                         plm.color_family)) in ('BROWN', 'BEIGE', 'NATURAL')
                              THEN 'NUDE'
                          WHEN UPPER(IFF(plm.color_family IS NULL OR plm.color_family = '', cf.color_family,
                                         plm.color_family)) = 'ANIMAL'
                              THEN 'PRINT'
                          WHEN UPPER(IFF(plm.color_family IS NULL OR plm.color_family = '', cf.color_family,
                                         plm.color_family)) = 'CLEAR' THEN 'OTHER'
                          WHEN UPPER(IFF(plm.color_family IS NULL OR plm.color_family = '', cf.color_family,
                                         plm.color_family)) = 'OFF-WHITE' THEN 'WHITE'
                          ELSE UPPER(IFF(plm.color_family IS NULL OR plm.color_family = '', cf.color_family,
                                         plm.color_family))
                    END)                                                                                  AS color_roll_up,
                coalesce(f.fabrication, f2.fabrication)                                                   AS fabric_grouping,
                DATE_TRUNC('day', pre.datetime_preorder_expires)                                          AS preorder_expires_date,
                plm.style_dept_name,
                IFNULL(plm.style_subdept_name, smg.sub_department)                                        AS actual_style_subdept_name,
                CASE WHEN actual_style_subdept_name ILIKE 'MEN\'S FOOTWEAR' THEN 'MEN\'S INTIMATES'
                     WHEN actual_style_subdept_name ILIKE 'WOMEN\'S FOOTWEAR' THEN 'WOMEN\'S INTIMATES'
                     ELSE actual_style_subdept_name
                END AS derived_style_subdept_name,
                smi.launch_collection,
                plm.is_centric,
                IFF(
                            po.color_sku = cb.color_sku AND UPPER(cb.status) = 'DISCONTINUED CORE',
                            'CORE DISCONTINUED',
                            IFF(
                                        UPPER(tb.category) = 'COLLECTIBLES' OR
                                        UPPER(IFNULL(plm.collection, smg.collection)) = 'COLLECTIBLES',
                                        'COLLECTIBLES',
                                        IFF(
                                                UPPER(
                                                    IFF(
                                                            po.color_sku = cb.color_sku,
                                                            'CORE BASIC',
                                                            IFF(
                                                                    IFNULL(plm.collection, smg.collection) =
                                                                    cc.collection,
                                                                    'CORE FASHION',
                                                                    'FASHION'
                                                                )
                                                        )
                                                    ) IN ('CORE FASHION', 'FASHION'),
                                                'FASHION',
                                                IFF(
                                                        po.color_sku = cb.color_sku,
                                                        'CORE BASIC',
                                                        IFF(
                                                                IFNULL(plm.collection, smg.collection) =
                                                                cc.collection,
                                                                'CORE FASHION',
                                                                'FASHION'
                                                            )
                                                    )
                                            )
                                )
                    )                                                                                     AS planning_reserve_type_na,
                dp.latest_vip_price                                                                       AS current_vip_price_on_site_na,
                dp.latest_retail_price                                                                    AS current_retail_price_on_site_na,
                IFNULL(MAX(sxsr.reorder_brand_showroom), ro.last_showroom)                                AS latest_brand_showroom,
                Case when (ro.last_stylename ilike '%pack%' or ro.last_stylename ilike '%set%') and ro.last_stylename not ilike '%corset%'
                and ro.FIRST_SHOWROOM >= '2024-01-01' then 'TRUE' else 'FALSE' end as is_prepack,  --- new logic for prepacked sets and packs
                po.product_status
FROM _sm_po_detail AS po
         LEFT JOIN _sm_plm_master AS plm ON (po.color_sku = plm.color_sku)
         LEFT JOIN _po_master_v2 AS pm ON (po.color_sku = pm.color_sku)
         LEFT JOIN _sm_reorder_first_last AS ro ON (ro.color_sku = po.color_sku)
         LEFT JOIN _merlin_color_family AS cf ON (cf.color_name = ro.last_po_color)
         LEFT JOIN _sm_receipt_first_last AS lr ON (ro.color_sku = lr.color_sku)
         LEFT JOIN _sm_dim_product AS dp ON (dp.product_sku = po.color_sku)
         LEFT JOIN _sm_bundle AS b ON (b.product_sku = dp.product_sku)
         LEFT JOIN _sm_tags AS t ON (t.product_sku = dp.product_sku)
         LEFT JOIN _sm_setsboxes AS sb ON (sb.product_sku = dp.product_sku)
         LEFT JOIN _sm_release_date AS rd ON (rd.product_sku = dp.product_sku)
         LEFT JOIN reporting_prod.sxf.sm_gsheet AS smg ON (po.color_sku = smg.product_sku)
         LEFT JOIN _po_detail_prices pdp ON pdp.color_sku = po.color_sku
         LEFT JOIN _inventory_first_ready ir ON (ir.product_sku = po.color_sku)
         LEFT JOIN _sxf_category_type_key tb ON LOWER(tb.category) = LOWER(IFNULL(plm.class, ro.last_category)) AND
                                                LOWER(tb.subcategory) = LOWER(IFNULL(plm.subclass, ro.last_subcategory))
         LEFT JOIN _style_master_merch_final_input AS smi ON (smi.color_sku = po.color_sku)
         LEFT JOIN _sxf_core_basic_key AS cb ON (cb.color_sku = po.color_sku)
         LEFT JOIN _first_sales_date fs ON (fs.product_sku = po.color_sku)
         LEFT JOIN _savage_showroom sxs ON (sxs.color_sku = po.color_sku)
         LEFT JOIN _cc AS cc ON (cc.collection = COALESCE(plm.collection, smg.collection))
         LEFT JOIN _sale_section AS sl ON (sl.product_sku = po.color_sku)
         LEFT JOIN _bs AS bs ON (bs.product_sku = po.color_sku) AND order_hq_latest = 1
         LEFT JOIN _sm_preorder_expire_date pre ON (pre.product_sku = po.color_sku)
         LEFT JOIN _reorder_brand_showroom sxsr ON (sxsr.color_sku = po.color_sku)
         LEFT JOIN LAKE_VIEW.SHAREPOINT.MED_SXF_CORE_FASHION ncf ON ncf.color_sku_po = UPPER(po.color_sku)
         LEFT JOIN LAKE_VIEW.SHAREPOINT.MED_SXF_BRA_FRAME_KEY bf ON dp.style_name = bf.site_name
         LEFT JOIN LAKE_VIEW.SHAREPOINT.MED_SXF_FABRICATION_KEY f    -- Fabrication pulled from Merch Inputs doc Fabrication tab
            ON f.style_number = UPPER(po.style_number_po)   -- Join to Merch Inputs Fabrication on style number first if style number is tagged in doc
         LEFT JOIN LAKE_VIEW.SHAREPOINT.MED_SXF_FABRICATION_KEY f2
            ON f2.collection = UPPER(IFNULL(plm.collection, smg.collection)) and  f.style_number is null-- Join to Merch Inputs Fabrication on collection if style num is null/style number is not tagged in doc
WHERE LOWER(po.po_status) NOT LIKE '%cancel%'
  AND LOWER(po.po_status) NOT LIKE '%error%'
GROUP BY smi.buy_rank,
         smi.persona,
         smi.vip_box,
         smi.planned_vip_price,
         pdp.member_price,
         smi.euro_vip,
         smi.gbp_vip,
         smi.msrp,
         pdp.non_member_price,
         pdp.msrp_us,
         smi.euro_msrp,
         smi.gbp_msrp,
         plm.fabric_merch_name,
         po.color_sku,
         po.style_number_po,
         ro.last_stylename,
         dp.style_name,
         ro.last_po_color,
         dp.site_color,
         dp.image_url,
         plm.color_family,
         cf.color_family,
         actual_final_category,
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
         IFF(po.color_sku = UPPER(cb.color_sku), 'CORE BASIC',
             IFF(IFNULL(plm.collection, smg.collection) = cc.collection, 'CORE FASHION',
                 'FASHION')),
         new_core_fashion,
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
         smg.vip_price,
         smg.euro_vip,
         smg.gbp_vip,
         smg.msrp,
         smg.euro_msrp,
         smg.gbp_msrp,
         smg.fabric,
         po_status,
         sl.rate,
         sl.date_added_first,
         pm.sr_weighted_est_freight_us,
         pm.sr_weighted_est_duty_us,
         pm.sr_weighted_est_cmt_us,
         pm.sr_weighted_est_cost_us,
         pm.sr_weighted_est_primary_tariff_us,
         pm.sr_weighted_est_uk_tariff_us,
         pm.sr_weighted_est_us_tariff_us,
         pm.sr_weighted_est_landed_cost_us,
         pm.sr_weighted_est_freight_eu,
         pm.sr_weighted_est_duty_eu,
         pm.sr_weighted_est_cmt_eu,
         pm.sr_weighted_est_cost_eu,
         pm.sr_weighted_est_primary_tariff_eu,
         pm.sr_weighted_est_uk_tariff_eu,
         pm.sr_weighted_est_us_tariff_eu,
         pm.sr_weighted_est_landed_cost_eu,
         cb.color_sku,
         cb.status,
         f.fabrication,
         f2.fabrication,
         pre.datetime_preorder_expires,
         plm.style_dept_name,
         actual_style_subdept_name,
         derived_style_subdept_name,
         smi.launch_collection,
         tb.category,
         dp.latest_vip_price,
         is_prepack,
         vip_price_type,
         msrp_price_type,
         po.product_status,
         dp.latest_retail_price QUALIFY ROW_NUMBER() OVER (PARTITION BY po.color_sku ORDER BY po.style_number_po) = 1
ORDER BY rank;

MERGE INTO reporting_prod.sxf.style_master  t
    USING _style_master_stg s ON EQUAL_NULL(t.color_sku_po, s.color_sku_po)
    WHEN NOT MATCHED THEN INSERT (
                                  color_sku_po, style_number_po, style_name, site_name, po_color, site_color, image_url,
                                  color_family, centric_category, category, subcategory, category_type, size_scale, size_scale_production,
                                  gsheet_size_range, size_range, buy_rank, core_fashion, new_core_fashion, fabric,
                                  persona, collection,
                                  gender, vip_price, euro_vip, gbp_vip, msrp, euro_msrp, gbp_msrp, first_showroom,
                                  latest_showroom, savage_showroom, first_vendor, latest_vendor, first_est_landed_cost,
                                  latest_est_landed_cost, latest_qty, active_bundle_programs, bundle_retail,
                                  active_sets, vip_box, first_sales_date, first_received_date, last_received_date,
                                  first_inventory_ready_date, first_inventory_occurrence_date, first_occurrence_date,
                                  inv_aged_days, inv_aged_months, inv_aged_status, planned_site_release_date,
                                  inv_aged_rank, rank, sale_section_price_current, sale_section_date_added_first,
                                  sr_weighted_est_freight_us, sr_weighted_est_duty_us, sr_weighted_est_cmt_us,
                                  sr_weighted_est_cost_us, sr_weighted_est_primary_tariff_us,
                                  sr_weighted_est_uk_tariff_us, sr_weighted_est_us_tariff_us,
                                  sr_weighted_est_landed_cost_us, sr_weighted_est_freight_eu, sr_weighted_est_duty_eu,
                                  sr_weighted_est_cmt_eu, sr_weighted_est_cost_eu, sr_weighted_est_primary_tariff_eu,
                                  sr_weighted_est_uk_tariff_eu, sr_weighted_est_us_tariff_eu,
                                  sr_weighted_est_landed_cost_eu, estimated_landed_cost, reporting_landed_cost,
                                  load_datetime, color_roll_up, fabric_grouping, preorder_expires_date, department,centric_sub_department, sub_department,
                                  launch_collection, is_centric, planning_reserve_type_na, current_vip_price_on_site_na,
                                  current_retail_price_on_site_na, latest_brand_showroom, is_prepack,vip_price_type, msrp_price_type,   product_status
        )
        VALUES (color_sku_po, style_number_po, style_name, site_name, po_color, site_color, image_url, color_family,
                actual_final_category, final_category, final_subcategory, final_category_type, size_scale, size_scale_production,
                gsheet_size_range, size_range, buy_rank, core_fashion, new_core_fashion, fabric, persona, collection,
                gender, vip_price,
                euro_vip, gbp_vip, msrp, euro_msrp, gbp_msrp, first_showroom, latest_showroom, savage_showroom,
                first_vendor, latest_vendor, first_est_landed_cost, latest_est_landed_cost, latest_qty,
                active_bundle_programs, bundle_retail, active_sets, vip_box, first_sales_date, first_received_date,
                last_received_date, first_inventory_ready_date, first_inventory_occurrence_date, first_occurrence_date,
                inv_aged_days, inv_aged_months, inv_aged_status, planned_site_release_date, inv_aged_rank, rank,
                sale_section_price_current, sale_section_date_added_first, sr_weighted_est_freight_us,
                sr_weighted_est_duty_us, sr_weighted_est_cmt_us, sr_weighted_est_cost_us,
                sr_weighted_est_primary_tariff_us, sr_weighted_est_uk_tariff_us, sr_weighted_est_us_tariff_us,
                sr_weighted_est_landed_cost_us, sr_weighted_est_freight_eu, sr_weighted_est_duty_eu,
                sr_weighted_est_cmt_eu, sr_weighted_est_cost_eu, sr_weighted_est_primary_tariff_eu,
                sr_weighted_est_uk_tariff_eu, sr_weighted_est_us_tariff_eu, sr_weighted_est_landed_cost_eu,
                estimated_landed_cost, reporting_landed_cost, load_datetime, color_roll_up,  fabric_grouping, preorder_expires_date, style_dept_name,
                actual_style_subdept_name, derived_style_subdept_name, launch_collection, is_centric, planning_reserve_type_na,
                current_vip_price_on_site_na, current_retail_price_on_site_na, latest_brand_showroom, is_prepack,vip_price_type, msrp_price_type,   product_status)
    WHEN MATCHED
        THEN
        UPDATE SET
            t.style_number_po = s.style_number_po,
            t.style_name = s.style_name,
            t.site_name = s.site_name,
            t.po_color = s.po_color,
            t.site_color = s.site_color,
            t.image_url = s.image_url,
            t.color_family = s.color_family,
            t.centric_category = s.actual_final_category,
            t.category = s.final_category,
            t.subcategory = s.final_subcategory,
            t.category_type = s.final_category_type,
            t.size_scale = s.size_scale,
            t.size_scale_production = s.size_scale_production,
            t.gsheet_size_range = s.gsheet_size_range,
            t.size_range = s.size_range,
            t.buy_rank = s.buy_rank,
            t.core_fashion = s.core_fashion,
            t.new_core_fashion = s.new_core_fashion,
            t.fabric = s.fabric,
            t.persona = s.persona,
            t.collection = s.collection,
            t.gender = s.gender,
            t.vip_price = s.vip_price,
            t.euro_vip = s.euro_vip,
            t.gbp_vip = s.gbp_vip,
            t.msrp = s.msrp,
            t.euro_msrp = s.euro_msrp,
            t.gbp_msrp = s.gbp_msrp,
            t.first_showroom = s.first_showroom,
            t.latest_showroom = s.latest_showroom,
            t.savage_showroom = s.savage_showroom,
            t.first_vendor = s.first_vendor,
            t.latest_vendor = s.latest_vendor,
            t.first_est_landed_cost = s.first_est_landed_cost,
            t.latest_est_landed_cost = s.latest_est_landed_cost,
            t.latest_qty = s.latest_qty,
            t.active_bundle_programs = s.active_bundle_programs,
            t.bundle_retail = s.bundle_retail,
            t.active_sets = s.active_sets,
            t.vip_box = s.vip_box,
            t.first_sales_date = s.first_sales_date,
            t.first_received_date = s.first_received_date,
            t.last_received_date = s.last_received_date,
            t.first_inventory_ready_date = s.first_inventory_ready_date,
            t.first_inventory_occurrence_date = s.first_inventory_occurrence_date,
            t.first_occurrence_date = s.first_occurrence_date,
            t.inv_aged_days = s.inv_aged_days,
            t.inv_aged_months = s.inv_aged_months,
            t.inv_aged_status = s.inv_aged_status,
            t.planned_site_release_date = s.planned_site_release_date,
            t.inv_aged_rank = s.inv_aged_rank,
            t.rank = s.rank,
            t.sale_section_price_current = s.sale_section_price_current,
            t.sale_section_date_added_first = s.sale_section_date_added_first,
            t.sr_weighted_est_freight_us = s.sr_weighted_est_freight_us,
            t.sr_weighted_est_duty_us = s.sr_weighted_est_duty_us,
            t.sr_weighted_est_cmt_us = s.sr_weighted_est_cmt_us,
            t.sr_weighted_est_cost_us = s.sr_weighted_est_cost_us,
            t.sr_weighted_est_primary_tariff_us = s.sr_weighted_est_primary_tariff_us,
            t.sr_weighted_est_uk_tariff_us = s.sr_weighted_est_uk_tariff_us,
            t.sr_weighted_est_us_tariff_us = s.sr_weighted_est_us_tariff_us,
            t.sr_weighted_est_landed_cost_us = s.sr_weighted_est_landed_cost_us,
            t.sr_weighted_est_freight_eu = s.sr_weighted_est_freight_eu,
            t.sr_weighted_est_duty_eu = s.sr_weighted_est_duty_eu,
            t.sr_weighted_est_cmt_eu = s.sr_weighted_est_cmt_eu,
            t.sr_weighted_est_cost_eu = s.sr_weighted_est_cost_eu,
            t.sr_weighted_est_primary_tariff_eu = s.sr_weighted_est_primary_tariff_eu,
            t.sr_weighted_est_uk_tariff_eu = s.sr_weighted_est_uk_tariff_eu,
            t.sr_weighted_est_us_tariff_eu = s.sr_weighted_est_us_tariff_eu,
            t.sr_weighted_est_landed_cost_eu = s.sr_weighted_est_landed_cost_eu,
            t.estimated_landed_cost = s.estimated_landed_cost,
            t.reporting_landed_cost = s.reporting_landed_cost,
            t.load_datetime = s.load_datetime,
            t.color_roll_up = s.color_roll_up,
            t.fabric_grouping = s.fabric_grouping,
            t.preorder_expires_date = s.preorder_expires_date,
            t.department = s.style_dept_name,
            t.centric_sub_department = s.actual_style_subdept_name,
            t.sub_department = s.derived_style_subdept_name,
            t.launch_collection = s.launch_collection,
            t.is_centric = s.is_centric,
            t.planning_reserve_type_na = s.planning_reserve_type_na,
            t.current_vip_price_on_site_na = s.current_vip_price_on_site_na,
            t.current_retail_price_on_site_na = s.current_retail_price_on_site_na,
            t.latest_brand_showroom = s.latest_brand_showroom,
            t.is_prepack = s.is_prepack,
            t.vip_price_type = s.vip_price_type,
            t.msrp_price_type = s.msrp_price_type,
            t.product_status = s.product_status;

CREATE
OR REPLACE TEMP TABLE _deleted_sku AS
-- Cancelled color_skus
SELECT DISTINCT color_sku AS color_sku_po
FROM reporting_prod.sxf.view_sm_po_detail
WHERE (po_status ILIKE '%cancel%'
    OR po_status ILIKE '%error%')
  --Exclude all color_skus where a single row has a valid value
  AND color_sku NOT IN (SELECT DISTINCT color_sku
                        FROM reporting_prod.sxf.view_sm_po_detail
                        WHERE po_status
    NOT ILIKE '%cancel%'
  AND po_status NOT ILIKE '%error%')

UNION ALL
-- Deleted color_skus
SELECT DISTINCT color_sku_po
FROM reporting_prod.sxf.style_master
WHERE color_sku_po NOT IN (SELECT color_sku
                           FROM reporting_prod.sxf.view_sm_po_detail)
;

DELETE
FROM reporting_prod.sxf.style_master
WHERE color_sku_po IN (SELECT DISTINCT color_sku_po FROM _deleted_sku);
