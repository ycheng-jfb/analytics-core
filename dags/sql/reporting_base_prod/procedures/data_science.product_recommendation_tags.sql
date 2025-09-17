/*
 Purpose: get a list of tags for master_product_id's for cosine similarity calculation.
    Combines ubt tags, site tags and a collab tag for each master_product_id, mpid.
    The final table is REPORTING_BASE_PROD.DATA_SCIENCE.PRODUCT_RECOMMENDATION_TAGS:
    mpid, name, value, date_expected, meta_create_datetime, meta_company_id.
    Temporary merch tag tables one row per mpid which will be pivoted to get tags
*/

CREATE OR REPLACE TEMPORARY TABLE _sku_to_mpid AS
SELECT DISTINCT mpid,
                store_id,
                product_sku,
                max(is_active)      AS is_active,      --Max means true wins over false
                min(product_status) AS product_status, --Min means 'Active' wins over 'Inactive'
                master_product_last_update_datetime,
                final_mpid
FROM (SELECT DISTINCT store_id,
                      edw_prod.stg.udf_unconcat_brand(iff(dp.master_product_id = -1, dp.product_id, dp.master_product_id)) AS mpid,
                      product_sku,
                      is_active,
                      product_status,
                      master_product_last_update_datetime,
                      first_value(mpid) OVER (PARTITION BY product_sku, store_id ORDER BY is_active DESC, product_status)  AS final_mpid
      FROM edw_prod.data_model.dim_product dp)
GROUP BY mpid, store_id, product_sku, master_product_last_update_datetime, final_mpid;

CREATE OR REPLACE TEMPORARY TABLE _fl_ubt_temp AS
(WITH fl_multiple_ubt AS (SELECT DISTINCT mpid,
                                          ubt.*,
                                          -- dedup multiple records with same sku
                                          -- dedup multiple records with different current_showroom to just maximum current_showroom
                                          first_value(ubt.sku)
                                                      OVER (PARTITION BY mpid ORDER BY ubt.sku, current_showroom DESC) AS first_sku,
                                          max(current_showroom) OVER (PARTITION BY ubt.sku)                            AS max_current_showroom,
                                          listagg(DISTINCT current_showroom, ',') OVER (PARTITION BY ubt.sku )         AS list_of_all_showrooms
                          FROM lake_view.excel.fl_merch_items_ubt_hierarchy ubt
                               JOIN _sku_to_mpid dp ON dp.product_sku = ubt.sku
                          ORDER BY current_showroom ASC)
-- dedup to first row for each mpid
 SELECT *
 FROM fl_multiple_ubt
 WHERE sku = first_sku
   AND current_showroom = max_current_showroom
 ORDER BY current_showroom DESC);


CREATE OR REPLACE TEMPORARY TABLE _jfb_ubt_temp AS
(WITH jfb_multiple_ubt AS (SELECT DISTINCT mpid,
                                           ubt.*,
                                           max(latest_launch_date) OVER (PARTITION BY ubt.product_sku) AS max_ll
                           FROM reporting_prod.gfb.merch_dim_product ubt
                                    -- dedup brand and country
                                JOIN _sku_to_mpid dp ON dp.product_sku = ubt.product_sku
                                JOIN edw_prod.data_model_jfb.dim_store st ON st.store_id = dp.store_id AND
                                                                             lower(st.store_brand) =
                                                                             lower(ubt.business_unit) AND
                                                                             st.store_country = ubt.country)
    , t2               AS (SELECT *,
                                  -- to dedup multiple records
                                  first_value(ubt.product_sku)
                                              OVER (PARTITION BY mpid,business_unit,country ORDER BY ubt.product_sku, latest_launch_date DESC) AS first_sku
                           FROM jfb_multiple_ubt ubt
                           WHERE latest_launch_date = max_ll)
 SELECT *
 FROM t2
 WHERE first_sku = product_sku);


CREATE OR REPLACE TEMPORARY TABLE _sxf_ubt_temp AS
(WITH sxf_multiple_ubt AS (SELECT DISTINCT mpid,
                                           dp.product_status as dp_product_status,
                                           is_active,
                                           master_product_last_update_datetime,
                                           store_id,
                                           -- dedup multiple records with same sku
                                           first_value(ubt.color_sku_po)
                                                       OVER (PARTITION BY mpid ORDER BY dp.product_status, latest_showroom DESC, ubt.color_sku_po) AS first_sku,
                                           max(latest_showroom) OVER (PARTITION BY color_sku_po)                                                AS max_current_showroom,
                                           max(master_product_last_update_datetime)
                                               OVER (PARTITION BY color_sku_po, store_id)                                                       AS max_master_product_last_update_datetime,
                                           listagg(DISTINCT latest_showroom, ',') OVER (PARTITION BY color_sku_po)                              AS list_of_all_showrooms,
                                           ubt.*
                           FROM reporting_prod.sxf.style_master ubt
                                JOIN _sku_to_mpid dp ON dp.product_sku = ubt.color_sku_po)
-- dedup to first row for each mpid
 SELECT *
 FROM sxf_multiple_ubt
 WHERE color_sku_po = first_sku);

CREATE OR REPLACE TEMPORARY TABLE _dim_product_tags AS
    SELECT edw_prod.stg.udf_unconcat_brand(mpid) AS mpid, tag AS name, value, 'dimproduct' AS tag_type, meta_company_id
    FROM (SELECT DISTINCT iff(dp.master_product_id = -1, dp.product_id, dp.master_product_id)              AS mpid,
                          color,
                          lower(trim(regexp_replace(color, '\n', '')))                                     AS full_color,
                          iff(contains(full_color, '/'), trim(split_part(full_color, '/', 1)),
                              full_color)                                                                  AS primary_color,
                          iff(contains(full_color, '/'), trim(split_part(full_color, '/', 2)),
                              'None')                                                                      AS secondary_color,
                          iff(full_color ILIKE '%/%/%', trim(split_part(full_color, '/', 3)),
                              'None')                                                                      AS tertiary_color,
                          product_category,
                          department,
                          category,
                          subcategory,
                          class,
                          product_name,
                          to_varchar(current_showroom_date)                                                AS current_showroom_date,
                          meta_company_id
          FROM edw_prod.data_model.dim_product dp
               JOIN lake_consolidated_view.ultra_merchant.product ump
                    ON ump.product_id = dp.product_id) UNPIVOT (value FOR tag IN (current_showroom_date, full_color, primary_color, secondary_color, tertiary_color, product_category, department, category, subcategory, class, product_name));


CREATE OR REPLACE TEMPORARY TABLE _site_tags AS
    WITH _multiple
             AS (SELECT DISTINCT edw_prod.stg.udf_unconcat_brand(iff(p.master_product_id IS NULL, p.product_id, p.master_product_id)) AS mpid,
                                 tg2.label                                                                                            AS name,
                                 tg1.label                                                                                            AS value,
                                 to_varchar(to_date(p.date_expected))                                                                 AS date_expected,
                                 p.meta_company_id,
                                 pt.datetime_modified,
                                 max(pt.datetime_modified) OVER (PARTITION BY mpid, name)                                             AS max_datetime_modified
                 FROM lake_consolidated_view.ultra_merchant.product p
                      LEFT JOIN lake_consolidated_view.ultra_merchant.product_tag pt ON pt.product_id = p.product_id
                      LEFT JOIN lake_consolidated_view.ultra_merchant.tag tg1 ON pt.tag_id = tg1.tag_id
                      LEFT JOIN lake_consolidated_view.ultra_merchant.tag tg2 ON tg1.parent_tag_id = tg2.tag_id)
    SELECT mpid,
           name,
           value,
           date_expected,
           'site' AS tag_type,
           meta_company_id,
           datetime_modified,
           max_datetime_modified
    FROM _multiple
    WHERE (datetime_modified = max_datetime_modified OR (datetime_modified IS NULL AND max_datetime_modified IS NULL))
    ORDER BY mpid DESC, name;

-- similarity tags: mpid, name, value, date_expected
-- ubt + site + capsule/collab
CREATE OR REPLACE TRANSIENT TABLE reporting_base_prod.data_science.product_recommendation_tags AS
    WITH _ubt     AS (SELECT mpid, name, value, 'merch' AS tag_type, 20 AS meta_company_id
                      FROM _fl_ubt_temp UNPIVOT ( value FOR name IN ( product_segment, gender, category, class, subclass, original_showroom_month, marketing_story, item_rank, design_style_number, current_name, color, color_family, buy_timing, item_status, style_status, sku_status, inseams_construction, end_use, eco_system, fit_block, color_application, eco_style, fabric, fit_style, lined_unlined, style_parent, size_scale, size_range, factory, editorial_features, channel, style_color, tall_inseam, short_inseam, sub_brand, list_of_all_showrooms ))
                      WHERE value IS NOT NULL
                        AND value != '0'

                      UNION ALL
                      --date tags
                      SELECT mpid, name, to_varchar(value) AS value, 'merch' AS tag_type, 20 AS meta_company_id
                      FROM _fl_ubt_temp UNPIVOT ( value FOR name IN (current_showroom, go_live_date))

                      UNION ALL

                      SELECT mpid, name, value, 'merch' AS tag_type, 10 AS meta_company_id
                      FROM _jfb_ubt_temp UNPIVOT ( value FOR name IN ( department, department_detail, subcategory, style_rank, ww_wc, color, master_color, style_name, classification, current_vip_retail, heel_height, heel_type, coresb_reorder_fashion, is_plussize, size_13_15, collection, subclass, fk_attribute, fk_site_name, fk_new_season_code, marketing_capsule_name, style_type, gender, memory_foam_flag, cushioned_footbed_flag, cushioned_heel_flag, arch_support_flag, antibacterial_flag, sweat_wicking_flag, breathing_holes_flag, comfort_flag, img_model_reg, img_model_plus, product_color_from_label ))
                      WHERE value IS NOT NULL
                        AND value != '0'

                      UNION ALL

                      SELECT mpid, name, value, 'merch' AS tag_type, 30 AS meta_company_id
                      FROM _sxf_ubt_temp UNPIVOT ( value FOR name IN ( collection,style_name,category,subcategory,category_type,persona,gender,size_scale_production,size_range, core_fashion,site_color,color_family, color_roll_up, fabric_grouping, fabric, sub_department, list_of_all_showrooms ))
                      WHERE value IS NOT NULL
                        AND value != '0'

                      UNION ALL
                      --date tags
                      SELECT mpid, name, to_varchar(value) AS value,  'merch' AS tag_type, 30 AS meta_company_id
                      FROM _sxf_ubt_temp UNPIVOT ( value FOR name IN ( latest_brand_showroom, latest_showroom, savage_showroom, first_showroom ))
                      WHERE value IS NOT NULL
                        AND value != '0')

       , _unioned AS (SELECT mpid, name, value, NULL AS date_expected, tag_type, meta_company_id
                      FROM _dim_product_tags
                      UNION ALL
                      SELECT mpid, name, value, date_expected, tag_type, meta_company_id
                      FROM _site_tags
                      UNION ALL
                      SELECT mpid, name, value, NULL AS date_expected, tag_type, meta_company_id
                      FROM _ubt)
    SELECT mpid,
           name,
           value,
           max(date_expected) OVER (PARTITION BY mpid ) AS date_expected,
           tag_type,
           current_timestamp()                          AS meta_create_datetime,
           meta_company_id
    FROM _unioned;


-- tag value text for Tableau product similarity dashboard
CREATE OR REPLACE TRANSIENT TABLE reporting_prod.data_science.tableau_product_site_merch_tags AS
(SELECT t.mpid,
        (SELECT listagg(concat('*', site.name, ': ', site.value), ', ')
         FROM reporting_base_prod.data_science.product_recommendation_tags site
         WHERE site.mpid = t.mpid
           AND tag_type = 'site'
         ORDER BY site.name)    AS site_tags,
        (SELECT listagg(concat('*', merch.name, ': ', merch.value), ', ')
         FROM reporting_base_prod.data_science.product_recommendation_tags merch
         WHERE merch.mpid = t.mpid
           AND tag_type = 'merch'
         ORDER BY merch.name)   AS merch_tags,
        (SELECT listagg(concat('*', dp.name, ': ', dp.value), ', ')
         FROM reporting_base_prod.data_science.product_recommendation_tags dp
         WHERE dp.mpid = t.mpid
           AND tag_type = 'dimproduct'
         ORDER BY dp.name)      AS dim_product_tags,
        p.category,
        p.image_url,
        p.product_name,
        p.current_showroom_date AS showroom_date,
        t.date_expected
 FROM edw_prod.data_model.dim_product p
      JOIN reporting_base_prod.data_science.product_recommendation_tags t
           ON t.mpid = edw_prod.stg.udf_unconcat_brand(p.product_id)
 GROUP BY t.mpid, p.category, p.image_url, p.product_name, showroom_date, t.date_expected);
