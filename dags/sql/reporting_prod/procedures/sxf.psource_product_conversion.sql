CREATE OR REPLACE TRANSIENT TABLE Reporting_prod.SXF.PSOURCE_PRODUCT_CONVERSION AS
(

WITH store AS (SELECT *
               FROM edw_prod.data_model_sxf.dim_store
               WHERE store_brand = 'Savage X'
                 AND store_type = 'Online'
				 AND is_core_store=TRUE)

   , dp1 AS (SELECT DISTINCT master_product_id
                          , product_sku
                          , product_category
                          , department
                          , product_name
                          , product_type
                          , ROW_NUMBER() OVER (PARTITION BY master_product_id ORDER BY product_sku) AS rn
            FROM edw_prod.data_model_sxf.dim_product d
                     JOIN store ON d.store_id = store.store_id
            WHERE master_product_id <> -1
              AND product_id_object_type <> 'No Size-Master Product ID'
              AND product_sku <> 'Unknown'
            QUALIFY ROW_NUMBER() OVER (PARTITION BY master_product_id ORDER BY product_sku) = 1
            UNION
            SELECT DISTINCT product_id                                                              AS master_product_id
                          , product_sku
                          , product_category
                          , department
                          , product_name
                          , product_type
                          , ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY product_sku) AS rn
            FROM edw_prod.data_model_sxf.dim_product dp
                     JOIN store ON dp.store_id = store.store_id
            WHERE product_id_object_type = 'No Size-Master Product ID'
                 AND (product_sku = SPLIT_PART(product_alias, ' ', 1) or product_alias not ilike ('do not use%'))
            QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY product_sku) = 1
)
   , bundle AS (SELECT DISTINCT product_id   AS bundle_product_id,
                                product_name AS bundle_name,
								product_alias AS bundle_alias
                FROM edw_prod.data_model_sxf.dim_product
                WHERE product_type = 'Bundle')
   , hv AS (SELECT DISTINCT properties_product_id, properties_has_video
            FROM lake.segment_sxf.javascript_sxf_product_viewed
            WHERE properties_has_video = TRUE)
   -- product tags list
   , pt AS (select product_id, label
   from lake_sxf_view.ultra_merchant.product_tag pt2
   left join lake_sxf_view.ultra_merchant.tag t on pt2.tag_id=t.tag_id
   join store on t.store_group_id=store.store_group_id
   )
   , ptl as (select product_id, left(listagg(distinct label, '|') WITHIN GROUP (order by label), 1000) as tag_label
   from pt
   group by 1)
   , fp AS (SELECT fpl.label, fp.*, dl.datetime_added AS expiration_date, dd.full_date, fpl.meta_original_featured_product_location_id as fpl_id
            FROM lake_consolidated.ULTRA_MERCHANT.featured_product_location fpl
                     LEFT JOIN lake_consolidated.ULTRA_MERCHANT.featured_product fp
                               ON fpl.featured_product_location_id = fp.featured_product_location_id and fp.meta_company_id=30
                     LEFT JOIN lake_sxf_view.ULTRA_MERCHANT.featured_product_delete_log dl
                               ON fp.meta_original_featured_product_id = dl.featured_product_id
                     LEFT JOIN EDW_PROD.DATA_MODEL_SXF.dim_date dd
                               ON dd.full_date BETWEEN fp.datetime_added::DATE AND COALESCE(dl.datetime_added, SYSDATE())
                     JOIN store ON fpl.store_group_id = store.store_group_id
            WHERE dd.full_date::DATE >= DATEADD(MONTHS, -5, SYSDATE())::DATE
   and fpl.meta_company_id=30)
      , l AS (SELECT edw_prod.stg.udf_unconcat_brand(product_id) as product_id,
                  full_date,
                  LEFT(LISTAGG(DISTINCT fpl_id, '|')
                               WITHIN GROUP (ORDER BY fpl_id), 1000) AS fpl_id_list,
                  LEFT(LISTAGG(DISTINCT label, '|') WITHIN GROUP (ORDER BY label), 1000)   AS fpl_label_list
           FROM fp
           GROUP BY 1, 2)
         , cfp AS (SELECT fpl.label, fp.*
            FROM lake_sxf_view.ultra_merchant.featured_product_location fpl
                     LEFT JOIN lake_sxf_view.ultra_merchant.featured_product fp
                               ON fpl.featured_product_location_id = fp.featured_product_location_id
                     JOIN store ON fpl.store_group_id = store.store_group_id)
   , cl AS (SELECT product_id,
                  LEFT(LISTAGG(DISTINCT featured_product_location_id, '|')
                               WITHIN GROUP (ORDER BY featured_product_location_id), 1000) AS fpl_id_list_current,
                  LEFT(LISTAGG(DISTINCT label, '|') WITHIN GROUP (ORDER BY label), 1000)   AS fpl_label_list_current
           FROM cfp
           GROUP BY 1)
   ,pc AS (SELECT l1_categorization, l2_categorization, ideal_grouping, psource
            FROM lake_view.sharepoint.psource_categorization
            WHERE brand = 'Savage X'
                QUALIFY ROW_NUMBER() OVER (PARTITION BY LOWER(psource) ORDER BY meta_row_hash) = 1)
,reviews_base as (
                SELECT IFF(dp.master_product_id = -1, dp.product_id, dp.master_product_id)       AS mpid,
                       r.review_id,
                       r.datetime_added::DATE                                                    AS date,
                       r.rating,
                       r.recommended,
                       MAX(CASE WHEN rtf.review_template_field_group_id = 5 THEN rtfa.score END) AS overall_score,
                       MAX(CASE WHEN rtf.review_template_field_group_id = 2 THEN rtfa.score END) AS comfort_score,
                       MAX(CASE WHEN rtf.review_template_field_group_id = 1 THEN rtfa.score END) AS style_score,
                       MAX(CASE
                           WHEN rtf.review_template_field_group_id = 4 AND rtf.review_template_field_id != 633
                           THEN rtfa.score END)                                                  AS fit_score
                FROM edw_prod.data_model_sxf.dim_product dp
                     LEFT JOIN edw_prod.data_model_sxf.dim_store st ON (dp.store_id = st.store_id)
                     LEFT JOIN lake_sxf_view.ultra_merchant.review r ON (r.product_id = dp.product_id)
                     LEFT JOIN lake_sxf_view.ultra_merchant.review_template rt
                               ON (r.review_template_id = rt.review_template_id)
                     LEFT JOIN lake_sxf_view.ultra_merchant.review_field_answer rfa ON (r.review_id = rfa.review_id)
                     LEFT JOIN lake_sxf_view.ultra_merchant.review_template_field_answer rtfa
                               ON (rfa.review_template_field_answer_id = rtfa.review_template_field_answer_id)
                     LEFT JOIN lake_sxf_view.ultra_merchant.review_template_field rtf
                               ON (rfa.review_template_field_id = rtf.review_template_field_id)
                WHERE st.store_name NOT LIKE '%SWAG%'
	              AND st.store_name NOT LIKE '%DM%'
	              AND st.store_name NOT LIKE '%Sample Request%'
	              AND st.store_name NOT LIKE '%Heels%'
	              AND st.store_name NOT LIKE '%Wholesale%'
	              AND st.store_name NOT LIKE '%Retail Replen%'
	              AND st.store_brand NOT IN ('Not Applicable', 'Unknown')
	              AND st.store_division = 'Savage X'
	              AND st.store_region IN ('NA', 'EU')
	              AND r.review_id IS NOT NULL
                GROUP BY 1, 2, 3, 4, 5
)
, reviews as (SELECT mpid, full_date,
       count(DISTINCT review_id) as review_count,
       count_if(recommended=1) as recommended_count,
       avg(rating) as avg_rating,
       avg(overall_score) as avg_overall_score,
       avg(comfort_score) as avg_comfort_score,
       avg(style_score) as avg_style_score,
       avg(fit_score) as avg_fit_rating
from reviews_base r
left join edw_prod.data_model.dim_date dd on dd.full_date>=r.date
where dd.full_date::DATE BETWEEN DATEADD(MONTHS, -5, SYSDATE())::DATE  AND DATEADD(DAYS, -1, SYSDATE())::DATE
group by 1, 2)
, mm AS (SELECT DISTINCT properties_product_id, properties_has_member_models
            FROM lake.segment_sxf.javascript_sxf_product_viewed
            WHERE properties_has_member_models = TRUE)
SELECT pp.session_start_date,
       PP.psource,
       PP.country,
       PP.product_id,
       PP.is_bundle,
       PP.bundle_product_id,
       PP.is_activating,
       PP.registration_session,
       PP.membership_status,
       user_status_initial,
       PP.refresh_datetime,
       PP.max_received,
       dp1.product_sku,
       dp1.product_category as site_product_category,
       dp1.department as site_department,
       IFNULL(bundle.bundle_name,'') as bundle_name,
       IFNULL(bundle.bundle_alias, '') as bundle_alias,
       COALESCE(IFF(cl.fpl_id_list_current ilike '%13506%', FALSE, TRUE), FALSE)           AS hide_from_grid,
       COALESCE(IFF(hv.properties_product_id IS NULL, FALSE, TRUE), FALSE) AS pdp_has_video,
       ptl.tag_label,
       IFF(ptl.tag_label ilike '%lead only%', TRUE, FALSE)as lead_only_tag,
       IFF(ptl.tag_label ilike '%show in fpl only%', TRUE, FALSE) as fpl_only_tag,
       IFF(ptl.tag_label ilike '%vip only%', TRUE, FALSE) as vip_only_tag,
       --sm.*,
       l.fpl_id_list,
       l.fpl_label_list,
       cl.fpl_label_list_current,
       cl.fpl_id_list_current,
       pc.l1_categorization,
       pc.l2_categorization,
       pc.ideal_grouping,
       r.review_count,
       r.recommended_count,
       r.avg_rating,
       r.avg_overall_score,
       r.avg_comfort_score,
       r.avg_style_score,
       avg_fit_rating,
         category_type,
         collection,
         color_family,
         color_roll_up,
         color_sku_po,
         core_fashion,
         new_core_fashion,
         sm.department,
         fabric,
         fabric_grouping,
         first_showroom,
         image_url,
         launch_collection,
         savage_showroom,
         site_color,
         site_name,
         size_range,
         size_scale,
         size_scale_production,
         subcategory,
         sub_department,
         vip_box,
       sm.category,
       sm.style_name,
       sm.preorder_expires_date,
       sm.msrp,
       sm.vip_price,
       coalesce(IFF(mm.properties_product_id IS NULL, FALSE, TRUE), FALSE) as pdp_has_member_models,
       SUM(PP.list_view_total) AS LIST_VIEW_TOTAL,
       SUM(pdp_view_total) AS PDP_VIEW_TOTAL,
       SUM(PP.pa_total) AS PA_TOTAL,
       SUM(oc_total) AS OC_TOTAL,
       SUM(ss_pdp_view) AS SS_PDP_VIEW,
       SUM(ss_product_added) AS SS_PRODUCT_ADDED,
       SUM(ss_product_ordered) AS SS_PRODUCT_ORDERED
FROM REPORTING_PROD.sxf.pcvr_product_psource pp
         LEFT JOIN dp1 ON pp.product_id::VARCHAR = dp1.master_product_id::varchar
         LEFT JOIN bundle ON pp.bundle_product_id::VARCHAR = bundle.bundle_product_id::VARCHAR
         LEFT JOIN hv ON pp.product_id::VARCHAR = hv.properties_product_id::VARCHAR
		 LEFT JOIN REPORTING_Prod.sxf.style_master sm ON dp1.product_sku = sm.color_sku_po
         LEFT JOIN pc ON LOWER(pp.psource) = LOWER(pc.psource)
         LEFT JOIN l ON pp.session_start_date::DATE = l.full_date::DATE AND
                        TRY_TO_NUMBER(pp.product_id) = TRY_TO_NUMBER(l.product_id)
		LEFT JOIN cl ON TRY_TO_NUMBER(pp.product_id) = TRY_TO_NUMBER(cl.product_id)
		left join ptl on TRY_TO_NUMBER(pp.product_id) = try_to_number(ptl.product_id)
LEFT JOIN reviews r on pp.product_id=r.mpid and pp.session_start_date=r.full_date
LEFT JOIN mm on pp.product_id::VARCHAR = mm.properties_product_id::VARCHAR
WHERE session_start_date::DATE BETWEEN DATEADD(MONTHS, -4, SYSDATE())::DATE AND DATEADD(DAYS, -1, SYSDATE())::DATE
and product_name not ilike '%membership%' and product_type not ilike '%bundle%' and product_name not ilike '%insert%' and product_type not ilike '%insert%'
GROUP BY
session_start_date,
         pp.psource,
         country,
         pp.product_id,
         is_bundle,
         pp.bundle_product_id,
         is_activating,
         registration_session,
         membership_status,
         user_status_initial,
         refresh_datetime,
         max_received,
         product_sku,
         site_product_category,
         site_department,
         bundle_name,
         bundle_alias,
         hide_from_grid,
         pdp_has_video,
         tag_label,
         lead_only_tag,
         fpl_only_tag,
         vip_only_tag,
         fpl_id_list,
         fpl_label_list,
         fpl_label_list_current,
         fpl_id_list_current,
         l1_categorization,
         l2_categorization,
         ideal_grouping,
         review_count,
         recommended_count,
         avg_rating,
         avg_overall_score,
         avg_comfort_score,
         avg_style_score,
         avg_fit_rating,
         pdp_has_member_models,
         category_type,
         category,
         sm.style_name,
         collection,
         color_family,
         color_roll_up,
         color_sku_po,
         core_fashion,
         new_core_fashion,
         sm.department,
         fabric,
         fabric_grouping,
         first_showroom,
         image_url,
         launch_collection,
         persona,
         preorder_expires_date,
         savage_showroom,
         site_color,
         site_name,
         size_range,
         size_scale,
         size_scale_production,
         subcategory,
         sub_department,
         vip_box,
         msrp,
         vip_price
    );
