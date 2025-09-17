CREATE OR REPLACE TEMP TABLE store AS
SELECT *
FROM edw_prod.data_model_sxf.dim_store
WHERE store_brand = 'Savage X'
  AND store_type = 'Online'
  AND is_core_store = TRUE;


CREATE OR REPLACE TEMP TABLE dp1 AS
SELECT DISTINCT master_product_id
              , product_sku
              , product_name
              , product_type
              , new_core_fashion
              , store_region
              , ROW_NUMBER() OVER (PARTITION BY master_product_id ORDER BY product_sku) AS rn
FROM edw_prod.data_model_sxf.dim_product d
         JOIN store ON d.store_id = store.store_id
         LEFT JOIN lake_view.sharepoint.med_sxf_core_fashion ncf ON ncf.color_sku_po = d.product_sku
WHERE master_product_id <> -1
  AND product_id_object_type <> 'No Size-Master Product ID'
  AND product_sku <> 'Unknown'
QUALIFY ROW_NUMBER() OVER (PARTITION BY master_product_id ORDER BY product_sku) = 1
UNION
SELECT DISTINCT product_id                                                       AS master_product_id
              , product_sku
              , product_name
              , product_type
              , new_core_fashion
              , store_region
              , ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY product_sku) AS rn
FROM edw_prod.data_model_sxf.dim_product dp
         JOIN store ON dp.store_id = store.store_id
         LEFT JOIN lake_view.sharepoint.med_sxf_core_fashion ncf
                   ON ncf.color_sku_po = dp.product_sku
WHERE product_id_object_type = 'No Size-Master Product ID'
  AND (product_sku = SPLIT_PART(product_alias, ' ', 1) OR product_alias NOT ILIKE ('do not use%'))
QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY product_sku) = 1;



CREATE OR REPLACE TEMP TABLE pc AS
SELECT l1_categorization
     , l2_categorization
     , psource
FROM lake_view.sharepoint.psource_categorization
WHERE brand = 'Savage X'
QUALIFY ROW_NUMBER() OVER (PARTITION BY LOWER(psource) ORDER BY meta_row_hash) = 1;



CREATE OR REPLACE TEMP TABLE mm AS
SELECT DISTINCT properties_product_id
              , properties_has_member_models
FROM lake.segment_sxf.javascript_sxf_product_viewed
WHERE properties_has_member_models = TRUE;



CREATE OR REPLACE TRANSIENT TABLE reporting_prod.sxf.pcvr_product_psource_reduced AS
SELECT pp.session_start_date,
       pp.country,
       dp1.store_region        AS region,
       pp.membership_status,
       sub_department,
       sm.category,
       dp1.new_core_fashion,
       collection,
       pp.psource,
       pc.l1_categorization,
       pc.l2_categorization,
       SUM(pp.list_view_total) AS list_view_total,
       SUM(pdp_view_total)     AS pdp_view_total,
       SUM(pp.pa_total)        AS pa_total,
       SUM(oc_total)           AS oc_total,
       SUM(ss_pdp_view)        AS ss_pdp_view,
       SUM(ss_product_added)   AS ss_product_added,
       SUM(ss_product_ordered) AS ss_product_ordered,
       pp.refresh_datetime,
       pp.max_received
FROM reporting_prod.sxf.pcvr_product_psource pp
         LEFT JOIN dp1 ON pp.product_id::VARCHAR = dp1.master_product_id::VARCHAR
         LEFT JOIN reporting_prod.sxf.style_master sm ON dp1.product_sku = sm.color_sku_po
         LEFT JOIN pc ON LOWER(pp.psource) = LOWER(pc.psource)
         LEFT JOIN mm ON pp.product_id::VARCHAR = mm.properties_product_id::VARCHAR
WHERE session_start_date >= '2023-01-01'
  AND product_name NOT ILIKE '%membership%'
  AND product_type NOT ILIKE '%bundle%'
  AND product_name NOT ILIKE '%insert%'
  AND product_type NOT ILIKE '%insert%'
GROUP BY session_start_date,
         pp.psource,
         country,
         region,
         membership_status,
         refresh_datetime,
         max_received,
         l1_categorization,
         l2_categorization,
         category,
         collection,
         sub_department,
         dp1.new_core_fashion;
