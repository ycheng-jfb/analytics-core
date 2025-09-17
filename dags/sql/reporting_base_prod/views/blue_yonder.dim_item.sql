CREATE OR REPLACE VIEW reporting_base_prod.blue_yonder.dim_item
            (
             item,
             product_sku,
             style,
             total_company,
             total_company_desc,
             company,
             company_desc,
             division,
             division_desc,
             product_segment,
             product_segment_desc,
             gender,
             gender_desc,
             sub_dept,
             sub_dept_desc,
             class,
             class_desc,
             sub_class,
             sub_class_desc,
             current_name,
             core_vs_fashion,
             is_ladder,
             is_retail_ladder,
             color,
             color_desc,
             size
                )
AS
SELECT *
FROM (WITH pdd_with_cc AS (SELECT pdd.*,
                                  CONCAT(SPLIT_PART(pdd.sku, '-', 1), '-', SPLIT_PART(pdd.sku, '-', 2)) AS cc
                           FROM reporting_prod.gsc.po_detail_dataset_agg pdd
                           WHERE 1 = 1
                             AND pdd.total_qty > 0
                             AND UPPER(pdd.division_id) IN ('FABLETICS', 'YITTY')
                             AND pdd.po_type IN
                                 ('CORE', 'RETAIL', 'FASHION', 'DIRECT', 'CAPSULE', 'CHASE', 'RE-ORDER', 'TOP-UP',
                                  'INTERNATIONAL EXCLUSIVES')),

           _base AS (SELECT DISTINCT pdd.sku                                                         AS item,
                                     COALESCE(pdd.cc, l.cc)                                          AS product_sku,      -- Christina updated 1.21.25 to bring in future ladder product skus
                                     COALESCE(SPLIT_PART(pdd.sku, '-', 1), SPLIT_PART(l.cc, '-', 1)) AS style,            -- Christina updated 1.22.25 to bring in future ladder styles #s
                                     'TECHSTYLE'                                                     AS total_company,
                                     'TOTAL TECHSTYLE'                                               AS total_company_desc,
                                     'TOT_FAB'                                                       AS company,
                                     'TOTAL FABLETICS'                                               AS company_desc,
                                     CASE
                                         WHEN ubt.product_segment = 'YITTY WOMENS' THEN 'YT02'
                                         WHEN ubt.product_segment IS NULL THEN NULL
                                         ELSE 'FL01' END                                             AS division,         -- Christina updated 1.22.25 to bring in null values for future ladders
                                     CASE
                                         WHEN ubt.product_segment = 'YITTY WOMENS' THEN 'YITTY'
                                         WHEN ubt.product_segment IS NULL THEN NULL
                                         ELSE 'FABLETICS' END                                        AS division_desc,    -- Christina updated 1.22.25 to bring in null values for future ladders
                                     CASE
                                         WHEN ubt.product_segment = 'YITTY WOMENS' THEN 'PSYT'
                                         WHEN ubt.product_segment = 'FL WOMENS' THEN 'PSFLW'
                                         WHEN ubt.product_segment = 'FL MENS' THEN 'PSFLM'
                                         WHEN ubt.product_segment = 'FL ACC' THEN 'PSFLA'
                                         WHEN ubt.product_segment = 'SC MENS' THEN 'PSSCM'
                                         WHEN ubt.product_segment = 'SC WOMENS' THEN 'PSSCW'
                                         END                                                         AS product_segment,
                                     ubt.product_segment                                             AS product_segment_desc,
                                     CASE
                                         WHEN ubt.gender ILIKE 'WOMEN%' THEN 'W'
                                         WHEN ubt.gender IS NULL THEN NULL
                                         ELSE 'M' END                                                AS gender,           -- Christina updated 1.22.25 to bring in null values for future ladders
                                     CASE
                                         WHEN ubt.gender ILIKE 'WOMEN%' THEN 'WOMENS'
                                         WHEN ubt.gender IS NULL THEN NULL
                                         ELSE 'MENS' END                                             AS gender_desc,      -- Christina updated 1.22.25 to bring in null values for future ladders
                                     LEFT(CASE
                                              WHEN ubt.product_segment = 'FL WOMENS' THEN CONCAT('SD_', 'FLW_',
                                                                                                 DENSE_RANK() OVER (PARTITION BY ubt.product_segment ORDER BY ms.min_current_showroom,REPLACE(CONCAT(ubt.product_segment, ubt.category), ' ', '')),
                                                                                                 ubt.category)
                                              WHEN ubt.product_segment = 'FL MENS' THEN CONCAT('SD_', 'FLM_',
                                                                                               DENSE_RANK() OVER (PARTITION BY ubt.product_segment ORDER BY ms.min_current_showroom,REPLACE(CONCAT(ubt.product_segment, ubt.category), ' ', '')),
                                                                                               ubt.category)
                                              WHEN ubt.product_segment = 'YITTY WOMENS' THEN CONCAT('SD_', 'YT_',
                                                                                                    DENSE_RANK() OVER (PARTITION BY ubt.product_segment ORDER BY ms.min_current_showroom,REPLACE(CONCAT(ubt.product_segment, ubt.category), ' ', '')),
                                                                                                    ubt.category)
                                              WHEN ubt.product_segment = 'SC MENS' THEN CONCAT('SD_', 'SCM_',
                                                                                               DENSE_RANK() OVER (PARTITION BY ubt.product_segment ORDER BY ms.min_current_showroom,REPLACE(CONCAT(ubt.product_segment, ubt.category), ' ', '')),
                                                                                               ubt.category)
                                              WHEN ubt.product_segment = 'SC WOMENS' THEN CONCAT('SD_', 'SCW_',
                                                                                                 DENSE_RANK() OVER (PARTITION BY ubt.product_segment ORDER BY ms.min_current_showroom,REPLACE(CONCAT(ubt.product_segment, ubt.category), ' ', '')),
                                                                                                 ubt.category)
                                              WHEN ubt.product_segment = 'FL ACC' THEN CONCAT('SD_', 'FLA_',
                                                                                              DENSE_RANK() OVER (PARTITION BY ubt.product_segment ORDER BY ms.min_current_showroom,REPLACE(CONCAT(ubt.product_segment, ubt.category), ' ', '')),
                                                                                              ubt.category)
                                              END, 15)                                               AS sub_dept,
                                     ubt.category                                                    AS sub_dept_desc,
                                     LEFT(CASE
                                              WHEN ubt.product_segment = 'FL WOMENS' THEN CONCAT('CL_', 'FLW_',
                                                                                                 DENSE_RANK() OVER (PARTITION BY ubt.product_segment ORDER BY mc.min_current_showroom,REPLACE(
                                                                                                         CONCAT(ubt.product_segment, ubt.category, ubt.class),
                                                                                                         ' ', '')),
                                                                                                 ubt.class)
                                              WHEN ubt.product_segment = 'FL MENS' THEN CONCAT('CL_', 'FLM_',
                                                                                               DENSE_RANK() OVER (PARTITION BY ubt.product_segment ORDER BY mc.min_current_showroom,REPLACE(
                                                                                                       CONCAT(ubt.product_segment, ubt.category, ubt.class),
                                                                                                       ' ', '')),
                                                                                               ubt.class)
                                              WHEN ubt.product_segment = 'YITTY WOMENS' THEN CONCAT('CL_', 'YT_',
                                                                                                    DENSE_RANK() OVER (PARTITION BY ubt.product_segment ORDER BY mc.min_current_showroom,REPLACE(
                                                                                                            CONCAT(ubt.product_segment, ubt.category, ubt.class),
                                                                                                            ' ', '')),
                                                                                                    ubt.class)
                                              WHEN ubt.product_segment = 'SC MENS' THEN CONCAT('CL_', 'SCM_',
                                                                                               DENSE_RANK() OVER (PARTITION BY ubt.product_segment ORDER BY mc.min_current_showroom,REPLACE(
                                                                                                       CONCAT(ubt.product_segment, ubt.category, ubt.class),
                                                                                                       ' ', '')),
                                                                                               ubt.class)
                                              WHEN ubt.product_segment = 'SC WOMENS' THEN CONCAT('CL_', 'SCW_',
                                                                                                 DENSE_RANK() OVER (PARTITION BY ubt.product_segment ORDER BY mc.min_current_showroom,REPLACE(
                                                                                                         CONCAT(ubt.product_segment, ubt.category, ubt.class),
                                                                                                         ' ', '')),
                                                                                                 ubt.class)
                                              WHEN ubt.product_segment = 'FL ACC' THEN CONCAT('CL_', 'FLA_',
                                                                                              DENSE_RANK() OVER (PARTITION BY ubt.product_segment ORDER BY mc.min_current_showroom,REPLACE(
                                                                                                      CONCAT(ubt.product_segment, ubt.category, ubt.class),
                                                                                                      ' ', '')),
                                                                                              ubt.class)
                                              END, 15)                                               AS class,
                                     ubt.class                                                       AS class_desc,
                                     LEFT(CASE
                                              WHEN ubt.product_segment = 'FL WOMENS' THEN CONCAT('SC_', 'FLW_',
                                                                                                 DENSE_RANK() OVER (PARTITION BY ubt.product_segment ORDER BY msb.min_current_showroom,REPLACE(
                                                                                                         CONCAT(ubt.product_segment, ubt.category, ubt.class, ubt.subclass),
                                                                                                         ' ', '')),
                                                                                                 ubt.subclass)
                                              WHEN ubt.product_segment = 'FL MENS' THEN CONCAT('SC_', 'FLM_',
                                                                                               DENSE_RANK() OVER (PARTITION BY ubt.product_segment ORDER BY msb.min_current_showroom,REPLACE(
                                                                                                       CONCAT(ubt.product_segment, ubt.category, ubt.class, ubt.subclass),
                                                                                                       ' ', '')),
                                                                                               ubt.subclass)
                                              WHEN ubt.product_segment = 'YITTY WOMENS' THEN CONCAT('SC_', 'YT_',
                                                                                                    DENSE_RANK() OVER (PARTITION BY ubt.product_segment ORDER BY msb.min_current_showroom,REPLACE(
                                                                                                            CONCAT(ubt.product_segment, ubt.category, ubt.class, ubt.subclass),
                                                                                                            ' ', '')),
                                                                                                    ubt.subclass)
                                              WHEN ubt.product_segment = 'SC MENS' THEN CONCAT('SC_', 'SCM_',
                                                                                               DENSE_RANK() OVER (PARTITION BY ubt.product_segment ORDER BY msb.min_current_showroom,REPLACE(
                                                                                                       CONCAT(ubt.product_segment, ubt.category, ubt.class, ubt.subclass),
                                                                                                       ' ', '')),
                                                                                               ubt.subclass)
                                              WHEN ubt.product_segment = 'SC WOMENS' THEN CONCAT('SC_', 'SCW_',
                                                                                                 DENSE_RANK() OVER (PARTITION BY ubt.product_segment ORDER BY msb.min_current_showroom,REPLACE(
                                                                                                         CONCAT(ubt.product_segment, ubt.category, ubt.class, ubt.subclass),
                                                                                                         ' ', '')),
                                                                                                 ubt.subclass)
                                              WHEN ubt.product_segment = 'FL ACC' THEN CONCAT('SC_', 'FLA_',
                                                                                              DENSE_RANK() OVER (PARTITION BY ubt.product_segment ORDER BY msb.min_current_showroom,REPLACE(
                                                                                                      CONCAT(ubt.product_segment, ubt.category, ubt.class, ubt.subclass),
                                                                                                      ' ', '')),
                                                                                              ubt.subclass)
                                              END, 15)                                               AS sub_class,
                                     ubt.subclass                                                    AS sub_class_desc,
                                     COALESCE(ubt.current_name, l.current_name)                      AS current_name,
                                     ubt.item_status                                                 AS core_vs_fashion,
                                     IFF(l.cc IS NOT NULL, TRUE, FALSE)                              AS is_ladder,        --added 8.26.24 to pull only ladder CCs
                                     IFF(lr.cc IS NOT NULL, TRUE, FALSE)                             AS is_retail_ladder, --added 12.9.24 to pull only Retail ladder CCs

                                     COALESCE(SPLIT_PART(pdd.sku, '-', 2), SPLIT_PART(l.cc, '-', 2)) AS color,
                                     COALESCE(ubt.color, l.color)                                    AS color_desc,
                                     SPLIT_PART(pdd.sku, '-', -1)                                    AS size
                     FROM pdd_with_cc pdd
                              FULL JOIN reporting_base_prod.fabletics.ubt_max_showroom_no_date_dh ubt
                                        ON pdd.cc = ubt.sku --updated 11.20.24 to DH UBT
                              FULL JOIN lake_view.sharepoint.fl_ladders l
                                        ON l.cc = COALESCE(pdd.cc, ubt.sku) --Christina updated 1.21.25 to full join to bring in future ladder CCs not yet in UBT
                              LEFT JOIN lake_view.sharepoint.fl_ladders_retail lr
                                        ON lr.cc = pdd.cc -- added 12.9.24 for new is retail ladder column
                              LEFT JOIN (SELECT DISTINCT category,
                                                         MIN(current_showroom) AS min_current_showroom --added 7/2/24 to dedupe
                                         FROM lake_view.excel.fl_merch_items_ubt_hierarchy --updated 11.20.24 to DH UBT
                                         GROUP BY category) ms ON ubt.category = ms.category
                              LEFT JOIN (SELECT DISTINCT class,
                                                         MIN(current_showroom) AS min_current_showroom --added 7/2/24 to dedupe
                                         FROM lake_view.excel.fl_merch_items_ubt_hierarchy --updated 11.20.24 to DH UBT
                                         GROUP BY class) mc ON mc.class = ubt.class
                              LEFT JOIN (SELECT DISTINCT subclass,
                                                         MIN(current_showroom) AS min_current_showroom --added 7/2/24 to dedupe
                                         FROM lake_view.excel.fl_merch_items_ubt_hierarchy --updated 11.20.24 to DH UBT
                                         GROUP BY subclass) msb ON msb.subclass = ubt.subclass),

           _hierarchy_map AS (SELECT DISTINCT b.division        AS division,
                                              b.division_desc   AS division_desc,
                                              b.product_segment AS product_segment,
                                              h.product_segment AS product_segment_desc,
                                              b.gender          AS gender,
                                              b.gender_desc     AS gender_desc,
                                              b.sub_dept        AS sub_dept,
                                              h.category        AS sub_dept_desc,
                                              b.class           AS class,
                                              h.class           AS class_desc,
                                              b.sub_class       AS sub_class,
                                              h.subclass        AS sub_class_desc
                              FROM _base b
                                       LEFT JOIN lake_view.sharepoint.dreamstate_ubt_hierarchy h
                                                 ON h.product_segment = b.product_segment_desc AND
                                                    h.category = b.sub_dept_desc AND h.class = b.class_desc AND
                                                    h.subclass = b.sub_class_desc),

           _new_ladders AS (SELECT DISTINCT l.cc                                            AS product_sku,
                                            COALESCE(hm.division, b.division)               AS division,
                                            COALESCE(hm.division_desc, b.division_desc)     AS division_desc,
                                            COALESCE(hm.product_segment, b.product_segment) AS product_segment,
                                            l.product_segment                               AS product_segment_desc,
                                            COALESCE(hm.gender, b.gender)                   AS gender,
                                            COALESCE(hm.gender_desc, b.gender_desc)         AS gender_desc,
                                            COALESCE(hm.sub_dept, b.sub_dept)               AS sub_dept,
                                            l.category                                      AS sub_dept_desc,
                                            COALESCE(hm.class, b.class)                     AS class,
                                            l.class                                         AS class_desc,
                                            COALESCE(hm.sub_class, b.sub_class)             AS sub_class,
                                            l.subclass                                      AS sub_class_desc
                            FROM _base b
                                     LEFT JOIN lake_view.sharepoint.fl_ladders l ON b.product_sku = l.cc
                                     LEFT JOIN _hierarchy_map hm
                                               ON hm.product_segment_desc = l.product_segment AND
                                                  hm.sub_dept_desc = l.category AND
                                                  hm.class_desc = l.class AND hm.sub_class_desc = l.subclass
                            WHERE b.product_segment_desc IS NULL)

      SELECT DISTINCT item,
                      COALESCE(nl.product_sku, b.product_sku)                   AS product_sku,
                      b.style,
                      b.total_company,
                      b.total_company_desc,
                      b.company,
                      b.company_desc,
                      COALESCE(b.division, nl.division)                         AS division,
                      COALESCE(b.division_desc, nl.division_desc)               AS division_desc,
                      COALESCE(b.product_segment, nl.product_segment)           AS product_segment,
                      COALESCE(b.product_segment_desc, nl.product_segment_desc) AS product_segment_desc,
                      COALESCE(b.gender, nl.gender)                             AS gender,
                      COALESCE(b.gender_desc, nl.gender_desc)                   AS gender_desc,
                      COALESCE(b.sub_dept, nl.sub_dept)                         AS sub_dept,
                      COALESCE(b.sub_dept_desc, nl.sub_dept_desc)               AS sub_dept_desc,
                      COALESCE(b.class, nl.class)                               AS class,
                      COALESCE(b.class_desc, nl.class_desc)                     AS class_desc,
                      COALESCE(b.sub_class, nl.sub_class)                       AS sub_class,
                      COALESCE(b.sub_class_desc, nl.sub_class_desc)             AS sub_class_desc,
                      b.current_name,
                      b.core_vs_fashion,
                      b.is_ladder,
                      b.is_retail_ladder,
                      b.color,
                      b.color_desc,
                      size
      FROM _base b
               LEFT JOIN _new_ladders nl ON nl.product_sku = b.product_sku)
WHERE division IS NOT NULL
  AND product_segment IS NOT NULL
  AND class IS NOT NULL
  AND sub_class IS NOT NULL;
