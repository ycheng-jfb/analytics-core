use reporting_prod;
CREATE OR REPLACE TEMP TABLE pdd_with_cc_size AS
SELECT pdd.*,
       CONCAT(SPLIT_PART(pdd.sku, '-', 1), '-', SPLIT_PART(pdd.sku, '-', 2)) AS CC,
       CASE
           WHEN UPPER(TRIM(i.SIZE)) ILIKE ANY ('OS%', 'ONE SIZE', 'ONESIZE') THEN 'OS'
           WHEN UPPER(TRIM(i.SIZE)) ILIKE '2XL%' THEN 'XXL'
           WHEN UPPER(TRIM(i.SIZE)) ILIKE '3XL%' THEN 'XXXL'
           WHEN UPPER(TRIM(i.SIZE)) ILIKE 'XXL' THEN 'XXL/1X'
           WHEN UPPER(TRIM(i.SIZE)) ILIKE 'XXS' THEN 'XXS'
           WHEN UPPER(TRIM(i.SIZE)) ILIKE ANY ('XS', 'SIZE XS') THEN 'XS'
           WHEN UPPER(TRIM(i.SIZE)) ILIKE 'S/M%' THEN 'S/M'
           WHEN UPPER(TRIM(i.SIZE)) ILIKE 'M/L%' THEN 'M/L'
           WHEN UPPER(TRIM(i.SIZE)) ILIKE 'L/XL%' THEN 'L/XL'
           WHEN UPPER(TRIM(i.SIZE)) ILIKE ANY ('S', 'SM', 'SIZE S') THEN 'S'
           WHEN UPPER(TRIM(i.SIZE)) ILIKE ANY ('M', 'MD', 'SIZE M') THEN 'M'
           WHEN UPPER(TRIM(i.SIZE)) ILIKE ANY ('L', 'LG', 'SIZE L') THEN 'L'
           WHEN UPPER(TRIM(i.SIZE)) ILIKE 'LG REG' THEN 'L REG'
           WHEN UPPER(TRIM(i.SIZE)) ILIKE 'LG SHORT' THEN 'L SHORT'
           WHEN UPPER(TRIM(i.SIZE)) ILIKE 'LG TALL' THEN 'L TALL'
           WHEN UPPER(TRIM(i.SIZE)) ILIKE 'MD REG' THEN 'M REG'
           WHEN UPPER(TRIM(i.SIZE)) ILIKE 'MD SHORT' THEN 'M SHORT'
           WHEN UPPER(TRIM(i.SIZE)) ILIKE 'MD TALL' THEN 'M TALL'
           WHEN UPPER(TRIM(i.SIZE)) ILIKE 'SM REG' THEN 'S REG'
           WHEN UPPER(TRIM(i.SIZE)) ILIKE 'SM SHORT' THEN 'S SHORT'
           WHEN UPPER(TRIM(i.SIZE)) ILIKE 'SM TALL' THEN 'S TALL'
           WHEN UPPER(TRIM(i.SIZE)) ILIKE 'XXL REG' THEN 'XXL/1X REG'
           WHEN UPPER(TRIM(i.SIZE)) ILIKE 'XXL SHORT' THEN 'XXL/1X SHORT'
           WHEN UPPER(TRIM(i.SIZE)) ILIKE 'XXL TALL' THEN 'XXL/1X TALL'
           WHEN UPPER(TRIM(i.SIZE)) ILIKE '1X REG' THEN 'XXL/1X REG'
           WHEN UPPER(TRIM(i.SIZE)) ILIKE '1X SHORT' THEN 'XXL/1X SHORT'
           WHEN UPPER(TRIM(i.SIZE)) ILIKE '1X TALL' THEN 'XXL/1X TALL'
           WHEN UPPER(TRIM(i.SIZE)) ILIKE 'X3%' THEN i.size
           WHEN UPPER(TRIM(i.SIZE)) ILIKE '1X' THEN 'XXL/1X'
           WHEN UPPER(TRIM(i.SIZE)) ILIKE 'Unknown' THEN 'OS'
           WHEN UPPER(TRIM(i.SIZE)) IN ('2T', '3T') THEN 'OTHER'
           ELSE UPPER(TRIM(i.SIZE)) END                                      AS SIZE_DESC
FROM REPORTING_PROD.GSC.PO_DETAIL_DATASET_AGG pdd
         LEFT JOIN lake_view.ultra_warehouse.item i ON i.item_number = pdd.sku
WHERE 1 = 1
  AND PDD.TOTAL_QTY > 0
  and UPPER(pdd.division_id) in ('FABLETICS', 'YITTY')
  and pdd.PO_TYPE in
      ('CORE', 'RETAIL', 'FASHION', 'DIRECT', 'CAPSULE', 'CHASE', 'RE-ORDER', 'TOP-UP', 'INTERNATIONAL EXCLUSIVES');

-- updated 6.11.24 to pull several attributes from dim view
CREATE OR REPLACE TEMP TABLE _item_sku AS
SELECT DISTINCT pdd.SKU                                                         AS ITEM,
                CONCAT(bd.CURRENT_NAME, ',', bd.COLOR_DESC, ',', pdd.SIZE_DESC) AS DESCR,
                bd.TOTAL_COMPANY                                                AS TOTAL_COMPANY,
                bd.TOTAL_COMPANY_DESC                                           AS TOTAL_COMPANY_DESC,
                bd.COMPANY                                                      AS COMPANY,      -- added 6.5.24
                bd.COMPANY_DESC                                                 AS COMPANY_DESC, -- added 6.5.24
                NULL                                                            AS BUS_SEG,
                NULL                                                            AS BUS_SEG_DESC,
                bd.DIVISION                                                     AS DIVISION,
                bd.DIVISION_DESC                                                AS DIVISION_DESC,
                bd.PRODUCT_SEGMENT                                              AS PRODUCT_SEGMENT,
                bd.PRODUCT_SEGMENT_DESC                                         AS PRODUCT_SEGMENT_DESC,
                bd.GENDER                                                       AS GENDER,
                bd.GENDER_DESC                                                  AS GENDER_DESC,
                bd.SUB_DEPT                                                     AS SUB_DEPT,     --updated 6.5.24 --7/2 includes dedupe logic from dim_item
                bd.SUB_DEPT_DESC                                                AS SUB_DEPT_DESC,
                bd.CLASS                                                        AS CLASS,        --updated 6.5.24
                bd.CLASS_DESC                                                   AS CLASS_DESC,
                bd.SUB_CLASS                                                    AS SUB_CLASS,    --added 6.5.24
                bd.SUB_CLASS_DESC                                               AS SUB_CLASS_DESC,
                bd.STYLE                                                        AS STYLE,        --base sku
                ubt.CURRENT_NAME                                                AS STYLE_DESC,
                pdd.CC                                                          AS CC,
                CONCAT(bd.CURRENT_NAME, ',', bd.COLOR_DESC)                     AS CC_DESC,
                bd.COLOR                                                        AS COLOR,
                bd.COLOR_DESC                                                   AS COLOR_DESC,
                bd.SIZE                                                         AS SIZE,
                pdd.SIZE_DESC                                                   AS SIZE_DESC,
                ubt.INSEAMS_CONSTRUCTION                                        AS SIZE_SEQ,
                ubt.ECO_SYSTEM                                                  AS ECOSYSTEM,
                ubt.FIT_BLOCK                                                   AS FIT_BLOCK,
                CASE
                    WHEN ubt.LINED_UNLINED = 'LINED' THEN 'Y'
                    WHEN ubt.LINED_UNLINED = 'UNLINED' THEN 'N'
                    END                                                         AS LINED_UNLINED,
                REPLACE(UBT.FABRIC, '|', '_')                                   AS FABRIC,       --updated 6.5.24
                REPLACE(UBT.FABRIC, '|', '_')                                   AS FABRIC_DESC,  --updated 6.5.24
                ubt.MARKETING_STORY                                             AS COLLECTION,
                ubt.MARKETING_STORY                                             AS COLLECTION_DESC,
                ubt.COLOR_FAMILY                                                AS COLOR_FAMILY,
                ubt.COLOR_FAMILY                                                AS COLOR_FAMILY_DESC,
                IFF(CORE_VS_FASHION IS NOT NULL, DENSE_RANK() OVER (ORDER BY bd.CORE_VS_FASHION) - 1,
                    NULL)                                                       AS CORE_ITEM,    --added 6.6.24 | 7.10.24 updated to make unique IDs
                bd.CORE_VS_FASHION                                              AS CORE_VS_FASHION,
                coalesce(P.retail_unit_price, ubt.US_MSRP_DOLLAR)               AS RETAIL_PRICE, --added 6/25/2024
                coalesce(p.vip_unit_price, ubt.us_vip_dollar)                   AS VIP_PRICE,    --added 6/25/2024
                0                                                               AS msp_price,    --added 6/26/2024 hardcoded to zero and a placeholder for wholesale
                0                                                               AS DMD_FLAG,
                IFF(bd.IS_LADDER = TRUE, 1, 0)                                  AS FF_FLAG,      --updated 8.26.24 to use new is ladder column
                1                                                               AS ALLOC_FLAG,
                1                                                               AS EP_FLAG,
                1                                                               AS SS_FLAG
FROM pdd_with_cc_size pdd
         LEFT JOIN REPORTING_BASE_PROD.FABLETICS.UBT_MAX_SHOWROOM_NO_DATE_DH ubt
                   ON pdd.CC = ubt.SKU --updated 11.20.24 to DH UBT
         join REPORTING_BASE_PROD.BLUE_YONDER.DIM_ITEM bd on bd.item = pdd.sku
         left join (select *
                    from (select distinct product_sku,
                                          retail_unit_price,
                                          vip_unit_price,
                                          rank() over (partition by product_sku order by product_id) as rn
                          from EDW_PROD.data_model_fl.DIM_ITEM_PRICE
                          where store_id = 52)
                    where rn = 1) p
                   on p.product_sku = bd.product_sku --added 6/25/2024 updated on 7/1 to remove dupes with rank function
;

-- updated 6.11.24 to pull several attributes from dim view
CREATE OR REPLACE TEMP TABLE _item_cc AS
SELECT DISTINCT bd.PRODUCT_SKU                                    AS ITEM,         -- Christina updated 1.23.25 to pull new ladder CCs
                CONCAT(bd.CURRENT_NAME, ',', bd.COLOR_DESC)       AS DESCR,
                bd.TOTAL_COMPANY                                  AS TOTAL_COMPANY,
                bd.TOTAL_COMPANY_DESC                             AS TOTAL_COMPANY_DESC,
                bd.COMPANY                                        AS COMPANY,      -- added 6.5.24
                bd.COMPANY_DESC                                   AS COMPANY_DESC, -- added 6.5.24
                'FABLETICS'                                       AS BUS_SEG,
                'FABLETICS'                                       AS BUS_SEG_DESC,
                bd.DIVISION                                       AS DIVISION,
                bd.DIVISION_DESC                                  AS DIVISION_DESC,
                bd.PRODUCT_SEGMENT                                AS PRODUCT_SEGMENT,
                bd.PRODUCT_SEGMENT_DESC                           AS PRODUCT_SEGMENT_DESC,
                bd.GENDER                                         AS GENDER,
                bd.GENDER_DESC                                    AS GENDER_DESC,
                bd.SUB_DEPT                                       AS SUB_DEPT,     --updated 6.5.24
                bd.SUB_DEPT_DESC                                  AS SUB_DEPT_DESC,
                bd.CLASS                                          AS CLASS,        --updated 6.5.24
                bd.CLASS_DESC                                     AS CLASS_DESC,
                bd.SUB_CLASS                                      AS SUB_CLASS,    --added 6.5.24
                bd.SUB_CLASS_DESC                                 AS SUB_CLASS_DESC,
                bd.STYLE                                          AS STYLE,        --base sku
                ubt.CURRENT_NAME                                  AS STYLE_DESC,
                NULL                                              AS CC,
                NULL                                              AS CC_DESC,
                bd.COLOR                                          AS COLOR,
                bd.COLOR_DESC                                     AS COLOR_DESC,
                NULL                                              AS SIZE,
                NULL                                              AS SIZE_DESC,
                NULL                                              AS SIZE_SEQ,
                ubt.ECO_SYSTEM                                    AS ECOSYSTEM,
                ubt.FIT_BLOCK                                     AS FIT_BLOCK,
                CASE
                    WHEN ubt.LINED_UNLINED = 'LINED' THEN 'Y'
                    WHEN ubt.LINED_UNLINED = 'UNLINED' THEN 'N'
                    END                                           AS LINED_UNLINED,
                REPLACE(UBT.FABRIC, '|', '_')                     AS FABRIC,       --updated 6.5.24
                REPLACE(UBT.FABRIC, '|', '_')                     AS FABRIC_DESC,  --updated 6.5.24
                ubt.MARKETING_STORY                               AS COLLECTION,
                ubt.MARKETING_STORY                               AS COLLECTION_DESC,
                ubt.COLOR_FAMILY                                  AS COLOR_FAMILY,
                ubt.COLOR_FAMILY                                  AS COLOR_FAMILY_DESC,
                IFF(CORE_VS_FASHION IS NOT NULL, DENSE_RANK() OVER (ORDER BY bd.CORE_VS_FASHION) - 1,
                    NULL)                                         AS CORE_ITEM,    --added 6.6.24 | 7.10.24 updated to make unique IDs
                bd.CORE_VS_FASHION                                AS CORE_VS_FASHION,
                coalesce(P.retail_unit_price, ubt.US_MSRP_DOLLAR) AS RETAIL_PRICE, --added 6/25/2024
                coalesce(p.vip_unit_price, ubt.us_vip_dollar)     AS VIP_PRICE,    --added 6/25/2024
                0                                                 AS msp_price,    --added 6/26/2024 hardcoded to zero and a placeholder for wholesale
                1                                                 AS DMD_FLAG,
                0                                                 AS FF_FLAG,
                0                                                 AS ALLOC_FLAG,
                0                                                 AS EP_FLAG,
                0                                                 AS SS_FLAG
FROM REPORTING_BASE_PROD.BLUE_YONDER.DIM_ITEM bd
         LEFT JOIN REPORTING_BASE_PROD.FABLETICS.UBT_MAX_SHOWROOM_NO_DATE_DH ubt
                   ON bd.PRODUCT_SKU = ubt.SKU --updated 11.20.24 to DH UBT
         left join (select *
                    from (select distinct product_sku,
                                          retail_unit_price,
                                          vip_unit_price,
                                          rank() over (partition by product_sku order by product_id) as rn
                          from EDW_PROD.data_model_fl.DIM_ITEM_PRICE
                          where store_id = 52)
                    where rn = 1) p
                   on p.product_sku = bd.product_sku --updated on 7/1 to remove dupes with rank function
WHERE bd.IS_LADDER = TRUE --updated 8.26.24 to use is ladder instead of core
;

CREATE OR REPLACE TEMP TABLE _item_final AS
SELECT *
FROM _item_sku
UNION ALL
SELECT *
FROM _item_cc;
;

set invalid_hierarchy_check = (select iff(count(1)>=1, False, True) as a
from (select distinct product_sku, concat(product_segment_desc,sub_dept_desc,class_desc,sub_class_desc)
    as Incorrect_Hierarchy
from REPORTING_BASE_PROD.BLUE_YONDER.DIM_ITEM
where Incorrect_Hierarchy not in(select distinct (concat(product_segment, category,class,subclass))
from LAKE_VIEW.SHAREPOINT.DREAMSTATE_UBT_HIERARCHY)) a)
;


set multiple_categories_check = (select iff(count(1)>=1, False, True) as a
from  (select item.product_segment_desc as product_segments,
            item.style,
            count(distinct item.class) as classes,
            count(distinct item.sub_class) as sub_classes,
            count(distinct item.division) as divisions,
            count(distinct item.sub_dept) as sub_depts
        from REPORTING_BASE_PROD.BLUE_YONDER.DIM_ITEM item
        join REPORTING_BASE_PROD.FABLETICS.UBT_MAX_SHOWROOM_NO_DATE_DH ubt on ubt.SKU = item.PRODUCT_SKU
        group by item.product_segment_desc,item.style
        having greatest(count(distinct item.class),
                        count(distinct item.sub_class),
                        count(distinct item.division),
                        count(distinct item.sub_dept)
                        ) > 1
order by greatest(classes, sub_classes, divisions, sub_depts) desc
) b
);


TRUNCATE TABLE reporting_prod.blue_yonder.export_by_item_interface_stg;

insert into reporting_prod.blue_yonder.export_by_item_interface_stg
(
ITEM,
DESCR,
TOTAL_COMPANY,
TOTAL_COMPANY_DESC,
COMPANY,
COMPANY_DESC,
DIVISION,
DIVISION_DESC,
PRODUCT_SEGMENT,
PRODUCT_SEGMENT_DESC,
GENDER,
GENDER_DESC,
SUB_DEPT,
SUB_DEPT_DESC,
CLASS,
CLASS_DESC,
SUB_CLASS,
SUB_CLASS_DESC,
STYLE,
STYLE_DESC,
CC,
CC_DESC,
COLOR,
COLOR_DESC,
SIZE,
SIZE_DESC,
SIZE_SEQ,
ECOSYSTEM,
FIT_BLOCK,
LINED_UNLINED,
FABRIC,
FABRIC_DESC,
COLLECTION,
COLLECTION_DESC,
COLOR_FAMILY,
COLOR_FAMILY_DESC,
CORE_ITEM,
CORE_VS_FASHION,
DMD_FLAG,
FF_FLAG,
ALLOC_FLAG,
EP_FLAG,
SS_FLAG,
VIP_PRICE,
RETAIL_PRICE,
MSP_PRICE
)
select ITEM,
    DESCR,
    TOTAL_COMPANY,
    TOTAL_COMPANY_DESC,
    COMPANY,
    COMPANY_DESC,
    DIVISION,
    DIVISION_DESC,
    PRODUCT_SEGMENT,
    PRODUCT_SEGMENT_DESC,
    GENDER,
    GENDER_DESC,
    SUB_DEPT,
    SUB_DEPT_DESC,
    CLASS,
    CLASS_DESC,
    SUB_CLASS,
    SUB_CLASS_DESC,
    STYLE,
    STYLE_DESC,
    CC,
    CC_DESC,
    COLOR,
    COLOR_DESC,
    SIZE,
    SIZE_DESC,
    SIZE_SEQ,
    ECOSYSTEM,
    FIT_BLOCK,
    LINED_UNLINED,
    FABRIC,
    FABRIC_DESC,
    COLLECTION,
    COLLECTION_DESC,
    COLOR_FAMILY,
    COLOR_FAMILY_DESC,
    CORE_ITEM,
    CORE_VS_FASHION,
    DMD_FLAG,
    FF_FLAG,
    ALLOC_FLAG,
    EP_FLAG,
    SS_FLAG,
    VIP_PRICE,
    RETAIL_PRICE,
    MSP_PRICE
FROM _item_final
where $invalid_hierarchy_check and $multiple_categories_check;

-- insert last successful batch when hierarchy or multiple categories issue occurs
insert into reporting_prod.blue_yonder.export_by_item_interface_stg
(
ITEM,
DESCR,
TOTAL_COMPANY,
TOTAL_COMPANY_DESC,
COMPANY,
COMPANY_DESC,
DIVISION,
DIVISION_DESC,
PRODUCT_SEGMENT,
PRODUCT_SEGMENT_DESC,
GENDER,
GENDER_DESC,
SUB_DEPT,
SUB_DEPT_DESC,
CLASS,
CLASS_DESC,
SUB_CLASS,
SUB_CLASS_DESC,
STYLE,
STYLE_DESC,
CC,
CC_DESC,
COLOR,
COLOR_DESC,
SIZE,
SIZE_DESC,
SIZE_SEQ,
ECOSYSTEM,
FIT_BLOCK,
LINED_UNLINED,
FABRIC,
FABRIC_DESC,
COLLECTION,
COLLECTION_DESC,
COLOR_FAMILY,
COLOR_FAMILY_DESC,
CORE_ITEM,
CORE_VS_FASHION,
DMD_FLAG,
FF_FLAG,
ALLOC_FLAG,
EP_FLAG,
SS_FLAG,
VIP_PRICE,
RETAIL_PRICE,
MSP_PRICE
)
select ITEM,
    DESCR,
    TOTAL_COMPANY,
    TOTAL_COMPANY_DESC,
    COMPANY,
    COMPANY_DESC,
    DIVISION,
    DIVISION_DESC,
    PRODUCT_SEGMENT,
    PRODUCT_SEGMENT_DESC,
    GENDER,
    GENDER_DESC,
    SUB_DEPT,
    SUB_DEPT_DESC,
    CLASS,
    CLASS_DESC,
    SUB_CLASS,
    SUB_CLASS_DESC,
    STYLE,
    STYLE_DESC,
    CC,
    CC_DESC,
    COLOR,
    COLOR_DESC,
    SIZE,
    SIZE_DESC,
    SIZE_SEQ,
    ECOSYSTEM,
    FIT_BLOCK,
    LINED_UNLINED,
    FABRIC,
    FABRIC_DESC,
    COLLECTION,
    COLLECTION_DESC,
    COLOR_FAMILY,
    COLOR_FAMILY_DESC,
    CORE_ITEM,
    CORE_VS_FASHION,
    DMD_FLAG,
    FF_FLAG,
    ALLOC_FLAG,
    EP_FLAG,
    SS_FLAG,
    VIP_PRICE,
    RETAIL_PRICE,
    MSP_PRICE
FROM reporting_prod.blue_yonder.export_by_item_interface
where not ($invalid_hierarchy_check and $multiple_categories_check)
and date_added = (select max(date_added) from reporting_prod.blue_yonder.export_by_item_interface);

update reporting_prod.blue_yonder.export_by_ITEM_interface_stg
set
    item = reporting_prod.blue_yonder.udf_cleanup_field(item),
    descr = reporting_prod.blue_yonder.udf_cleanup_field(descr),
    total_company = reporting_prod.blue_yonder.udf_cleanup_field(total_company),
    total_company_desc = reporting_prod.blue_yonder.udf_cleanup_field(total_company_desc),
    company = reporting_prod.blue_yonder.udf_cleanup_field(company),
    company_desc = reporting_prod.blue_yonder.udf_cleanup_field(company_desc),
    division = reporting_prod.blue_yonder.udf_cleanup_field(division),
    division_desc = reporting_prod.blue_yonder.udf_cleanup_field(division_desc),
    product_segment = reporting_prod.blue_yonder.udf_cleanup_field(product_segment),
    product_segment_desc = reporting_prod.blue_yonder.udf_cleanup_field(product_segment_desc),
    gender = reporting_prod.blue_yonder.udf_cleanup_field(gender),
    gender_desc = reporting_prod.blue_yonder.udf_cleanup_field(gender_desc),
    sub_dept = reporting_prod.blue_yonder.udf_cleanup_field(sub_dept),
    sub_dept_desc = reporting_prod.blue_yonder.udf_cleanup_field(sub_dept_desc),
    class = reporting_prod.blue_yonder.udf_cleanup_field(class),
    class_desc = reporting_prod.blue_yonder.udf_cleanup_field(class_desc),
    sub_class = reporting_prod.blue_yonder.udf_cleanup_field(sub_class),
    sub_class_desc = reporting_prod.blue_yonder.udf_cleanup_field(sub_class_desc),
    style = reporting_prod.blue_yonder.udf_cleanup_field(style),
    style_desc = left(reporting_prod.blue_yonder.udf_cleanup_field(style_desc),50),
    cc = reporting_prod.blue_yonder.udf_cleanup_field(cc),
    cc_desc = reporting_prod.blue_yonder.udf_cleanup_field(cc_desc),
    color = reporting_prod.blue_yonder.udf_cleanup_field(color),
    color_desc = reporting_prod.blue_yonder.udf_cleanup_field(color_desc),
    size = reporting_prod.blue_yonder.udf_cleanup_field(size),
    size_desc = reporting_prod.blue_yonder.udf_cleanup_field(size_desc),
    size_seq = reporting_prod.blue_yonder.udf_cleanup_field(size_seq),
    ecosystem = reporting_prod.blue_yonder.udf_cleanup_field(ecosystem),
    fit_block = reporting_prod.blue_yonder.udf_cleanup_field(fit_block),
    lined_unlined = reporting_prod.blue_yonder.udf_cleanup_field(lined_unlined),
    fabric = left(reporting_prod.blue_yonder.udf_cleanup_field(fabric),10),
    fabric_desc = reporting_prod.blue_yonder.udf_cleanup_field(fabric_desc),
    collection = left(reporting_prod.blue_yonder.udf_cleanup_field(collection),10),
    collection_desc = reporting_prod.blue_yonder.udf_cleanup_field(collection_desc),
    color_family = reporting_prod.blue_yonder.udf_cleanup_field(color_family),
    color_family_desc = reporting_prod.blue_yonder.udf_cleanup_field(color_family_desc),
    core_item = reporting_prod.blue_yonder.udf_cleanup_field(core_item),
    core_vs_fashion = reporting_prod.blue_yonder.udf_cleanup_field(core_vs_fashion),
    dmd_flag = reporting_prod.blue_yonder.udf_cleanup_field(dmd_flag),
    ff_flag = reporting_prod.blue_yonder.udf_cleanup_field(ff_flag),
    alloc_flag = reporting_prod.blue_yonder.udf_cleanup_field(alloc_flag),
    ep_flag = reporting_prod.blue_yonder.udf_cleanup_field(ep_flag),
    ss_flag = reporting_prod.blue_yonder.udf_cleanup_field(ss_flag),
    vip_price = reporting_prod.blue_yonder.udf_cleanup_field(vip_price),
    retail_price = reporting_prod.blue_yonder.udf_cleanup_field(retail_price),
    msp_price = reporting_prod.blue_yonder.udf_cleanup_field(msp_price)
;
