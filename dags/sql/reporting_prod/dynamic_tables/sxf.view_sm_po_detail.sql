CREATE OR REPLACE DYNAMIC TABLE REPORTING_PROD.SXF.VIEW_SM_PO_DETAIL
    TARGET_LAG = '10 minutes'
    WAREHOUSE = DA_WH_ANALYTICS
AS (
SELECT LEFT(sku, POSITION('-', sku, POSITION('-', sku) + 1) - 1)              AS color_sku,
       TRIM(UPPER(color))                                                     AS po_color,
       TRIM(UPPER(style))                                                     AS style_number_po,
       IFF(UPPER(style_type) = 'UNDERWEAR', 'UNDIE', TRIM(UPPER(style_type))) AS category_po,
       TRIM(UPPER(subclass))                                                  AS subcategory_po,
       TRIM(UPPER(pod.style_name))                                            AS style_name_po,
       UPPER(pos.description)                                                 AS po_status,
       region_id
FROM lake_view.jf_portal.po_hdr AS po
         JOIN lake_view.jf_portal.po_dtl AS pod ON (po.po_id = pod.po_id)
         JOIN LAKE_VIEW.JF_PORTAL.PO_STATUS AS pos ON (po.po_status_id = pos.po_status_id)
WHERE division_id = 'LINGERIE' --savage specific
  AND pod.qty <> 0             -- exclude cancelled skus
  AND (TRY_CAST(RIGHT(po_number, 1) AS NUMBER) IS NOT NULL -- Includes Only Direct/Regular, Fashion & Re Order POs  --
    OR RIGHT(po_number, 1) = 'L' -- Includes "core" styles POs, started using this later for savage
    OR RIGHT(po_number, 1) = 'R' -- Includes retail POs
    OR
       RIGHT(po_number, 1) = 'C' -- Includes Incremental skus to mainline placed after the 10 month calendar. (ie. carryovers in new colors, acquisition chase)
    OR RIGHT(po_number, 1) = 'P' -- Skus bought for capsule
    OR RIGHT(po_number, 1) = 'X' -- International Exclusives (eg. for EU only)
    OR RIGHT(po_number, 2) = 'XU' -- Savage X You
    )
GROUP BY color_sku,
         po_color,
         style_number_po,
         category_po,
         subcategory_po,
         style_name_po,
         po_status,
         region_id
)
;

ALTER DYNAMIC TABLE REPORTING_PROD.SXF.VIEW_SM_PO_DETAIL REFRESH;

GRANT SELECT ON REPORTING_PROD.SXF.VIEW_SM_PO_DETAIL TO ROLE __REPORTING_PROD_SXF_R;
GRANT SELECT ON REPORTING_PROD.SXF.VIEW_SM_PO_DETAIL TO ROLE __REPORTING_PROD_SXF_RW;
GRANT SELECT ON REPORTING_PROD.SXF.VIEW_SM_PO_DETAIL TO ROLE __REPORTING_PROD_SXF_RWC;
GRANT OWNERSHIP ON REPORTING_PROD.SXF.VIEW_SM_PO_DETAIL TO ROLE SYSADMIN COPY CURRENT GRANTS;
