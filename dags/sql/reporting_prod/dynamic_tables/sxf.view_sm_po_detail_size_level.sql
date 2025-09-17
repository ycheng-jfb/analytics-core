CREATE OR REPLACE DYNAMIC TABLE REPORTING_PROD.SXF.VIEW_SM_PO_DETAIL_SIZE_LEVEL
    TARGET_LAG = '10 minutes'
    WAREHOUSE = DA_WH_ANALYTICS
AS (
select distinct
    sku,
    iff(size_name='OS','ONE SIZE', size_name) as size_name,
    rank() over (partition by sku order by po.show_room desc) as show_room_latest,
    rank() over (partition by sku, po.show_room order by pod.meta_update_datetime desc) as latest_update
from LAKE_VIEW.JF_PORTAL.PO_HDR as po
join LAKE_VIEW.JF_PORTAL.PO_DTL as pod
    on (po.po_id=pod.po_id)
join LAKE_VIEW.JF_PORTAL.PO_STATUS as pos
    on (po.po_status_id=pos.po_status_id)
where
    DIVISION_ID = 'LINGERIE' --savage specific
    and (pod.QTY <> 0
        OR pos.po_status_id = 10)-- exclude cancelled skus
    and (
        try_cast(right(po_number,1) as number) is not null -- Includes Only Direct/Regular, Fashion & Re Order POs  --
        or right(po_number,1)='L' -- Includes "core" styles POs, started using this later for savage
        or right(po_number,1)='R' -- Includes retail POs
        or right(po_number,1)='C' -- Includes Incremental skus to mainline placed after the 10 month calendar. (ie. carryovers in new colors, acquisition chase)
        or right(po_number,1)='P' -- Skus bought for capsule
        or right(po_number,1)='X')
    )
;

ALTER DYNAMIC TABLE REPORTING_PROD.SXF.VIEW_SM_PO_DETAIL_SIZE_LEVEL REFRESH;
GRANT SELECT ON REPORTING_PROD.SXF.VIEW_SM_PO_DETAIL_SIZE_LEVEL TO ROLE __REPORTING_PROD_SXF_R;
GRANT SELECT ON REPORTING_PROD.SXF.VIEW_SM_PO_DETAIL_SIZE_LEVEL TO ROLE __REPORTING_PROD_SXF_RW;
GRANT SELECT ON REPORTING_PROD.SXF.VIEW_SM_PO_DETAIL_SIZE_LEVEL TO ROLE __REPORTING_PROD_SXF_RWC;
GRANT OWNERSHIP ON REPORTING_PROD.SXF.VIEW_SM_PO_DETAIL_SIZE_LEVEL TO ROLE SYSADMIN COPY CURRENT GRANTS;
