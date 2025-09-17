BEGIN TRANSACTION NAME RMA_DETAIL_DATASET;

TRUNCATE TABLE REPORTING_PROD.GSC.RMA_DETAIL_DATASET;

INSERT INTO REPORTING_PROD.GSC.RMA_DETAIL_DATASET
(ORDER_ID, RMA_ID, RETURN_REASON, COUNTRY_CODE, CUSTOMER_ID, CUSTOMER_TYPE, LPN, RETURN_DAYS,REASON_COMMENT)
select
	r.order_id,
	r.rma_id,
	rr.label as return_reason,
	a.country_code,
	o.customer_id,
	fme.membership_state as customer_type,
	coalesce(ol.lpn_code,rp.lpn_code) as LPN,
    return_days,
    rp.reason_comment
from
	lake_consolidated_view.ultra_merchant.rma r
	left join lake_consolidated_view.ultra_merchant.rma_product rp
	on r.rma_id = rp.rma_id
	left join lake_consolidated_view.ultra_merchant.order_line ol
	on rp.order_line_id = ol.order_line_id
	left join lake_consolidated_view.ultra_merchant.return_reason rr
	on rp.return_reason_id = rr.return_reason_id
	left join lake_consolidated_view.ultra_merchant."ORDER" o
	on r.order_id = o.order_id
	left join lake_consolidated_view.ultra_merchant.address a
	on o.shipping_address_id = a.address_id
    --customer type logic below
    left join EDW_PROD.DATA_MODEL.FACT_MEMBERSHIP_EVENT fme
    on fme.CUSTOMER_ID = o.CUSTOMER_ID
    and fme.EVENT_START_LOCAL_DATETIME <= o.DATETIME_ADDED
    and fme.EVENT_END_LOCAL_DATETIME > o.DATETIME_ADDED
where
	r.datetime_added > '2019-01-01'
;
COMMIT;
