BEGIN
TRANSACTION NAME RETURN_DETAIL_DATASET;

TRUNCATE TABLE REPORTING_PROD.GSC.RETURN_DETAIL_DATASET;

INSERT INTO REPORTING_PROD.GSC.RETURN_DETAIL_DATASET
(RETURN_ID, RETURN_DATE, RETURN_DATE_YEAR, RETURN_DATE_MONTH, RETURN_DATE_ID, RETURN_FACILITY, IN_STORE_RETURN, RMA_ID,
 ORDER_ID, LPN, RETURN_REASON, DISPOSITION, RETURN_RESOLVED_DATE, RMA_SOURCE)
select r.return_id,
       TO_DATE(r.datetime_added) as RETURN_DATE, YEAR (r.datetime_added) AS RETURN_DATE_YEAR, DATE (date_trunc('MONTH', r.datetime_added)) AS RETURN_DATE_MONTH, TO_VARCHAR(r.datetime_added, 'YYYYMM') AS RETURN_DATE_ID, w.label as RETURN_FACILITY, CASE WHEN w.region_id = 8 THEN 'Y' ELSE 'N'
END as IN_STORE_RETURN,
    rma.rma_id,
    rma.order_id,
    rp.lpn_code as LPN,
    rdd.return_reason,
    rd.label as DISPOSITION,
    to_date(r.datetime_resolved) as RETURN_RESOLVED_DATE,
    rs.label as rma_source
from
	lake_consolidated_view.ultra_merchant.return r
    left join lake_consolidated_view.ultra_merchant.rma rma
    on r.rma_id = rma.rma_id
    left join lake_consolidated_view.ultra_merchant.return_product rp
	on r.return_id = rp.return_id
	left join lake_consolidated_view.ultra_merchant.return_disposition rd
	on rp.return_disposition_id = rd.return_disposition_id
    left join lake_consolidated_view.ultra_merchant.return_batch rb
    on r.return_batch_id = rb.return_batch_id
    left join lake_view.ultra_warehouse.warehouse w
    on rb.warehouse_id = w.warehouse_id
    left join reporting_prod.gsc.rma_detail_dataset rdd
	on r.rma_id = rdd.rma_id
    and rp.lpn_code = rdd.lpn
    left join lake_consolidated_view.ultra_merchant.rma_source rs
        on rma.rma_source_id = rs.rma_source_id
where
	r.datetime_added > '2019-01-01';

COMMIT;
