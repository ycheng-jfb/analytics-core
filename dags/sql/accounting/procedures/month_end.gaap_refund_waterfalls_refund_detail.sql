SET process_to_date = DATE_TRUNC(MONTH, CURRENT_DATE);

CREATE OR REPLACE TEMPORARY TABLE _shipped_order_metrics AS
SELECT *
FROM month_end.gaap_refund_waterfalls_order_breakout_detail;

--keeping the structure for the refund part
ALTER TABLE _shipped_order_metrics
DROP COLUMN individual_bu, STORE_ID, SUBTOTAL, discount, tax, shipping, credit, actual_ship_date;


CREATE OR REPLACE TRANSIENT TABLE month_end.gaap_refund_waterfalls_refund_detail AS
SELECT s.*,
       r.refund_id,
       r.payment_method,
       dd.month_date                                                                            AS refund_month,
       CASE
           WHEN r.payment_method = 'store_credit' THEN 'Store Credit Refund'
           WHEN r.payment_method = 'membership_token' THEN 'Token Refund'
           WHEN r.payment_method IN ('creditcard', 'psp', 'cash') THEN 'Cash Refund'
           ELSE 'Unknown'
           END                                                                                  AS refund_type,
       datediff(MONTH, s.date_shipped, dd.month_date) + 1                                       AS monthoffset,
       CAST(zeroifnull(r.total_refund / (zeroifnull(s.rate))) - r.tax_refund AS DECIMAL(20, 6)) AS refund_total
FROM _shipped_order_metrics s
         JOIN lake_consolidated_view.ultra_merchant.refund r ON s.order_id = r.order_id
    AND r.statuscode = 4570
         JOIN edw_prod.data_model.dim_date dd ON dd.full_date = CAST(r.datetime_refunded AS DATE)
WHERE dd.month_date < $process_to_date
  AND datediff(MONTH, s.date_shipped, dd.month_date) + 1 >= 0;
-- delete the SD negative offsets

ALTER TABLE month_end.gaap_refund_waterfalls_refund_detail
    SET DATA_RETENTION_TIME_IN_DAYS = 0;

CREATE TRANSIENT TABLE IF NOT EXISTS month_end.gaap_refund_waterfalls_refund_detail_snapshot (
	individual_bu VARCHAR(61),
	credit_billing_store_id NUMBER(38,0),
	membership_store_id VARCHAR(16777216),
	membership_store VARCHAR(16777216),
	refund_type VARCHAR(19),
	order_type VARCHAR(34),
	order_id NUMBER(38,0),
	refund_id NUMBER(38,0),
	payment_method VARCHAR(25),
	date_shipped DATE,
	refund_month DATE,
	monthoffset NUMBER(10,0),
	refund_total NUMBER(20,6),
	snapshot_timestamp TIMESTAMP_LTZ(9)
);

INSERT INTO month_end.gaap_refund_waterfalls_refund_detail_snapshot
(individual_bu,
 credit_billing_store_id,
 membership_store_id,
 membership_store,
 refund_type,
 order_type,
 order_id,
 refund_id,
 payment_method,
 date_shipped,
 refund_month,
 monthoffset,
 refund_total,
 snapshot_timestamp)
SELECT NULL                 individual_bu,
       NULL                 credit_billing_store_id,
       membership_store_id,
       membership_store,
       refund_type,
       order_type,
       order_id,
       refund_id,
       payment_method,
       date_shipped,
       refund_month,
       monthoffset,
       refund_total,
       CURRENT_TIMESTAMP AS snapshot_timestamp
FROM month_end.gaap_refund_waterfalls_refund_detail;

DELETE
FROM month_end.gaap_refund_waterfalls_refund_detail_snapshot
WHERE snapshot_timestamp < DATEADD(MONTH, -18, getdate());
