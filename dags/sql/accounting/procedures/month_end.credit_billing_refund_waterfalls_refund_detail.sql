
CREATE OR REPLACE TEMP TABLE _refund_details AS
SELECT o.order_id,
       r.refund_id,
       o.store_id,
       o.individual_bu,
       CAST(COALESCE(r.datetime_refunded, r.date_added) AS DATE)                         AS date_refunded,
       r.total_refund,
       r.tax_refund,
       (r.total_refund / (1 + ZEROIFNULL(v.rate))) - r.tax_refund                        AS credit_billing_refund_as_cash,
       o.capture_payment_transaction_id,
       o.payment_method,
       CAST(DATE_TRUNC(MONTH, o.payment_month) AS DATE)                                  AS payment_month,
       CAST(DATE_TRUNC(MONTH, COALESCE(r.datetime_refunded, r.date_added)) AS DATE)      AS refund_month,
       DATEDIFF(MONTH, o.payment_month, COALESCE(r.datetime_refunded, r.date_added)) + 1 AS refund_month_offset
FROM month_end.credit_billing_refund_waterfalls_order_detail o
         LEFT JOIN lake_consolidated_view.ultra_merchant.refund r
                   ON o.order_id = r.order_id
         LEFT JOIN edw_prod.reference.vat_rate_history v
                   ON v.country_code = REPLACE(REPLACE(o.store_country_abbr, 'UK', 'GB'), 'EU', 'NL')
                       AND o.payment_month >= v.start_date
                       AND o.payment_month < v.expires_date -- Added this join for the VAT rates
WHERE 1 = 1
  AND (r.statuscode = 4570 OR r.statuscode IS NULL) -- refunded
  AND (r.payment_method IN ('psp', 'creditcard', 'cash', 'Check_request') OR r.payment_method IS NULL)
  AND EXISTS(SELECT 1
             FROM lake_consolidated_view.ultra_merchant.order_classification oc
             WHERE oc.order_id = o.order_id
               AND oc.order_type_id IN (10, 39));

CREATE OR REPLACE TEMP TABLE _credit_billing_refund_waterfalls_refund_detail AS
SELECT t.*,
       dd.month_date                                       AS new_refund_month,
       DATEDIFF(MONTH, t.payment_month, dd.month_date) + 1 AS new_refund_month_offset
FROM _refund_details t
         JOIN
         (SELECT DISTINCT month_date FROM edw_prod.data_model.dim_date) AS dd
         ON dd.month_date < CURRENT_DATE AND dd.month_date >= t.payment_month;

UPDATE _credit_billing_refund_waterfalls_refund_detail
SET refund_id                     = NULL,
    date_refunded                 = NULL,
    total_refund                  = 0,
    tax_refund                    = 0,
    credit_billing_refund_as_cash = 0
WHERE new_refund_month < refund_month;

DELETE
FROM _credit_billing_refund_waterfalls_refund_detail
WHERE refund_id IS NOT NULL
  AND DATEDIFF(MONTH, refund_month, new_refund_month) >= 1;

CREATE OR REPLACE TRANSIENT TABLE month_end.credit_billing_refund_waterfalls_refund_detail AS
SELECT order_id,
       refund_id,
       store_id,
       individual_bu,
       date_refunded,
       total_refund,
       tax_refund,
       credit_billing_refund_as_cash,
       capture_payment_transaction_id,
       payment_method,
       payment_month,
       new_refund_month        AS refund_month,
       new_refund_month_offset AS refund_month_offset
FROM _credit_billing_refund_waterfalls_refund_detail;

ALTER TABLE month_end.credit_billing_refund_waterfalls_refund_detail
    SET DATA_RETENTION_TIME_IN_DAYS = 0;

DELETE
FROM month_end.credit_billing_refund_waterfalls_refund_detail
WHERE payment_month IS NULL;

CREATE TRANSIENT TABLE IF NOT EXISTS month_end.credit_billing_refund_waterfalls_refund_detail_snapshot (
	order_id NUMBER(38,0),
	refund_id NUMBER(38,0),
	store_id NUMBER(38,0),
	individual_bu VARCHAR(61),
	date_refunded DATE,
	total_refund NUMBER(19,4),
	tax_refund NUMBER(19,4),
	credit_billing_refund_as_cash NUMBER(32,10),
	capture_payment_transaction_id NUMBER(38,0),
	payment_method VARCHAR(25),
	payment_month DATE,
	refund_month DATE,
	refund_month_offset NUMBER(10,0),
    snapshot_date DATE
);

INSERT INTO month_end.credit_billing_refund_waterfalls_refund_detail_snapshot
SELECT order_id,
       refund_id,
       store_id,
       individual_bu,
       date_refunded,
       total_refund,
       tax_refund,
       credit_billing_refund_as_cash,
       capture_payment_transaction_id,
       payment_method,
       payment_month,
       refund_month,
       refund_month_offset,
       CURRENT_TIMESTAMP AS snapshot_timestamp
FROM month_end.credit_billing_refund_waterfalls_refund_detail;

DELETE
FROM month_end.credit_billing_refund_waterfalls_refund_detail_snapshot
WHERE snapshot_timestamp < DATEADD(MONTH, -18, getdate());
