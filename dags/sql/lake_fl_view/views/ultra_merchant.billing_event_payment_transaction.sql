CREATE OR REPLACE VIEW LAKE_FL_VIEW.ULTRA_MERCHANT.BILLING_EVENT_PAYMENT_TRANSACTION  (
	billing_event_id,
	payment_transaction_id,
	datetime_added,
	hvr_is_deleted,
    hvr_change_time,
	meta_row_source
)AS
SELECT
	billing_event_id,
	payment_transaction_id,
	datetime_added,
	hvr_is_deleted,
    hvr_change_time,
	meta_row_source
FROM LAKE_FL.ULTRA_MERCHANT.BILLING_EVENT_PAYMENT_TRANSACTION
WHERE NVL(hvr_is_deleted, 0) = 0;
