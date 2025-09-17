CREATE OR REPLACE VIEW LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.BILLING_EVENT_PAYMENT_TRANSACTION  (
	data_source_id,
	meta_company_id,
	billing_event_id,
	payment_transaction_id,
	datetime_added,
	meta_original_billing_event_id,
	meta_original_payment_transaction_id,
	hvr_is_deleted,
	meta_create_datetime,
	meta_update_datetime
)AS
SELECT
   	data_source_id,
	meta_company_id,
	billing_event_id,
	payment_transaction_id,
	datetime_added,
	meta_original_billing_event_id,
	meta_original_payment_transaction_id,
	hvr_is_deleted,
	meta_create_datetime,
	meta_update_datetime
FROM LAKE_CONSOLIDATED.ULTRA_MERCHANT.BILLING_EVENT_PAYMENT_TRANSACTION
WHERE NVL(hvr_is_deleted, 0) = 0;
