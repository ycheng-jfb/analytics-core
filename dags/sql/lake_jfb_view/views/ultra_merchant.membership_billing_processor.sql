CREATE OR REPLACE VIEW LAKE_JFB_VIEW.ULTRA_MERCHANT.MEMBERSHIP_BILLING_PROCESSOR  (
	membership_billing_processor_id,
	membership_id,
	billing_processor_id,
	order_id,
	retry_only,
	datetime_added,
	hvr_is_deleted,
	hvr_change_time,
	meta_row_source
)AS
SELECT
   	membership_billing_processor_id,
	membership_id,
	billing_processor_id,
	order_id,
	retry_only,
	datetime_added,
	hvr_is_deleted,
	hvr_change_time,
	meta_row_source
FROM LAKE_JFB.ULTRA_MERCHANT.MEMBERSHIP_BILLING_PROCESSOR
WHERE NVL(hvr_is_deleted, 0) = 0;
