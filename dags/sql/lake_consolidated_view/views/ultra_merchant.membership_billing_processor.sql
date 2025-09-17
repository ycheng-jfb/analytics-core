CREATE OR REPLACE VIEW LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.MEMBERSHIP_BILLING_PROCESSOR  (
	data_source_id,
	meta_company_id,
	membership_billing_processor_id,
	membership_id,
	billing_processor_id,
	order_id,
	retry_only,
	datetime_added,
	meta_original_membership_billing_processor_id,
	hvr_is_deleted,
	meta_create_datetime,
	meta_update_datetime
)AS
SELECT
   	data_source_id,
	meta_company_id,
	membership_billing_processor_id,
	membership_id,
	billing_processor_id,
	order_id,
	retry_only,
	datetime_added,
	meta_original_membership_billing_processor_id,
	hvr_is_deleted,
	meta_create_datetime,
	meta_update_datetime
FROM LAKE_CONSOLIDATED.ULTRA_MERCHANT.MEMBERSHIP_BILLING_PROCESSOR
WHERE NVL(hvr_is_deleted, 0) = 0;
