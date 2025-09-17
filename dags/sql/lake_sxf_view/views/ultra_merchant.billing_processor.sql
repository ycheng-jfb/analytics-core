CREATE OR REPLACE VIEW LAKE_SXF_VIEW.ULTRA_MERCHANT.BILLING_PROCESSOR  (
	billing_processor_id,
	label,
	datetime_added,
	hvr_is_deleted,
	hvr_change_time,
	meta_row_source
)AS
SELECT
    billing_processor_id,
	label,
	datetime_added,
	hvr_is_deleted,
	hvr_change_time,
	meta_row_source
FROM LAKE_SXF.ULTRA_MERCHANT.BILLING_PROCESSOR
WHERE NVL(hvr_is_deleted, 0) = 0;
