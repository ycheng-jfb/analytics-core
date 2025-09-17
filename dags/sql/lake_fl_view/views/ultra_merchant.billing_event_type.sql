CREATE OR REPLACE VIEW LAKE_FL_VIEW.ULTRA_MERCHANT.BILLING_EVENT_TYPE  (
	billing_event_type_id,
	label,
	datetime_added,
	hvr_is_deleted,
	hvr_change_time,
	meta_row_source
)AS
SELECT
    billing_event_type_id,
	label,
	datetime_added,
	hvr_is_deleted,
	hvr_change_time,
	meta_row_source
FROM LAKE_FL.ULTRA_MERCHANT.BILLING_EVENT_TYPE
WHERE NVL(hvr_is_deleted, 0) = 0;
