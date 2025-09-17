CREATE OR REPLACE VIEW LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.BILLING_EVENT_TYPE  (
	billing_event_type_id,
	label,
	datetime_added,
	hvr_is_deleted,
	meta_create_datetime,
	meta_update_datetime
)AS
SELECT
    billing_event_type_id,
	label,
	datetime_added,
	hvr_is_deleted,
	meta_create_datetime,
	meta_update_datetime
FROM LAKE_CONSOLIDATED.ULTRA_MERCHANT.BILLING_EVENT_TYPE
WHERE NVL(hvr_is_deleted, 0) = 0;
