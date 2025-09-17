CREATE VIEW IF NOT EXISTS LAKE_VIEW.EMARSYS.EXTERNAL_EVENTS AS
SELECT
    csm.store_group,
    s.CONTACT_ID,
	s.EVENT_ID,
	s.EVENT_TIME,
	s.EVENT_TYPE_ID,
	s.CUSTOMER_ID,
	s.LOADED_AT,
	s.META_CREATE_DATETIME
FROM LAKE.EMARSYS.EXTERNAL_EVENTS s
JOIN lake.emarsys.customer_store_mapping csm on csm.customer_id = s.customer_id;
