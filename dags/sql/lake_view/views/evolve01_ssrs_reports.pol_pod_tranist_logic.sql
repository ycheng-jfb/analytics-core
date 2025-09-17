CREATE VIEW lake_view.evolve01_ssrs_reports.pol_pod_tranist_logic(
	TRAFFIC_MODE,
	POL,
	POL_WAREHOUSE_ID,
	POD,
	POD_WAREHOUSE_ID,
	DESTINATION,
	TOTAL_TRANSIT_TIME,
	DESTINATION_WAREHOUSE_ID,
	TRANSIT_LOGIC,
	DATETIME_ADDED,
	DATETIME_MODIFIED,
	POL_POD_TRANIST_LOGIC_ID,
	META_CREATE_DATETIME,
	META_UPDATE_DATETIME
) as


SELECT
    traffic_mode,
    trim(trim(pol,char(9))),
    pol_warehouse_id,
    pod,
    pod_warehouse_id,
    destination,
    total_transit_time,
    destination_warehouse_id,
    transit_logic,
    datetime_added,
    datetime_modified,
    pol_pod_tranist_logic_id,
    meta_create_datetime,
    meta_update_datetime
FROM lake.evolve01_ssrs_reports.pol_pod_tranist_logic;
