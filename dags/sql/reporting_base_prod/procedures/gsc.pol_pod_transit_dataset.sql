CREATE OR REPLACE TEMP TABLE _pol as
SELECT
	p.traffic_mode,
	p.pol_city,
	p.pol_state_country,
	UPPER(TRIM(pol.label)) AS POL,
	pol.warehouse_id AS pol_warehouse_id,
	p.pod_city,
	p.pod_state_country,
	UPPER(TRIM(pod.label)) AS POD,
	pod.warehouse_id AS pod_warehouse_id,
	UPPER(TRIM(d.label)) AS destination_warehouse,
	d.warehouse_id AS destination_warehouse_id,
	p.Null_value,
	p.full_supply_chain,
	p.cargo_received,
	p.vessel_departed_no_eta,
	p.vessel_departed_with_eta,
	p.vessel_arrived,
	p.transload_departed
FROM lake_view.excel.pol_pod_transit_data AS p
LEFT JOIN lake_view.ultra_warehouse.warehouse AS pol
    ON UPPER(TRIM(P.pol_city)) = UPPER(TRIM(SPLIT_PART(pol.label, ',', 1)))
LEFT JOIN lake_view.ultra_warehouse.warehouse AS pod
    ON UPPER(TRIM(P.pod_city)) = UPPER(TRIM(SPLIT_PART(pod.label, ',', 1)))
LEFT JOIN lake_view.ultra_warehouse.warehouse AS d
	ON UPPER(TRIM(p.fc_code)) = UPPER(TRIM(LEFT(d.airport_code,3)))
;

/***************************
error capture
****************************/
INSERT INTO reporting_base_prod.gsc.pol_pod_transit_data_rejects
SELECT
    *,
	CURRENT_TIMESTAMP() as meta_create_date,
	CURRENT_TIMESTAMP() as meta_update_date
FROM _pol
WHERE pol_warehouse_id IS NULL
	OR pod_warehouse_id IS NULL
	OR destination_warehouse_id IS NULL;

insert into reporting_base_prod.gsc.pol_pod_transit_data_rejects (
    pol_city,
    pol_state_country,
    pod_city,
    traffic_mode,
    meta_create_date,
    meta_update_date
)
SELECT 'No Errors',
	'No Errors',
	'No Errors',
	'No Errors',
	CURRENT_TIMESTAMP() AS meta_create_date,
	CURRENT_TIMESTAMP() AS meta_update_date
FROM _pol
WHERE 0 = (
	SELECT count(1) AS rn
	FROM _pol
	WHERE pol_warehouse_id IS NULL
		OR pod_warehouse_id IS NULL
		OR destination_warehouse_id IS NULL
	)
LIMIT 1;

/************************
error clear
************************/
DELETE FROM  _pol
WHERE pol_warehouse_id IS NULL
	OR pod_warehouse_id IS NULL
	OR destination_warehouse_id IS NULL
;

CREATE OR REPLACE TEMP TABLE _pol_f AS
SELECT *
FROM (
	SELECT
		p.traffic_mode,
		p.pol,
		pol_warehouse_id,
		p.pod,
		pod_warehouse_id,
		destination_warehouse,
		destination_warehouse_id,
		p.null_value,
		p.full_supply_chain,
		p.cargo_received,
		p.vessel_departed_no_eta,
		p.vessel_departed_with_eta,
		p.vessel_arrived,
		p.transload_departed
	FROM _pol AS p
) p1
UNPIVOT ( Total_transit_time FOR Transit_logic
IN (
	null_value,
	full_supply_chain,
	cargo_received,
	vessel_departed_no_eta,
	vessel_departed_with_eta,
	vessel_arrived,
	transload_departed
	)
) AS m
;

UPDATE _pol_f
SET transit_logic =
	CASE
		WHEN LOWER(TRIM(transit_logic)) = 'vessel_departed_no_eta'
		THEN 'Vessel Departed No ETA'
		WHEN LOWER(TRIM(transit_logic)) = 'full_supply_chain'
		THEN 'Full Supply Chain'
		WHEN LOWER(TRIM(transit_logic)) = 'null_value'
		THEN 'Null'
		WHEN LOWER(TRIM(transit_logic)) = 'vessel_departed_with_eta'
		THEN 'Vessel Departed With ETA'
		WHEN LOWER(TRIM(transit_logic)) = 'transload_departed'
		THEN 'Transload Departed'
		WHEN LOWER(TRIM(transit_logic)) = 'vessel_arrived'
		THEN 'Vessel Arrived'
		WHEN LOWER(TRIM(transit_logic)) = 'cargo_received'
		THEN 'Cargo Received'
	ELSE UPPER(TRIM(transit_logic))
	END;

MERGE INTO reporting_base_prod.gsc.pol_pod_transit_dataset AS tgt
USING (
	SELECT
	    f.traffic_mode,
	    f.pol,
	    f.pol_warehouse_id,
	    f.pod,
	    f.pod_warehouse_id,
	    f.destination_warehouse,
	    f.destination_warehouse_id,
	    CAST(f.total_transit_time AS INT) AS total_transit_time,
	    f.transit_logic
  	FROM _pol_f AS f
) AS src
    ON tgt.traffic_mode = src.traffic_mode
    AND tgt.pol_warehouse_id = src.pol_warehouse_id
    AND tgt.pod_warehouse_id = src.pod_warehouse_id
    AND tgt.destination_warehouse_id = src.destination_warehouse_id
    AND tgt.transit_logic = src.transit_logic
WHEN MATCHED THEN UPDATE SET
    tgt.pol = src.pol,
	tgt.pod = src.pod,
	tgt.destination_warehouse = src.destination_warehouse,
	tgt.total_transit_time = src.total_transit_time,
	tgt.meta_update_datetime = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
	traffic_mode,
	pol,
	pol_warehouse_id,
	pod,
	pod_warehouse_id,
	destination_warehouse,
	destination_warehouse_id,
	transit_logic,
	total_transit_time,
	meta_create_datetime,
	meta_update_datetime
	)
VALUES (
	src.traffic_mode,
	src.pol,
	src.pol_warehouse_id,
	src.pod,
	src.pod_warehouse_id,
	src.destination_warehouse,
	src.destination_warehouse_id,
	src.transit_logic,
	src.total_transit_time,
	CURRENT_TIMESTAMP(),
	CURRENT_TIMESTAMP()
	)
;
