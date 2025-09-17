SET target_table = 'stg.dim_store';
SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
--SET is_full_refresh = (SELECT IFF(stg.udf_get_watermark($target_table, NULL) = '1900-01-01'::TIMESTAMP_LTZ(3), TRUE, FALSE));
SET is_full_refresh = TRUE;
/*
-- Initial load / full refresh
UPDATE stg.meta_table_dependency_watermark
SET high_watermark_datetime = '1900-01-01',
    meta_update_datetime = CURRENT_TIMESTAMP()
WHERE table_name = $target_table;
*/

/*
-- Add a new field
ALTER TABLE stg.dim_store_stg ADD store_retail_status VARCHAR(50);
ALTER TABLE stg.dim_store_excp ADD store_retail_status VARCHAR(50);
ALTER TABLE stg.dim_store ADD store_retail_status VARCHAR(50);
*/

-- Use watermark variables for each dependent table to allow pruning of micro-partitions which doesn't happen with UDFs.
SET wm_lake_ultra_merchant_store = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.store'));
SET wm_lake_ultra_merchant_store_classification = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.store_classification'));
SET wm_lake_ultra_warehouse_retail_location = (SELECT stg.udf_get_watermark($target_table, 'lake.ultra_warehouse.retail_location'));
SET wm_lake_sharepoint_fl_retail_store_detail = (SELECT stg.udf_get_watermark($target_table, 'lake_view.sharepoint.fl_retail_store_detail'));
SET wm_reference_specialty_store_ids = (SELECT stg.udf_get_watermark($target_table, 'reference.specialty_store_ids'));
/*
SELECT
    $wm_lake_ultra_merchant_store,
    $wm_lake_ultra_merchant_store_classification,
    $wm_lake_ultra_warehouse_retail_location,
    $wm_lake_sharepoint_fl_retail_store_detail;
*/

CREATE OR REPLACE TEMP TABLE _dim_store__store_base (store_id INT);

-- Full Refresh
INSERT INTO _dim_store__store_base (store_id)
SELECT st.store_id
FROM (
    SELECT s.store_id
    FROM lake_consolidated.ultra_merchant.store AS s
    UNION ALL
    SELECT store_id
    FROM reference.specialty_store_ids
) AS st
WHERE $is_full_refresh = TRUE
ORDER BY st.store_id;

-- Incremental Refresh
INSERT INTO _dim_store__store_base (store_id)
SELECT DISTINCT incr.store_id
FROM (
    SELECT store_id
    FROM lake_consolidated.ultra_merchant.store
    WHERE meta_update_datetime > $wm_lake_ultra_merchant_store
    UNION ALL
    SELECT store_id
	FROM lake_consolidated.ultra_merchant.store_classification
	WHERE meta_update_datetime > $wm_lake_ultra_merchant_store_classification
    UNION ALL
    SELECT store_id
	FROM lake.ultra_warehouse.retail_location
	WHERE hvr_change_time > $wm_lake_ultra_warehouse_retail_location
	UNION ALL
	SELECT store_id
	FROM lake_view.sharepoint.fl_retail_store_detail
	WHERE meta_update_datetime > $wm_lake_sharepoint_fl_retail_store_detail
	UNION ALL
	SELECT store_id
	FROM reference.specialty_store_ids
	WHERE meta_update_datetime > $wm_reference_specialty_store_ids
    UNION ALL /* previously errored rows */
    SELECT store_id
    FROM excp.dim_store
--    WHERE
--    meta_is_current_excp AND
--    meta_data_quality = 'error'
) AS incr
WHERE NOT $is_full_refresh
ORDER BY incr.store_id;

CREATE OR REPLACE TEMP TABLE _dim_store__country_details (
    country VARCHAR(100),
    country_abbr VARCHAR(6),
    currency_code VARCHAR(3),
    time_zone_abbr VARCHAR(5),
    region VARCHAR(50),
    region_abbr VARCHAR(5)
    );

-- Populate temp table with existing country details
INSERT INTO _dim_store__country_details VALUES ('US','US-CST','USD','CST','North America','NA');
INSERT INTO _dim_store__country_details VALUES ('US','US-EST','USD','EST','North America','NA');
INSERT INTO _dim_store__country_details VALUES ('US','US','USD','PST','North America','NA');
INSERT INTO _dim_store__country_details VALUES ('Germany','DE','EUR','CET','Europe','EU');
INSERT INTO _dim_store__country_details VALUES ('UK','UK','GBP','GMT','Europe','EU');
INSERT INTO _dim_store__country_details VALUES ('Canada','CA','USD', 'PST','North America','NA'); /* switched to US on 1/1/2015 (original CAD rows are added manually) */
INSERT INTO _dim_store__country_details VALUES ('France','FR','EUR','CET','Europe','EU');
INSERT INTO _dim_store__country_details VALUES ('Spain','ES','EUR','WET','Europe','EU');
INSERT INTO _dim_store__country_details VALUES ('Italy','IT','EUR','CET','Europe','EU');
INSERT INTO _dim_store__country_details VALUES ('Netherlands','NL','EUR','CET','Europe','EU');
INSERT INTO _dim_store__country_details VALUES ('Denmark','DK','DKK','CET','Europe','EU');
INSERT INTO _dim_store__country_details VALUES ('Sweden','SE','SEK','CET','Europe','EU');
INSERT INTO _dim_store__country_details VALUES ('EU','EU','EUR','CET','Europe','EU');

-- Create store-country mapping
CREATE OR REPLACE TEMP TABLE _dim_store__store_country_mapping (
    store_id INT,
    country_abbr VARCHAR(6)
    --,time_zone varchar(100) /* for future brick-and-mortar retail stores */
    );

--Populate store-country mapping
INSERT INTO _dim_store__store_country_mapping
SELECT
    s.store_id,
    CASE
        WHEN RIGHT(sg.label, 3) LIKE ' %%' THEN RIGHT(sg.label, 2)
        WHEN rl.label IN ('The Domain') THEN 'US-CST'
        WHEN rl.label IN ('St Johns Town Center', 'Soho New York', 'Roosevelt Field', 'King of Prussia', 'Natick Mall') THEN 'US-EST'
        WHEN sg.label IN ('Just Fabulous', 'JustFabulous', 'JustFab', 'FabKids', 'Fabletics', 'ShoeDazzle') THEN 'US'
        ELSE 'US' /* Default to-be-removed stores (i.e. Sensa) */
        END AS country_abbr
    --,s.time_zone /* for future brick-and-mortar retail stores */
FROM lake_consolidated.ultra_merchant.store AS s
    JOIN lake_consolidated.ultra_merchant.store_group AS sg
        ON sg.store_group_id = s.store_group_id
    LEFT JOIN lake.ultra_warehouse.retail_location AS rl
        ON rl.store_id = s.store_id;

-- Populate lkp_store_country_mapping table with additional attributes
CREATE OR REPLACE TEMP TABLE _lkp_store_country_mapping AS
SELECT
    scm.store_id,
    cd.country,
    cd.country_abbr,
    cd.currency_code,
    cd.time_zone_abbr,
    cd.region,
    cd.region_abbr
FROM _dim_store__store_country_mapping AS scm
    JOIN _dim_store__country_details AS cd
        ON scm.country_abbr = cd.country_abbr;

CREATE OR REPLACE TEMP TABLE _retail_location_configuration AS
SELECT
    retail_location_id,
    retail_configuration_id,
    configuration_value
FROM lake.ultra_warehouse.retail_location_configuration
QUALIFY row_number() over (PARTITION BY retail_location_id, retail_configuration_id ORDER BY datetime_added DESC) = 1;

CREATE OR REPLACE TEMP TABLE _stg_store AS
WITH cte_store_group --gets the country of the store by using the last 2 letters of the store_group label
AS
(
	SELECT sg.store_group_id,
        sg.label AS store_group,
        CASE
            WHEN LEN(sg.label) - CHARINDEX(' ', sg.label) > 2 THEN 'US'
            ELSE RIGHT(sg.label, LEN(sg.label) - CHARINDEX(' ', sg.label))
        END AS country_abbr
	FROM lake_consolidated.ultra_merchant.store_group sg
),
store_brand AS --creates the initial brand logic which is used is the final select statement below
(
	SELECT store_group_id,
		store_group,
		country_abbr,
	    CASE
			WHEN store_group ILIKE 'Just%%' THEN 'JustFab'
			WHEN store_group ILIKE 'FabKids%%' THEN 'FabKids'
			WHEN store_group ILIKE 'Fabletics%%' THEN 'Fabletics'
			WHEN store_group ILIKE 'Shoe%%' THEN 'ShoeDazzle'
			WHEN store_group ILIKE 'Sav%%' THEN 'Savage X'
			WHEN store_group_id BETWEEN 1 AND 8 THEN 'Legacy'
			ELSE 'Unknown'
		END AS brand_name,
		CASE
			WHEN store_group ILIKE 'Just%%' THEN 'JF'
			WHEN store_group ILIKE 'FabKids%%' THEN 'FK'
			WHEN store_group ILIKE 'Fabletics%%' THEN 'FL'
			WHEN store_group ILIKE 'Shoe%%' THEN 'SD'
			WHEN store_group ILIKE 'Sava%%' THEN 'SX'
			WHEN store_group_id BETWEEN 1 AND 8 THEN 'LGCY'
			ELSE 'Unknown'
        END AS brand_abbr
	FROM cte_store_group
),
non_retail_mobile_stores AS (
    SELECT base.store_id
    FROM _dim_store__store_base AS base
        JOIN (SELECT DISTINCT store_id FROM lake_consolidated.ultra_merchant.store_classification) AS sc
            ON sc.store_id = base.store_id
    WHERE base.store_id NOT IN (SELECT store_id FROM lake_consolidated.ultra_merchant.store_classification WHERE store_type_id IN (6,8))
)
SELECT s.store_id,
	REPLACE(REPLACE(s.label, 'JustFabulous', 'JustFab'),'Just Fabulous', 'JustFab') AS store_full_name,
	sb.store_group_id,
	REPLACE(REPLACE(store_group, 'JustFabulous', 'JustFab'),'Just Fabulous', 'JustFab') AS store_group,
	CASE
	    WHEN sr.store_type_id = 6 OR s.store_id = 56 THEN 'Retail'
	    WHEN sr.store_type_id = 8 THEN 'Mobile App'
	    WHEN s.label ilike 'GRP-%' THEN 'Group Order'
	    WHEN nrms.store_id IS NOT NULL THEN 'Online'
	    WHEN sb.brand_name = 'Legacy' THEN 'Online'
	    WHEN s.label ilike '%sample request%' THEN 'Online'
	    WHEN s.store_id IN (43,44,116) THEN 'Online'
	    ELSE 'Retail'
	END store_type,
    CASE
        WHEN s.code ILIKE '%%varsity%%' THEN 'Varsity'
        WHEN s.code ILIKE '%%kiosk%%' OR s.label ILIKE '%legging bar%' THEN 'Legging Bar'
        WHEN s.code ILIKE '%%toughmudder%%' THEN 'Tough Mudder'
        WHEN s.code ILIKE '%%avp%%' THEN 'AVP'
        WHEN s.code ILIKE '%%retail%%' THEN 'Store'
        WHEN s.code ILIKE '%%sxrt%%' OR s.store_id = 56 THEN 'Store'
        WHEN sr.store_type_id = 6 THEN 'Store'
        WHEN s.code ILIKE '%%App%%' OR sr.store_type_id = 8 THEN 'Mobile App'
        WHEN nrms.store_id IS NOT NULL OR s.label ilike any ('GRP-%', '%sample request%') OR sb.brand_name = 'Legacy' THEN 'Browser'
        WHEN s.store_id IN (43,44,116) THEN 'Browser'
        ELSE 'Store'
    END AS store_sub_type,
    CASE
        WHEN s.label ILIKE '%%(DM)%%' OR s.label ILIKE '%%sample%%' OR s.label ILIKE '%%swag%%'
        OR s.label ILIKE '%%derma%%' OR s.label ILIKE '%%beauty%%' OR s.label ILIKE 'JustFab - Heels.com Drop Ship'
        OR sb.brand_name = 'Legacy' OR s.label ILIKE 'JustFab - Retail Replen' THEN 0
        ELSE 1
    END AS is_core_store,
	CASE
		WHEN sb.brand_name IN ('JustFab', 'ShoeDazzle') THEN 'Fast Fashion'
		WHEN sb.brand_name IN ('Fabletics','FabKids') THEN sb.brand_name
		WHEN sb.brand_name IN ('Savage X') THEN 'Savage X'
		ELSE 'Unknown'
	END store_division,
	CASE
		WHEN sb.brand_name IN ('JustFab', 'ShoeDazzle') THEN 'FF'
		WHEN sb.brand_name = 'Fabletics' THEN 'FL'
		WHEN sb.brand_name = 'FabKids' THEN 'FK'
		WHEN sb.brand_name = 'Savage X' THEN 'SX'
		ELSE 'Unknown'
	END store_division_abbr,
	IFF(s.code = 'yitty', 'Yitty', sb.brand_name) AS store_brand,
	IFF(s.code = 'yitty', 'YTY', sb.brand_abbr) AS store_brand_abbr,
	scm.region_abbr AS store_region,
	LEFT(scm.country_abbr,2) AS store_country,
	scm.currency_code AS store_currency,
	st.time_zone AS store_time_zone,
	CASE
	    WHEN s.store_id = 54 THEN 'CA'
	    ELSE COALESCE(sr.retail_state,'N/A')
	END AS store_retail_state,
	CASE
	    WHEN s.store_id = 54 THEN 'Glendale'
	    ELSE COALESCE(sr.retail_city,'N/A')
	END AS store_retail_city,
	CASE
	    WHEN s.store_id = 54 THEN 'Glendale Galleria'
	    ELSE COALESCE(sr.retail_location,'N/A')
	END AS store_retail_location,
	CASE
	    WHEN s.store_id = 54 THEN '91210'
	    ELSE COALESCE(sr.retail_zip_code,'N/A')
	END AS store_retail_zip_code,
    COALESCE(rlc.retail_location_code, 'N/A') AS retail_location_code,
    COALESCE(sr.retail_status, 'N/A') AS retail_status,
    COALESCE(sr.retail_region, 'N/A') as retail_region,
    COALESCE(sr.retail_district, 'N/A') as retail_district,
	$execution_start_time AS meta_create_datetime,
	$execution_start_time AS meta_update_datetime
FROM lake_consolidated.ultra_merchant.store s
JOIN _dim_store__store_base AS base ON base.store_id = s.store_id
LEFT JOIN store_brand sb ON s.store_group_id = sb.store_group_id
LEFT JOIN lake_consolidated.ultra_merchant.store_language sl ON sl.store_id = s.store_id
LEFT JOIN _lkp_store_country_mapping scm ON s.store_id = scm.store_id
LEFT JOIN (SELECT DISTINCT retail_location_code, store_id FROM lake.ultra_warehouse.retail_location) AS rlc
    ON rlc.store_id = s.store_id
LEFT JOIN reference.store_timezone st ON st.store_id = s.store_id
LEFT JOIN non_retail_mobile_stores AS nrms ON nrms.store_id = base.store_id
LEFT JOIN
	(
		SELECT DISTINCT
			sc.store_id,
			sc.store_type_id,
			CAST(rl.state AS VARCHAR(5)) AS retail_state,
			rl.city AS retail_city,
			rl.label AS retail_location,
			rl.zip AS retail_zip_code,
		    rsd.store_status AS retail_status,
            COALESCE(rlc_region.configuration_value, rlc_region_default.default_value) as retail_region,
            COALESCE(rlc_district.configuration_value, rlc_district_default.default_value) as retail_district
		FROM lake_consolidated.ultra_merchant.store_classification AS sc
		JOIN lake_consolidated.ultra_merchant.store_type AS st
			ON sc.store_type_id = st.store_type_id
		LEFT JOIN lake.ultra_warehouse.retail_location AS rl
			ON rl.store_id = sc.store_id
		LEFT JOIN lake_view.sharepoint.fl_retail_store_detail AS rsd
		    ON rsd.store_id = sc.store_id
        LEFT JOIN _retail_location_configuration rlc_region
            ON rlc_region.retail_location_id = rl.retail_location_id AND rlc_region.retail_configuration_id = 35
        LEFT JOIN lake.ultra_warehouse.retail_configuration rlc_region_default
            ON rlc_region_default.retail_configuration_id = 35 AND rl.retail_location_id IS NOT NULL
        LEFT JOIN _retail_location_configuration rlc_district
            ON rlc_district.retail_location_id = rl.retail_location_id AND rlc_district.retail_configuration_id = 18
        LEFT JOIN lake.ultra_warehouse.retail_configuration rlc_district_default
            ON rlc_district_default.retail_configuration_id = 18 AND rl.retail_location_id IS NOT NULL
		WHERE sc.store_type_id in (6,8)
	)sr
	ON s.store_id = sr.store_id;
-- SELECT * FROM _stg_store;

/* Inserting filler store_ids for YITTY mobile app (24101) and FL US Media (9999) */
INSERT INTO _stg_store
(
    store_id,
    store_full_name,
    store_group_id,
    store_group,
    store_type,
    store_sub_type,
    is_core_store,
    store_division,
    store_division_abbr,
    store_brand,
    store_brand_abbr,
    store_region,
    store_country,
    store_currency,
    store_time_zone,
    store_retail_state,
    store_retail_city,
    store_retail_location,
    store_retail_zip_code,
    retail_location_code,
    retail_status,
    retail_region,
    retail_district,
    meta_create_datetime,
    meta_update_datetime
)

SELECT
    ssi.store_id,
    ssi.store_full_name,
    ssi.store_group_id,
    ssi.store_group,
    ssi.store_type,
    ssi.store_sub_type,
    IFF(ssi.is_core_store = TRUE,1,0) AS is_core_store,
    ssi.store_division,
    ssi.store_division_abbr,
    ssi.store_brand,
    ssi.store_brand_abbr,
    ssi.store_region,
    ssi.store_country,
    ssi.store_currency,
    ssi.store_time_zone,
    ssi.store_retail_state,
    ssi.store_retail_city,
    ssi.store_retail_location,
    ssi.store_retail_zip_code,
    ssi.retail_location_code,
    ssi.retail_status,
    ssi.retail_region,
    ssi.retail_district,
    $execution_start_time,
    $execution_start_time
FROM _dim_store__store_base AS sb
    JOIN reference.specialty_store_ids AS ssi
        ON ssi.store_id = sb.store_id;


INSERT INTO stg.dim_store_stg
(
    store_id,
	store_group_id,
	store_group,
	store_name,
	store_name_region,
    store_full_name,
	store_type,
    store_sub_type,
    is_core_store,
	store_division,
	store_division_abbr,
	store_brand,
	store_brand_abbr,
	store_region,
	store_country,
	store_currency,
	store_time_zone,
	store_retail_state,
	store_retail_city,
	store_retail_location,
	store_retail_zip_code,
    store_retail_location_code,
    store_retail_status,
    store_retail_region,
    store_retail_district,
    company_id,
	meta_create_datetime,
	meta_update_datetime
)
SELECT
	store_id,
	store_group_id,
	store_group,
    store_brand || COALESCE(' ' || IFF(store_country='EU', 'EUREM', store_country),'') AS store_name,
	store_brand || COALESCE(' ' || store_region,'') AS store_name_region,
    CASE
        WHEN store_type='Retail' AND store_full_name LIKE '%%RTL-%%' THEN REPLACE(store_full_name,'RTL-','')
        WHEN store_country='US' AND store_full_name IN ('JustFab','FabKids','Fabletics','ShoeDazzle','Savage X','Yitty') THEN store_full_name || ' ' || store_country
        ELSE store_full_name
        END AS store_full_name,
	store_type,
    store_sub_type,
    is_core_store,
	store_division,
	store_division_abbr,
	store_brand,
	store_brand_abbr,
	store_region,
	IFF(store_country='EU', 'EUREM', store_country) AS store_country,
	store_currency,
	store_time_zone,
	store_retail_state,
	store_retail_city,
	store_retail_location,
	store_retail_zip_code,
    retail_location_code AS store_retail_location_code,
    retail_status        AS store_retail_status,
    retail_region        AS store_retail_region,
    retail_district      AS store_retail_district,
    CASE
        WHEN lower(store_brand) IN ('justfab', 'fabkids', 'shoedazzle') THEN 10
        WHEN lower(store_brand) IN ('fabletics', 'yitty') THEN 20
        WHEN lower(store_brand) IN ('savage x') THEN 30
        WHEN lower(store_brand) IN ('legacy') THEN 40
        END              AS company_id,
    meta_create_datetime,
    meta_update_datetime
FROM _stg_store;
