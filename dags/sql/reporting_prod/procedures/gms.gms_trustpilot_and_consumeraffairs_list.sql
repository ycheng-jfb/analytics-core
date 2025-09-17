--consumer affairs load
SET datetime_added  = (SELECT current_timestamp);
SET low_watermark_date  = (SELECT CAST(DATEADD(DAY,-90,$datetime_added) AS DATE));

SET today_date = (SELECT CAST(DATEADD(MONTH, DATEDIFF(MONTH, '1900-01-01',$datetime_added), '1900-01-01') AS date));
SET today_key = (SELECT CAST(replace($today_date,'-','') AS INT));

SET low_watermark_date_key = (SELECT CAST(replace($low_watermark_date,'-','') AS INT));

CREATE OR REPLACE TEMPORARY TABLE _list_segments_ca ( list_type VARCHAR,store_abbreviation VARCHAR,store_id INT,segment_count INT) ;

INSERT INTO _list_segments_ca ( list_type,store_abbreviation,store_id,segment_count) VALUES
('ConsumerAffairs', 'JF US' , 26 , 5000 ),
('ConsumerAffairs', 'JF CA' , 41 , 5000 ),
('ConsumerAffairs', 'FL US' , 52 , 5000 ),
('ConsumerAffairs', 'FL CA' , 79 , 5000 ),
('ConsumerAffairs', 'SD US' , 55 , 5000 ),
('ConsumerAffairs', 'FK US' , 46 , 5000 ),
('ConsumerAffairs', 'SX US' , 121 ,10000),
('ConsumerAffairs', 'SX CA' , 141 , 10000);

CREATE OR REPLACE TEMPORARY TABLE _customer_base AS
SELECT DISTINCT customer_id
FROM edw_prod.data_model.fact_order fo
WHERE CAST(REPLACE(DATE(CONVERT_TIMEZONE('America/Los_Angeles',fo.order_local_datetime::TIMESTAMP_TZ)),'-','') AS INT) >= $low_watermark_date_key;

CREATE OR REPLACE TEMPORARY TABLE _specialty_customer_au_ca AS
SELECT dc.customer_id,ds.store_id
FROM edw_prod.data_model.dim_customer dc join edw_prod.data_model.dim_store ds on ds.store_id=dc.store_id
WHERE specialty_country_code in ('AU','CA')  and ds.store_id in (46,52) ;

CREATE OR REPLACE TEMPORARY TABLE _consumer_affairs_list AS
SELECT
    tp.store_id,
    tp.individual_BU,
    tp.customer_id,
    tp.firstname,
    tp.lastname,
    tp.email,
    tp.phone,
    tp.campaign_name,
    tp.datetime_added,
    tp.city,
    tp.state
    FROM
    (SELECT
		s.store_id,
		s.store_name as individual_BU,
		m.customer_id,
		REPLACE(REPLACE(REPLACE(m.first_name,',',''),CHAR(13),''),CHAR(10),'') as firstname,
		REPLACE(REPLACE(REPLACE(m.last_name,',',''),CHAR(13),''),CHAR(10),'') as lastname,
		m.email,
		a.phone,
		ls.store_abbreviation  || '_' || ls.list_type || '_' || CAST($today_key as VARCHAR(8)) as campaign_name,
		$datetime_added as datetime_added,
		a.city,
		a.state,
        row_number() OVER(PARTITION BY m.store_id order by (SELECT NULL)) as rn
	FROM edw_prod.data_model.dim_customer m
	JOIN _customer_base c ON c.customer_id = m.customer_id
	JOIN lake_consolidated_view.ultra_merchant.customer oc ON oc.customer_id = m.customer_id
	JOIN edw_prod.data_model.dim_store s ON s.store_id = m.store_id
    JOIN _list_segments_ca ls ON s.store_id=ls.store_id
	LEFT JOIN lake_consolidated_view.ultra_merchant.address a ON a.address_id = oc.default_address_id
		WHERE EXISTS (
			SELECT 1
			FROM edw_prod.data_model.fact_order o
				JOIN edw_prod.data_model.dim_order_status dos ON o.order_status_key = dos.order_status_key
				JOIN edw_prod.data_model.dim_order_sales_channel dosc ON o.order_sales_channel_key=dosc.order_sales_channel_key --modified
				WHERE m.customer_id = o.customer_id
					AND dos.order_status = 'Success'
					AND dosc.order_classification_l1 = 'Product Order'
					AND replace(date(CONVERT_TIMEZONE('America/Los_Angeles',o.order_local_datetime::TIMESTAMP_TZ)),'-','') >= $low_watermark_date_key
			)
		AND NOT EXISTS
			(
				SELECT 1
				FROM reporting_base_prod.gms.jf_us_trustpilot tp
				WHERE tp.customer_id = m.customer_id
			)
		AND NOT EXISTS
			(
				SELECT 1
				FROM reporting_prod.gms.gms_trustpilot_and_consumeraffairs_list tp
				WHERE tp.customer_id = m.customer_id
			)
		AND NOT EXISTS (
			SELECT 1
			FROM _specialty_customer_au_ca ac
			WHERE ac.customer_id = m.customer_id
			)
        ) tp JOIN _list_segments_ca ls ON tp.store_id = ls.store_id AND tp.rn <= ls.segment_count;

INSERT INTO reporting_prod.gms.gms_trustpilot_and_consumeraffairs_list
SELECT
    store_id,
    individual_BU,
    customer_id,
    firstname,
    lastname,
    email,
    phone,
    campaign_name,
    datetime_added,
    city,
    state,
    hash(*) AS meta_row_hash,
    current_timestamp AS meta_create_datetime,
    current_timestamp AS meta_update_datetime
FROM _consumer_affairs_list;

CREATE OR REPLACE TEMPORARY TABLE _consumer_affairs_list_fl_au AS
	SELECT
		ac.store_id || '001' as custom_store_id,
		'Fabletics AU' as individual_BU,
		ac.customer_id,
		REPLACE(REPLACE(REPLACE(m.first_name,',',''),CHAR(13),''),CHAR(10),'') as firstname,
		REPLACE(REPLACE(REPLACE(m.last_name,',',''),CHAR(13),''),CHAR(10),'') as lastname,
		oc.email,
		a.phone,
		'FL AU' || '_' || 'ConsumerAffairs' || '_' || CAST($today_key as VARCHAR(8)) as campaign_name,
		$datetime_added as datetime_added,
		a.city,
		a.state
	FROM _specialty_customer_au_ca ac
	JOIN edw_prod.data_model.dim_customer m ON m.customer_id = ac.customer_id
	JOIN _customer_base c ON c.customer_id = m.customer_id
	JOIN lake_consolidated_view.ultra_merchant.customer oc ON oc.customer_id = ac.customer_id
	JOIN edw_prod.data_model.dim_store s ON s.store_id = m.store_id
	LEFT JOIN lake_consolidated_view.ultra_merchant.address a ON a.address_id = oc.default_address_id
	WHERE ac.store_id = 52
		AND EXISTS (
			SELECT 1
			FROM edw_prod.data_model.fact_order o
			JOIN edw_prod.data_model.dim_order_status dos ON o.order_status_key = dos.order_status_key
			JOIN edw_prod.data_model.dim_order_sales_channel dosc ON o.order_sales_channel_key=dosc.order_sales_channel_key
			WHERE m.customer_id = o.customer_id
				AND dos.order_status = 'Success'
				AND dosc.order_classification_l1='Product Order'
				AND replace(date(CONVERT_TIMEZONE('America/Los_Angeles',o.order_local_datetime::TIMESTAMP_TZ)),'-','') >= $low_watermark_date_key
			)
		AND NOT EXISTS
			(
				SELECT 1
				FROM reporting_base_prod.gms.jf_us_trustpilot tp
				WHERE tp.customer_id = m.customer_id
			)
		AND NOT EXISTS
			(
				SELECT 1
				FROM reporting_prod.gms.gms_trustpilot_and_consumeraffairs_list tp
				WHERE tp.customer_id = m.customer_id
			)
		AND NOT EXISTS (
			SELECT 1
			FROM reporting_base_prod.gms.flau_monthly_gms_list_storage au
			WHERE au.customer_id = m.customer_id
			)
            LIMIT 5000;

INSERT INTO reporting_prod.gms.gms_trustpilot_and_consumeraffairs_list
SELECT
    custom_store_id,
    individual_BU,
    customer_id,
    firstname,
    lastname,
    email,
    phone,
    campaign_name,
    datetime_added,
    city,
    state,
    hash(*) as meta_row_hash,
    current_timestamp as meta_create_datetime,
    current_timestamp as meta_update_datetime
FROM _consumer_affairs_list_fl_au;

--trust pilot load
SET datetime_added  = (SELECT current_timestamp);
SET low_watermark_date  = (select cast(DATEADD(DAY,-30,$datetime_added) as date));
SET today_date = (select cast(DATEADD(MONTH, DATEDIFF(MONTH, '1900-01-01',$datetime_added), '1900-01-01')as date));
SET today_key = (select cast(replace($today_date,'-','') as INT));
SET low_watermark_date_key = (select cast(replace($low_watermark_date,'-','') as INT));

CREATE OR REPLACE TEMPORARY TABLE _list_segments_tp ( list_type VARCHAR,store_abbreviation VARCHAR,store_id INT,segment_count INT) ;
INSERT INTO _list_segments_tp ( list_type,store_abbreviation,store_id,segment_count) VALUES
('TrustPilot', 'JF US' , 26 , 15000 ),
('TrustPilot', 'JF CA' , 41 , 15000 ),
('TrustPilot', 'FL US' , 52 , 15000 ),
('TrustPilot', 'FL CA' , 79 , 15000 ),
('TrustPilot', 'SD US' , 55 , 10000 ),
('TrustPilot', 'FK US' , 46 , 10000 ),
('TrustPilot', 'SX US' , 121 ,10000 ),
('TrustPilot', 'SX CA' , 141 ,10000 );

CREATE OR REPLACE TEMPORARY TABLE _customer_base AS
SELECT DISTINCT customer_id
FROM edw_prod.data_model.fact_order fo
WHERE CAST(REPLACE(DATE(CONVERT_TIMEZONE('America/Los_Angeles',fo.order_local_datetime::TIMESTAMP_TZ)),'-','') AS INT) >= $low_watermark_date_key;

CREATE OR REPLACE TEMPORARY TABLE _trust_pilot_list AS
SELECT  tp.store_id,
		tp.individual_BU,
		tp.customer_id,
		tp.firstname,
		tp.lastname,
		tp.email,
		tp.phone,
		tp.campaign_name,
		tp.datetime_added,
		tp.city,
		tp.state
        from  (SELECT
		s.store_id,
		s.store_name as individual_BU,
		m.customer_id,
		REPLACE(REPLACE(REPLACE(m.first_name,',',''),CHAR(13),''),CHAR(10),'') as firstname,
		REPLACE(REPLACE(REPLACE(m.last_name,',',''),CHAR(13),''),CHAR(10),'') as lastname,
		m.email,
		a.phone,
		ls.store_abbreviation  || '_' || ls.list_type || '_' || CAST($today_key as VARCHAR(8)) as campaign_name,
		$datetime_added as datetime_added,
		a.city,
		a.state,
        row_number() OVER(PARTITION BY m.store_id order by (SELECT NULL)) as rn
	FROM edw_prod.data_model.dim_customer m
	JOIN _customer_base c ON c.customer_id = m.customer_id
	JOIN lake_consolidated_view.ultra_merchant.customer oc ON oc.customer_id = m.customer_id
	JOIN edw_prod.data_model.dim_store s ON s.store_id = m.store_id
    JOIN _list_segments_tp ls ON s.store_id=ls.store_id
	LEFT JOIN lake_consolidated_view.ultra_merchant.address a ON a.address_id = oc.default_address_id
		WHERE EXISTS (
			SELECT 1
			FROM edw_prod.data_model.fact_order o
			JOIN edw_prod.data_model.dim_order_status dos ON o.order_status_key = dos.order_status_key
			JOIN edw_prod.data_model.dim_order_sales_channel dosc ON o.order_sales_channel_key=dosc.order_sales_channel_key
			WHERE m.customer_id = o.customer_id
				AND dos.order_status = 'Success'
				AND dosc.order_classification_l1='Product Order'
				AND replace(date(CONVERT_TIMEZONE('America/Los_Angeles',o.order_local_datetime::TIMESTAMP_TZ)),'-','') >= $low_watermark_date_key
			)
		AND NOT EXISTS
			(
				SELECT 1
				FROM reporting_base_prod.gms.jf_us_trustpilot tp
				WHERE tp.customer_id = m.customer_id
			)
		AND NOT EXISTS
			(
				SELECT 1
				FROM reporting_prod.gms.gms_trustpilot_and_consumeraffairs_list tp
				WHERE tp.customer_id = m.customer_id
			)
		AND NOT EXISTS (
			SELECT 1
			FROM _specialty_customer_au_ca ac
			WHERE ac.customer_id = m.customer_id
			)) tp JOIN _list_segments_tp ls ON tp.store_id = ls.store_id AND tp.rn <= ls.segment_count;

INSERT INTO reporting_prod.gms.gms_trustpilot_and_consumeraffairs_list
SELECT
    store_id,
    individual_BU,
    customer_id,
    firstname,
    lastname,
    email,
    phone,
    campaign_name,
    datetime_added,
    city,
    state,
    hash(*) as meta_row_hash,
    current_timestamp as meta_create_datetime,
    current_timestamp as meta_update_datetime
FROM _trust_pilot_list;

CREATE OR REPLACE TEMPORARY TABLE _trust_pilot_list_fl_au AS
SELECT
		ac.store_id || '001' as custom_store_id,
		'Fabletics AU' as individual_BU,
		ac.customer_id,
		REPLACE(REPLACE(REPLACE(m.first_name,',',''),CHAR(13),''),CHAR(10),'') as firstname,
		REPLACE(REPLACE(REPLACE(m.last_name,',',''),CHAR(13),''),CHAR(10),'') as lastname,
		oc.email,
		a.phone,
		'FL AU' || '_' || 'TrustPilot' || '_' || CAST($today_key as VARCHAR(8)) as campaign_name,
		$datetime_added as datetime_added,
		a.city,
		a.state
FROM _specialty_customer_au_ca ac
JOIN edw_prod.data_model.dim_customer m ON m.customer_id = ac.customer_id
JOIN _customer_base c ON c.customer_id = m.customer_id
JOIN lake_consolidated_view.ultra_merchant.customer oc ON oc.customer_id = ac.customer_id
JOIN edw_prod.data_model.dim_store s ON s.store_id = m.store_id
LEFT JOIN lake_consolidated_view.ultra_merchant.address a ON a.address_id = oc.default_address_id
WHERE ac.store_id = 52
		AND EXISTS (
			SELECT 1
			FROM edw_prod.data_model.fact_order o
			JOIN edw_prod.data_model.dim_order_status dos ON o.order_status_key = dos.order_status_key
			JOIN edw_prod.data_model.dim_order_sales_channel dosc ON o.order_sales_channel_key=dosc.order_sales_channel_key
			WHERE m.customer_id = o.customer_id
				AND dos.order_status = 'Success'
				AND dosc.order_classification_l1='Product Order'
				AND replace(date(CONVERT_TIMEZONE('America/Los_Angeles',o.order_local_datetime::TIMESTAMP_TZ)),'-','') >= $low_watermark_date_key
			)
		AND NOT EXISTS
			(
				SELECT 1
				FROM reporting_base_prod.gms.jf_us_trustpilot tp
				WHERE tp.customer_id = m.customer_id
			)
		AND NOT EXISTS
			(
				SELECT 1
				FROM reporting_prod.gms.gms_trustpilot_and_consumeraffairs_list tp
				WHERE tp.customer_id = m.customer_id
			)
		AND NOT EXISTS
		    (
			SELECT 1
			FROM reporting_base_prod.gms.flau_monthly_gms_list_storage au
			WHERE au.customer_id = m.customer_id
			)
LIMIT 5000;

INSERT INTO reporting_prod.gms.gms_trustpilot_and_consumeraffairs_list
SELECT
custom_store_id,
individual_BU,
customer_id,
firstname,
lastname,
email,
phone,
campaign_name,
datetime_added,
city,
state,
hash(*) AS meta_row_hash,
current_timestamp as meta_create_datetime,
current_timestamp as meta_update_datetime
from _trust_pilot_list_fl_au;
