CREATE TABLE IF NOT EXISTS reporting_base_prod.data_science.zip_50_miles_fl_retail
(
customer_state	VARCHAR(10),
customer_zip	VARCHAR(10),
customer_state_zip	VARCHAR(150),
store_state	VARCHAR(10),
store_zip	VARCHAR(10),
store_state_zip	VARCHAR(150),
miles	FLOAT,
row_num	NUMBER(18,0)
);

CREATE OR REPLACE TEMPORARY TABLE _customer_zip AS
SELECT DISTINCT
    state AS customer_state
    ,zip AS customer_zip
    ,state || ' ' || CAST(zip AS VARCHAR(100)) AS customer_state_zip
    ,latitude AS customer_latitude
	,longitude AS customer_longitude
FROM lake_fl_view.ultra_merchant.zip_city_state;

CREATE OR REPLACE TEMPORARY TABLE _retail_zip AS
SELECT DISTINCT
    state AS store_state
    ,zip AS store_zip
    ,state || ' ' || CAST(zip AS VARCHAR(100)) AS store_state_zip
    ,latitude AS store_latitude
	,longitude AS store_longitude
FROM lake_fl_view.ultra_merchant.zip_city_state
WHERE
    state || ' ' || CAST(zip AS VARCHAR(100))
    IN (SELECT store_retail_state || ' ' || CAST(store_retail_zip_code AS VARCHAR(100))
        FROM EDW_PROD.DATA_MODEL_FL.DIM_STORE
		WHERE store_type = 'Retail'
			AND store_group = 'Fabletics'
		);

CREATE OR REPLACE TEMPORARY TABLE _zip_combination AS
SELECT
	cz.*
	,dz.*
FROM _customer_zip cz
JOIN _retail_zip dz
	ON 1 = 1;

TRUNCATE TABLE reporting_base_prod.DATA_SCIENCE.zip_50_miles_fl_retail;
INSERT INTO reporting_base_prod.DATA_SCIENCE.zip_50_miles_fl_retail
SELECT DISTINCT
	zc.customer_state
	,zc.customer_zip
	,zc.customer_state_zip
	,zc.store_state
	,zc.store_zip
	,zc.store_state_zip
	,CASE
		WHEN ( SIN ( zc.store_latitude / 57.2958 ) * SIN ( zc.customer_latitude / 57.2958 )) + ( COS ( zc.store_latitude / 57.2958 ) * COS ( zc.customer_latitude /57.2958 ) * COS ( zc.customer_longitude / 57.2958 - zc.store_longitude / 57.2958 )) >= 1
		THEN 0
		ELSE ROUND (( ACOS (( SIN ( zc.store_latitude / 57.2958 ) * SIN ( zc.customer_latitude / 57.2958 )) + ( COS ( zc.store_latitude / 57.2958 ) * COS ( zc.customer_latitude /57.2958 ) * COS ( zc.customer_longitude / 57.2958 - zc.store_longitude / 57.2958 )))) * 3963 , 2 )
		END AS miles
  	,ROW_NUMBER() over (ORDER BY zc.customer_zip) as row_num
FROM _zip_combination zc
WHERE
	CASE
		WHEN ( SIN ( zc.store_latitude / 57.2958 ) * SIN ( zc.customer_latitude / 57.2958 )) + ( COS ( zc.store_latitude / 57.2958 ) * COS ( zc.customer_latitude /57.2958 ) * COS ( zc.customer_longitude / 57.2958 - zc.store_longitude / 57.2958 )) >= 1
		THEN 0
		ELSE ROUND (( ACOS (( SIN ( zc.store_latitude / 57.2958 ) * SIN ( zc.customer_latitude / 57.2958 )) + ( COS ( zc.store_latitude / 57.2958 ) * COS ( zc.customer_latitude /57.2958 ) * COS ( zc.customer_longitude / 57.2958 - zc.store_longitude / 57.2958 )))) * 3963 , 2 )
		END <= 50
ORDER BY zc.customer_zip;
