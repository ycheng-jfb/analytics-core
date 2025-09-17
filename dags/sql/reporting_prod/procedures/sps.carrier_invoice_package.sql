SET low_watermark_datetime = %(low_watermark)s :: TIMESTAMP_LTZ;

CREATE OR REPLACE TEMP TABLE _carrier_invoice_package_stg AS
SELECT
	c.invoice_number,
	c.scac,
	p.value:package_number::string package_number,
	p.value:tracking_number::string tracking_number,
	p.value:description::string description,
	p.value:date::string date,
	p.value:packaging_type::string packaging_type,
	p.value:weight::string weight,
	p.value:weight_measurement::string weight_measurement,
	p.value:quantity::string quantity,
	p.value:packaging_form::string packaging_form,
	p.value:weight_unit_code::string weight_unit_code,
	p.value:charge_count::string charge_count,
	p.value:length::string length,
	p.value:width::string width,
	p.value:height::string height,
	p.value:dimension_measurement::string dimension_measurement,
	p.value:remarks_1::string remarks_1,
	p.value:remarks_2::string remarks_2,
	c.file_name,
	c.datetime_added
FROM lake.sps.carrier_invoice c
  ,LATERAL FLATTEN( input => package ) p
  WHERE c.meta_create_datetime > $low_watermark_datetime;


MERGE INTO reporting_prod.sps.carrier_invoice_package t
USING
(
	SELECT
	    carrier_invoice_id,stg.*
	FROM (SELECT invoice_number,scac,
			package_number,
			tracking_number,
			description,
			to_date(date,'yyyymmdd') date,
			packaging_type,
			weight,
			weight_measurement,
			quantity,
			packaging_form,
			weight_unit_code,
			charge_count,
			length,
			width,
			height,
			dimension_measurement,
			remarks_1,
			remarks_2 ,
			current_timestamp meta_create_datetime,
			current_timestamp meta_update_datetime,
            row_number() OVER (PARTITION BY invoice_number, scac, package_number, tracking_number order BY datetime_added DESC) AS rn
		FROM _carrier_invoice_package_stg)stg
	JOIN (SELECT *
		  FROM (SELECT m.*, row_number() OVER (PARTITION BY invoice_number order BY date_current DESC) AS rn
				FROM reporting_prod.sps.carrier_invoice m)
				WHERE rn=1) m
		ON stg.invoice_number = m.invoice_number
		AND stg.scac = m.scac
  WHERE stg.rn=1
)s ON t.carrier_invoice_id = s.carrier_invoice_id
	AND t.package_number = s.package_number
	AND equal_null(t.tracking_number,s.tracking_number)
WHEN NOT MATCHED THEN INSERT (carrier_invoice_id,package_number,tracking_number,description,date,packaging_type,
				weight,weight_measurement,quantity,packaging_form,weight_unit_code,charge_count,length,width,height,
				dimension_measurement,remarks_1,remarks_2,meta_create_datetime,meta_update_datetime)
VALUES (carrier_invoice_id,package_number,tracking_number,description,date,packaging_type,
	weight,weight_measurement,quantity,packaging_form,weight_unit_code,charge_count,length,width,height,
	dimension_measurement,remarks_1,remarks_2,meta_create_datetime,meta_update_datetime)
WHEN MATCHED THEN UPDATE
SET t.package_number = s.package_number
	,t.tracking_number = s.tracking_number
	,t.description = s.description
	,t.date = s.date
	,t.packaging_type = s.packaging_type
	,t.weight = s.weight
	,t.weight_measurement = s.weight_measurement
	,t.quantity = s.quantity
	,t.packaging_form = s.packaging_form
	,t.weight_unit_code = s.weight_unit_code
	,t.charge_count = s.charge_count
	,t.length = s.length
	,t.width = s.width
	,t.height = s.height
	,t.dimension_measurement = s.dimension_measurement
	,t.remarks_1 = s.remarks_1
	,t.remarks_2 = s.remarks_2
	,t.meta_update_datetime = s.meta_update_datetime;
