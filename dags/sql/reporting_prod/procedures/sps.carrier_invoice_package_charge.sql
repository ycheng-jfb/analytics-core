SET low_watermark_datetime = %(low_watermark)s :: TIMESTAMP_LTZ;

CREATE OR REPLACE TEMP TABLE _carrier_invoice_package_charge_stg AS
SELECT
	c.invoice_number,
	c.scac,
	p.value:package_number::string      package_number,
	p.value:tracking_number::string     tracking_number,
	pc.value:charge_amount              charge_amount,
	pc.value:charge_code                charge_code,
	pc.value:charge_description         charge_description,
	pc.value:declared_value             declared_value,
	pc.value:percent_fuel_surchage_fac  percent_fuel_surchage_fac,
	pc.value:currency_code              currency_code,
	pc.value:rate_scale                 rate_scale,
	c.file_name,
	c.datetime_added
FROM lake.sps.carrier_invoice c
  ,LATERAL FLATTEN( input => package ) p
  ,LATERAL FLATTEN( input => p.value:charge ) pc
  WHERE c.meta_create_datetime > $low_watermark_datetime;


MERGE INTO reporting_prod.sps.carrier_invoice_package_charge t
USING
( SELECT *
  FROM(
		SELECT
			p.carrier_invoice_package_id,
			stg.*,
			row_number() OVER (PARTITION BY carrier_invoice_package_id, stg.charge_code, stg.rate_scale order BY stg.datetime_added DESC) AS rn
		FROM (SELECT
				invoice_number,
				scac,
				package_number,
				tracking_number,
				charge_amount,
				charge_code,
				charge_description,
				declared_value,
				percent_fuel_surchage_fac,
				currency_code,
				rate_scale,
				current_timestamp meta_create_datetime,
				current_timestamp meta_update_datetime,
				datetime_added
			 FROM _carrier_invoice_package_charge_stg) stg
		JOIN (SELECT *
				FROM (SELECT row_number() OVER (PARTITION BY invoice_number order BY date_current DESC) AS rn,
							m.*
						FROM reporting_prod.sps.carrier_invoice m)
						WHERE rn = 1) m
			ON stg.invoice_number = m.invoice_number
			AND stg.scac = m.scac
		JOIN reporting_prod.sps.carrier_invoice_package p ON m.carrier_invoice_id = p.carrier_invoice_id
			AND equal_null(p.package_number,stg.package_number)
			AND equal_null(p.tracking_number,stg.tracking_number)
	)WHERE rn=1
)s ON t.carrier_invoice_package_id = s.carrier_invoice_package_id
	AND t.charge_code = s.charge_code
	AND t.rate_scale = s.rate_scale
WHEN NOT MATCHED THEN INSERT
	(carrier_invoice_package_id
	,charge_amount
	,charge_code
	,charge_description
	,declared_value
	,percent_fuel_surchage_fac
	,currency_code
	,rate_scale
	,meta_create_datetime
	,meta_update_datetime)
VALUES
	(carrier_invoice_package_id
	,charge_amount
	,charge_code
	,charge_description
	,declared_value
	,percent_fuel_surchage_fac
	,currency_code
	,rate_scale
	,meta_create_datetime
	,meta_update_datetime)
WHEN MATCHED THEN UPDATE
	SET t.carrier_invoice_package_id = s.carrier_invoice_package_id
	,t.charge_amount = s.charge_amount
	,t.charge_code = s.charge_code
	,t.charge_description = s.charge_description
	,t.declared_value = s.declared_value
	,t.percent_fuel_surchage_fac = s.percent_fuel_surchage_fac
	,t.currency_code = s.currency_code
	,t.rate_scale = s.rate_scale
	,t.meta_update_datetime = s.meta_update_datetime;
