SET low_watermark_datetime = %(low_watermark)s :: TIMESTAMP_LTZ;

CREATE OR REPLACE TEMP TABLE _carrier_invoice_package_milestone_stg AS
SELECT
distinct
    stg.invoice_number,
	stg.scac,
    rc.value:package_number::string                                             package_number,
    rc.value:tracking_number::string                                            tracking_number,
    coalesce(sh.value:address_type,pc.value:address_type)::string               address_type,
    coalesce(sh.value:name,
        iff(pc.value:address_type is not null,pc.key,null))::string             name,
	coalesce(sh.value:country,pc.value:country)::string                         country,
    coalesce(sh.value:zipcode,pc.value:zipcode)::string                         zipcode,
	coalesce(rc.value:reference_id,pc2.value:reference_id)::string              reference_id,
	coalesce(rc.value:reference_qualifier,
             pc2.value:reference_qualifier)::string                             reference_qualifier,
	coalesce(rc.value:reference_description,
             pc2.value:reference_description)::string                           reference_description,
	TO_DATE(coalesce(rc.value:pod_date,pc2.value:pod_date)::string, 'YYYYMMDD') pod_date,
	coalesce(rc.value:pod_time,pc2.value:pod_time)::string                      pod_time,
	coalesce(rc.value:pod_name,pc2.value:pod_name)::string                      pod_name,
	stg.file_name,
	stg.datetime_added
FROM lake.sps.carrier_invoice stg
  ,LATERAL FLATTEN( input => package, outer => true ) rc
  ,LATERAL FLATTEN( input => rc.value:shipment, outer => true ) sh
  ,LATERAL FLATTEN( input => sh.value, outer => true ) pc
  ,LATERAL FLATTEN( input => pc.value:reference, outer => true ) pc2
  WHERE stg.meta_create_datetime > $low_watermark_datetime;



CREATE TABLE IF NOT EXISTS reporting_prod.sps.carrier_invoice_package_milestone (
	carrier_invoice_package_milestone_id number(38,0) NOT NULL Identity(1,1),
	carrier_invoice_package_shipment_id number(38,0),
	reference_id varchar(50),
	reference_qualifier varchar(50),
	reference_description varchar(80),
	pod_date timestamp_ntz(9),
	pod_time number(8,0),
	pod_name varchar(60),
	meta_create_datetime timestamp_ltz DEFAULT current_timestamp,
	meta_update_datetime timestamp_ltz DEFAULT current_timestamp,
	PRIMARY KEY (carrier_invoice_package_shipment_id)
);


MERGE INTO reporting_prod.sps.carrier_invoice_package_milestone  t
USING
(
SELECT *
FROM (
		SELECT carrier_invoice_package_shipment_id,
		stg.*,
		row_number() OVER (PARTITION BY s.carrier_invoice_package_shipment_id, stg.reference_id order BY stg.datetime_added DESC) AS rn
		FROM (SELECT
				invoice_number,
				scac,
				package_number,
				tracking_number,
				address_type,name,
				country,
				zipcode,
				reference_id,
				reference_qualifier,
				reference_description,
				pod_date,
				pod_time,
				pod_name,
				current_timestamp meta_create_datetime,
				current_timestamp meta_update_datetime,
				datetime_added
			  FROM _carrier_invoice_package_milestone_stg) stg
		JOIN (SELECT *
			  FROM (SELECT m.*,
						row_number() OVER (PARTITION BY invoice_number order BY date_current DESC) AS rn
					FROM reporting_prod.sps.carrier_invoice m
					)
			  WHERE rn=1) m
			ON stg.invoice_number = m.invoice_number
			AND stg.scac =m.scac
		JOIN reporting_prod.sps.carrier_invoice_package p ON m.carrier_invoice_id = p.carrier_invoice_id
			AND equal_null(p.package_number,stg.package_number)
			AND equal_null(p.tracking_number,stg.tracking_number)
		JOIN reporting_prod.sps.carrier_invoice_package_shipment s ON s.carrier_invoice_package_id = p.carrier_invoice_package_id
			AND equal_null(s.address_type,stg.address_type)
			AND equal_null(s.country,stg.country)
			AND equal_null(s.name,stg.name)
			AND equal_null(s.zipcode,stg.zipcode)
	) WHERE rn=1
)s ON t.carrier_invoice_package_shipment_id = s.carrier_invoice_package_shipment_id
    and equal_null(t.reference_id,s.reference_id)
WHEN NOT MATCHED THEN INSERT
	(carrier_invoice_package_shipment_id
	,reference_id
	,reference_qualifier
	,reference_description
	,pod_date
	,pod_time
	,pod_name
	,meta_create_datetime
	,meta_update_datetime)
VALUES
	(carrier_invoice_package_shipment_id
	,reference_id
	,reference_qualifier
	,reference_description
	,pod_date
	,pod_time
	,pod_name
	,meta_create_datetime
	,meta_update_datetime)
WHEN MATCHED THEN UPDATE
SET  t.carrier_invoice_package_shipment_id = s.carrier_invoice_package_shipment_id
	,t.reference_id = s.reference_id
	,t.reference_qualifier = s.reference_qualifier
	,t.reference_description = s.reference_description
	,t.pod_date = s.pod_date
	,t.pod_time = s.pod_time
	,t.pod_name = s.pod_name
	,t.meta_update_datetime = s.meta_update_datetime;
