SET low_watermark_datetime = %(low_watermark)s :: TIMESTAMP_LTZ;

CREATE OR REPLACE TEMP TABLE _carrier_invoice_package_shipment_stg AS
SELECT
	distinct
    c.invoice_number,
	c.scac,
	p.value:package_number::string                          package_number,
	p.value:tracking_number::string                         tracking_number,
	coalesce(pc.value:address_type,
             pc2.value:address_type)::string                address_type,
	coalesce(pc.value:name::string,
             iff(pc2.value:address_type is not null
                 ,pc2.key,null))::string                    name,
	coalesce(pc.value:identification_code,
             pc2.value:identification_code)::string         identification_code,
	coalesce(pc.value:contact_name_1,
             pc2.value:contact_name_1)::string              contact_name_1,
	coalesce(pc.value:contact_name_2,
             pc2.value:contact_name_2)::string              contact_name_2,
	coalesce(pc.value:address_line_1,
             pc2.value:address_line_1)::string              address_line_1,
	coalesce(pc.value:address_line_2,
             pc2.value:address_line_2)::string              address_line_2,
	coalesce(pc.value:city,pc2.value:city)::string          city,
	coalesce(pc.value:state,pc2.value:state)::string        state,
	coalesce(pc.value:zipcode,pc2.value:zipcode)::string    zipcode,
	coalesce(pc.value:country,pc2.value:country)::string    country,
	c.file_name,
	c.datetime_added
FROM lake.sps.carrier_invoice c
  ,LATERAL FLATTEN( input => package,outer=>true ) p
  ,LATERAL FLATTEN( input => p.value:shipment,outer=>true) pc
  ,LATERAL FLATTEN( input => pc.value,outer=>true) pc2
  WHERE c.meta_create_datetime > $low_watermark_datetime;

CREATE TABLE IF NOT EXISTS reporting_prod.sps.carrier_invoice_package_shipment (
	carrier_invoice_package_shipment_id number(38,0) NOT NULL Identity(1,1),
	carrier_invoice_package_id number(38,0),
	address_type varchar(50),
	name varchar(60),
	identification_code varchar(80),
	contact_name_1 varchar(60),
	contact_name_2 varchar(60),
	address_line_1 varchar(55),
	address_line_2 varchar(55),
	city varchar(30),
	state varchar(2),
	zipcode varchar(15),
	country varchar(3),
	meta_create_datetime timestamp_ltz DEFAULT current_timestamp,
	meta_update_datetime timestamp_ltz DEFAULT current_timestamp,
	PRIMARY KEY (carrier_invoice_package_id)
);

MERGE INTO reporting_prod.sps.carrier_invoice_package_shipment t
USING
(
 SELECT *
 FROM (
        SELECT
            p.carrier_invoice_package_id,
            stg.*,
            row_number() OVER (PARTITION BY CARRIER_INVOICE_PACKAGE_ID,p.package_number,
                p.tracking_number,stg.ADDRESS_TYPE,stg.name order BY stg.datetime_added DESC) AS rn
            FROM(
                SELECT
                    invoice_number,
                    scac,
                    package_number,
                    tracking_number,
                    address_type,
                    name,
                    identification_code,
                    contact_name_1,
                    contact_name_2,
                    address_line_1,
                    address_line_2,
                    city,
                    state,
                    zipcode,
                    country,
                    current_timestamp meta_create_datetime,
                    current_timestamp meta_update_datetime,
                    datetime_added
                FROM _carrier_invoice_package_shipment_stg)stg
            JOIN (SELECT *
                  FROM (SELECT row_number() OVER (PARTITION BY invoice_number order BY date_current DESC) AS rn,
                        m.*
                        FROM reporting_prod.sps.carrier_invoice m
                        )WHERE rn = 1) m
                ON stg.invoice_number = m.invoice_number
                AND stg.scac = m.scac
            JOIN reporting_prod.sps.carrier_invoice_package p ON m.carrier_invoice_id = p.carrier_invoice_id
                AND equal_null(p.package_number,stg.package_number)
                AND equal_null(p.tracking_number,stg.tracking_number)
    ) WHERE rn=1
)s ON t.carrier_invoice_package_id = s.carrier_invoice_package_id
    and equal_null(t.address_type,s.address_type)
    and equal_null(t.name,s.name)
WHEN NOT MATCHED THEN INSERT
	(carrier_invoice_package_id
	,address_type
	,name
	,identification_code
	,contact_name_1
	,contact_name_2
	,address_line_1
	,address_line_2
	,city
	,state
	,zipcode
	,country
	,meta_create_datetime
	,meta_update_datetime
	)
VALUES
	(carrier_invoice_package_id
	,address_type
	,name
	,identification_code
	,contact_name_1
	,contact_name_2
	,address_line_1
	,address_line_2
	,city
	,state
	,zipcode
	,country
	,meta_create_datetime
	,meta_update_datetime)
WHEN MATCHED THEN UPDATE
	SET t.carrier_invoice_package_id  = s.carrier_invoice_package_id
	,t.address_type  = s.address_type
	,t.name  = s.name
	,t.identification_code  = s.identification_code
	,t.contact_name_1 = s.contact_name_1
	,t.contact_name_2 = s.contact_name_2
	,t.address_line_1 = s.address_line_1
	,t.address_line_2 = s.address_line_2
	,t.city = s.city
	,t.state = s.state
	,t.zipcode = s.zipcode
	,t.country = s.country
	,t.meta_update_datetime = s.meta_update_datetime;
