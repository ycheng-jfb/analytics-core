SET low_watermark_datetime = %(low_watermark)s :: TIMESTAMP_LTZ;

CREATE OR REPLACE TEMP TABLE _carrier_invoice_billing_stg AS
SELECT
	c.invoice_number,
	c.scac,
	a.value:entity_identifier::string entity_identifier,
	a.value:company_name::string company_name,
	a.value:contact_name::string contact_name,
	a.value:address_line_1::string address_line_1,
	a.value:address_line_2::string address_line_2,
	a.value:city::string city,
	a.value:state::string state,
	a.value:zipcode::string zipcode,
	a.value:country::string country,
	a.value:account_number::string account_number,
	file_name,
	datetime_added
FROM lake.sps.carrier_invoice c
  ,LATERAL FLATTEN( input => billing ) a
  WHERE c.meta_create_datetime > $low_watermark_datetime;


CREATE TABLE IF NOT EXISTS reporting_prod.sps.carrier_invoice_billing (
	carrier_invoice_billing_id number(38,0) NOT NULL Identity(1,1),
	carrier_invoice_id number(38,0) NOT NULL,
	entity_identifier varchar(50) NOT NULL,
	company_name varchar(60) NOT NULL,
	contact_name varchar(60),
	address_line_1 varchar(55) NOT NULL,
	address_line_2 varchar(55),
	city varchar(30),
	state varchar(100),
	zipcode varchar(15) NOT NULL,
	country varchar(100) NOT NULL,
	account_number varchar(50),
	meta_create_datetime timestamp_ltz DEFAULT current_timestamp,
	meta_update_datetime timestamp_ltz DEFAULT current_timestamp,
    PRIMARY KEY (carrier_invoice_id, entity_identifier, company_name)
);

MERGE INTO reporting_prod.sps.carrier_invoice_billing t
USING
(
	SELECT
        m.carrier_invoice_id,
        stg.*
    FROM (select *
          from (SELECT invoice_number,
                   scac,
                   entity_identifier,
                   company_name,
                   contact_name,
                   address_line_1,
                   address_line_2,
                   city,
                   state,
                   zipcode,
                   country,
                   account_number,
                   datetime_added,
                   current_timestamp  meta_create_datetime,
                   current_timestamp  meta_update_datetime,
                   row_number() OVER (PARTITION BY invoice_number, scac, entity_identifier, company_name order BY datetime_added DESC) AS rn
                FROM _carrier_invoice_billing_stg)
         Where rn=1)stg
    JOIN (SELECT *
          FROM (
                SELECT m.*, row_number() OVER (PARTITION BY invoice_number,scac order BY date_current DESC) AS rn
                FROM reporting_prod.sps.carrier_invoice m)
                WHERE rn=1) m
        ON stg.invoice_number = m.invoice_number
        AND stg.scac = m.scac
) s ON t.carrier_invoice_id = s.carrier_invoice_id
	AND t.company_name = s.company_name
	AND t.entity_identifier = s.entity_identifier
	WHEN NOT MATCHED THEN INSERT (
	carrier_invoice_id,
	entity_identifier, company_name, contact_name, address_line_1, address_line_2, city, state, zipcode, country, account_number
	,meta_create_datetime,meta_update_datetime
)
VALUES (
   carrier_invoice_id,
   entity_identifier
   ,company_name, contact_name, address_line_1, address_line_2, city, state, zipcode, country, account_number,
  meta_create_datetime,meta_update_datetime
)
WHEN MATCHED THEN UPDATE
SET t.carrier_invoice_id = s.carrier_invoice_id,
    t.entity_identifier = s.entity_identifier,
    t.company_name = s.company_name,
    t.contact_name = s.contact_name,
    t.address_line_1 = s.address_line_1,
    t.address_line_2 = s.address_line_2,
    t.city = s.city,
    t.state = s.state,
    t.zipcode = s.zipcode,
    t.country = s.country,
    t.account_number = s.account_number,
    t.meta_update_datetime = s.meta_update_datetime;
