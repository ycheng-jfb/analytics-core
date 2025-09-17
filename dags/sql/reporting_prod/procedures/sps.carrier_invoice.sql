SET low_watermark_datetime = %(low_watermark)s :: TIMESTAMP_LTZ;

CREATE TABLE IF NOT EXISTS reporting_prod.sps.carrier_invoice (
	carrier_invoice_id number(38,0) NOT NULL Identity(1,1),
	invoice_number varchar(22) NOT NULL,
	payment_method varchar(50),
	invoice_date date NOT NULL,
	net_amount_due number(12,2) NOT NULL,
	invoice_type varchar(50),
	scac varchar(4) NOT NULL,
	date_current date,
	settlement_option varchar,
	currency_code varchar(3) NOT NULL,
	terms_type varchar(50),
	terms_basis_date varchar(50),
	terms_net_due_date date,
	terms_net_days number(3,0),
	invoice_reference varchar(50),
	invoice_note varchar(50),
	account_number varchar(50),
	package_count number(15,0) NOT NULL,
	total_charged number(12,2),
	sac_code varchar(50),
    datetime_added timestamp_ntz(9),
    meta_create_datetime timestamp_ltz DEFAULT current_timestamp,
	meta_update_datetime timestamp_ltz DEFAULT current_timestamp,
    PRIMARY KEY (invoice_number, scac)
);


MERGE INTO reporting_prod.sps.carrier_invoice t
USING
(
	SELECT *
	FROM
	   (SELECT stg.invoice_number,
			payment_method,
			invoice_date,
			sums.net_amount_due,
			invoice_type,
			stg.scac,
			date_current,
			settlement_option,
			currency_code,
			terms_type,
			terms_basis_date,
			terms_net_due_date ,
			terms_net_days,
			invoice_reference,
			invoice_note,
			account_number,
			sums.package_count,
			sums.total_charged,
			sac_code,
			current_timestamp  meta_create_datetime,
			current_timestamp  meta_update_datetime,
			datetime_added,
            row_number() OVER (PARTITION BY stg.invoice_number, stg.scac order BY date_current DESC) AS rn
		FROM lake.sps.carrier_invoice stg
		JOIN (SELECT invoice_number,
				scac,
				sum(total_charged) AS total_charged,
				sum(package_count) AS package_count,
				sum(net_amount_due) AS net_amount_due
			  FROM lake.sps.carrier_invoice a
			  WHERE meta_create_datetime > $low_watermark_datetime
			  GROUP BY invoice_number,scac) sums
			ON equal_null(stg.invoice_number,sums.invoice_number)
			AND equal_null(stg.scac,sums.scac)
	   ) WHERE rn=1 AND meta_create_datetime > $low_watermark_datetime
) s ON equal_null(t.invoice_number,s.invoice_number)
	AND equal_null(t.scac,s.scac)
WHEN NOT MATCHED THEN INSERT (invoice_number, payment_method, invoice_date, net_amount_due, invoice_type, scac,date_current,
	settlement_option, currency_code,terms_type, terms_basis_date, terms_net_due_date,
	terms_net_days, invoice_reference, invoice_note, account_number, package_count,
	total_charged, sac_code,meta_create_datetime, meta_update_datetime, datetime_added
	)
VALUES (
	invoice_number, payment_method, invoice_date, net_amount_due, invoice_type, scac, date_current, settlement_option,
	currency_code, terms_type, terms_basis_date, terms_net_due_date, terms_net_days, invoice_reference, invoice_note, account_number,
	package_count, total_charged, sac_code, meta_create_datetime, meta_update_datetime, datetime_added
)
WHEN MATCHED AND s.datetime_added > t.datetime_added THEN UPDATE
SET t.payment_method = s.payment_method,
    t.invoice_date = s.invoice_date,
    t.net_amount_due = t.net_amount_due + s.net_amount_due,
    t.invoice_type = s.invoice_type,
    t.date_current = s.date_current,
    t.settlement_option = s.settlement_option,
    t.currency_code = s.currency_code,
    t.terms_type = s.terms_type,
    t.terms_basis_date = s.terms_basis_date,
    t.terms_net_due_date = s.terms_net_due_date,
    t.terms_net_days = s.terms_net_days,
    t.invoice_reference = s.invoice_reference,
    t.invoice_note = s.invoice_note,
    t.account_number = s.account_number,
    t.package_count = t.package_count + s.package_count,
    t.total_charged = t.total_charged + s.total_charged,
    t.meta_update_datetime = s.meta_update_datetime,
    t.datetime_added = s.datetime_added;
