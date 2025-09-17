CREATE TABLE IF NOT EXISTS reporting_prod.retail.cash_management(
	store_id NUMBER(38, 0) NOT NULL,
	store_name VARCHAR(50) NOT NULL,
	date DATE NULL,
	r1_time_counted NUMBER(38, 0) NULL,
	r1_user VARCHAR(51) NULL,
	r1_cash_expected NUMBER(38, 4) NULL,
	r1_cash_counted NUMBER(38, 4) NULL,
	r1_variance NUMBER(38, 4) NULL,
	r2_time_counted NUMBER(38, 0) NULL,
	r2_user VARCHAR(51) NULL,
	r2_cash_expected NUMBER(38, 4) NULL,
	r2_cash_counted NUMBER(38, 4) NULL,
	r2_variance NUMBER(38, 4) NULL,
	safe_time_counted NUMBER(38, 0) NULL,
	safe_user VARCHAR(51) NULL,
	safe_cash_expected NUMBER(38, 4) NULL,
	safe_cash_counted NUMBER(38, 4) NULL,
	safe_variance NUMBER(38, 4) NULL,
	eod_time TIMESTAMP NULL,
	eod_user VARCHAR(51) NULL,
	eod_expected NUMBER(38, 4) NULL,
	eod_actual NUMBER(38, 4) NULL,
	eod_variance NUMBER(38, 4) NULL,
	deposit_amount NUMBER(38, 4) NULL,
	deposit_variance NUMBER(38, 4) NULL,
	deposit_user VARCHAR(51) NULL,
	deposit_status VARCHAR(50) NULL,
	meta_create_datetime TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP::TIMESTAMP_LTZ,
	meta_update_datetime TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP::TIMESTAMP_LTZ
);

SET beg_of_month = (
    SELECT
        NVL(from_date, '2019-03-19') AS from_date
    FROM (
        SELECT MAX(date) AS max_month_date,
            ADD_MONTHS(max_month_date, -5) AS from_date
        FROM reporting_prod.retail.cash_management
    ) a
);

SET end_of_month = CURRENT_DATE();

CREATE OR REPLACE TEMPORARY TABLE _cash_drawer_ids AS
SELECT cd.store_id,
	cd.cash_drawer_id AS cash_drawer_id,
	cd.label
FROM lake_fl_view.ultra_merchant.cash_drawer cd
WHERE type IN ('safe', 'drawer');

CREATE OR REPLACE TEMPORARY TABLE _opening_cash_transaction_ids AS
SELECT si.store_id,
    --ct.statuscode,
	si.cash_drawer_id,
	MIN(ct.cash_transaction_id) AS opening_cash_transaction_id,
	ct.datetime_added AS opening_datetime_added
FROM lake_fl_view.ultra_merchant.cash_transaction ct
JOIN _cash_drawer_ids si ON ct.cash_drawer_id = si.cash_drawer_id
	AND ct.object = 'open drawer'
	AND ct.statuscode = 5411
WHERE ct.datetime_added >= $beg_of_month
	AND ct.datetime_added < $end_of_month
GROUP BY ct.datetime_added,
	si.store_id,
	si.cash_drawer_id;

CREATE OR REPLACE TEMPORARY TABLE _closing_cash_transaction_ids AS
SELECT socti.store_id,
	socti.cash_drawer_id,
	socti.opening_datetime_added,
	socti.opening_cash_transaction_id,
	MIN(cash_transaction_id) AS closing_cash_transaction_id,
	MIN(ct.datetime_added) AS closing_datetime_added
FROM lake_fl_view.ultra_merchant.cash_transaction ct
JOIN _opening_cash_transaction_ids socti ON ct.cash_drawer_id = socti.cash_drawer_id
	AND ct.object = 'close drawer'
	AND ct.statuscode = 5411
	AND ct.cash_transaction_id > socti.opening_cash_transaction_id
GROUP BY socti.store_id,
	socti.opening_cash_transaction_id,
	socti.cash_drawer_id,
	socti.opening_datetime_added;

CREATE OR REPLACE TEMPORARY TABLE _duplicate_opens AS
SELECT *,
	ROW_NUMBER() OVER(PARTITION BY cash_drawer_id, closing_cash_transaction_id ORDER BY opening_cash_transaction_id) AS rn
FROM _closing_cash_transaction_ids;

DELETE
FROM _closing_cash_transaction_ids c
USING _duplicate_opens t
WHERE t.closing_cash_transaction_id = c.closing_cash_transaction_id
	AND t.opening_cash_transaction_id = c.opening_cash_transaction_id
    AND rn > 1;

CREATE OR REPLACE TEMPORARY TABLE _variance AS
SELECT sccti.store_id,
	ct.cash_drawer_id,
	sccti.opening_datetime_added::DATE AS transaction_date,
	sccti.opening_datetime_added AS open_datetime_added,
	sccti.closing_datetime_added AS close_datetime_added,
	SUM(ct.amount) AS eod_variance,
	SUM(ct.balance) AS eod_actual,
	SUM(ct.balance) - SUM(ct.amount) AS eod_expected
FROM lake_fl_view.ultra_merchant.cash_transaction ct
JOIN _closing_cash_transaction_ids sccti ON ct.cash_drawer_id = sccti.cash_drawer_id
	AND ct.object = 'variance'
	AND ct.cash_transaction_id BETWEEN sccti.opening_cash_transaction_id AND sccti.closing_cash_transaction_id
GROUP BY sccti.store_id,
	ct.cash_drawer_id,
	sccti.opening_datetime_added,
	sccti.closing_datetime_added;

CREATE OR REPLACE TEMPORARY TABLE _time_counted AS
SELECT sccti.store_id,
	ct.cash_drawer_id,
	sccti.opening_datetime_added,
	sccti.closing_datetime_added,
	COUNT(1) AS time_counted,
	f.name AS eod_user,
	f.balance AS eod_actual
FROM lake_fl_view.ultra_merchant.cash_transaction ct
JOIN _closing_cash_transaction_ids sccti ON ct.cash_drawer_id = sccti.cash_drawer_id
	AND ct.cash_transaction_id BETWEEN sccti.opening_cash_transaction_id AND sccti.closing_cash_transaction_id
LEFT JOIN (
	SELECT  CONCAT(COALESCE(a.firstname,''),' ',COALESCE(a.lastname,'')) AS name,
		ct2.balance,ct2.cash_transaction_id
	FROM lake_fl_view.ultra_merchant.cash_transaction ct2
	JOIN lake_fl_view.ultra_merchant.administrator a ON a.administrator_id = ct2.administrator_id
	) f ON f.cash_transaction_id = sccti.closing_cash_transaction_id
GROUP BY sccti.store_id,
	ct.cash_drawer_id,
	sccti.opening_datetime_added,
	sccti.closing_datetime_added,
	f.name,
	f.balance;

DELETE FROM reporting_prod.retail.cash_management
WHERE date >= $beg_of_month;

INSERT INTO reporting_prod.retail.cash_management (
	store_id,
	store_name,
	date,
	r1_time_counted,
	r1_user,
	r1_cash_expected,
	r1_cash_counted,
	r1_variance,
	r2_time_counted,
	r2_user,
	r2_cash_expected,
	r2_cash_counted,
	r2_variance,
	safe_time_counted,
	safe_user,
	safe_cash_expected,
	safe_cash_counted,
	safe_variance,
	eod_time,
	eod_user,
	eod_expected,
	eod_actual,
	eod_variance,
	deposit_amount,
	deposit_variance,
	deposit_user,
	deposit_status
)
SELECT scc.store_id,
	ds.store_full_name as store_name,
	scc.opening_datetime_added::DATE AS DATE,
	SUM(CASE WHEN TRIM(cd.label,' ') = 'Left Register' THEN scc.time_counted END) AS r1_time_counted,
	MAX(CASE WHEN TRIM(cd.label,' ') = 'Left Register' THEN scc.eod_user END) AS r1_user,
	SUM(CASE WHEN TRIM(cd.label,' ') = 'Left Register' THEN scc.eod_actual - COALESCE(sv.eod_variance, 0) END) AS r1_cash_expected,
	SUM(CASE WHEN TRIM(cd.label,' ') = 'Left Register' THEN scc.eod_actual END) AS r1_cash_counted,
	SUM(CASE WHEN TRIM(cd.label,' ') = 'Left Register' THEN COALESCE(sv.eod_variance, 0) END) AS r1_variance,
	SUM(CASE WHEN TRIM(cd.label,' ') = 'Right Register' THEN scc.time_counted END) AS r2_time_counted,
	MAX(CASE WHEN TRIM(cd.label,' ') = 'Right Register' THEN scc.eod_user END) AS r2_user,
	SUM(CASE WHEN TRIM(cd.label,' ') = 'Right Register' THEN scc.eod_actual - COALESCE(sv.eod_variance, 0) END) AS r2_cash_expected,
	SUM(CASE WHEN TRIM(cd.label,' ') = 'Right Register' THEN scc.eod_actual END) AS r2_cash_counted,
	SUM(CASE WHEN TRIM(cd.label,' ') = 'Right Register' THEN COALESCE(sv.eod_variance, 0) END) AS r2_variance,
	SUM(CASE WHEN TRIM(cd.label,' ') = 'Safe' THEN scc.time_counted END) AS safe_time_counted,
	MAX(CASE WHEN TRIM(cd.label,' ') = 'Safe' THEN scc.eod_user END) AS safe_user,
	SUM(CASE WHEN TRIM(cd.label,' ') = 'Safe' THEN scc.eod_actual - COALESCE(sv.eod_variance, 0) END) AS safe_cash_expected,
	SUM(CASE WHEN TRIM(cd.label,' ') = 'Safe' THEN scc.eod_actual END) AS safe_cash_counted,
	SUM(CASE WHEN TRIM(cd.label,' ') = 'Safe' THEN COALESCE(sv.eod_variance, 0) END) AS safe_variance,
	MAX(eod_user.closing_datetime_added) AS eod_time,
	MAX(eod_user.eod_user) AS eod_user,
	SUM(CASE WHEN TRIM(cd.label,' ') IN ('Left Register', 'Right Register') THEN scc.eod_actual - COALESCE(sv.eod_variance, 0) END) AS eod_expected,
	SUM(CASE WHEN TRIM(cd.label,' ') IN ('Left Register', 'Right Register') THEN scc.eod_actual END) AS eod_actual,
	SUM(CASE WHEN TRIM(cd.label,' ') IN ('Left Register', 'Right Register') THEN COALESCE(sv.eod_variance, 0) END) AS eod_variance,
	MAX(dep.amount) AS deposit_amount,
	SUM(CASE WHEN TRIM(cd.label,' ') = 'Safe' THEN scc.eod_actual - COALESCE(sv.eod_variance, 0) END)-MAX(dep.amount) AS deposit_variance,
	MAX(CONCAT(COALESCE(dep_admin.firstname,''),' ',COALESCE(dep_admin.lastname,''))) AS deposit_user,
	MAX(dep_status.label) AS deposit_status
FROM _time_counted scc
JOIN edw_prod.data_model_fl.dim_store ds ON ds.store_id = scc.store_id
JOIN lake_fl_view.ultra_merchant.cash_drawer cd ON cd.cash_drawer_id = scc.cash_drawer_id
LEFT JOIN _variance sv ON sv.store_id = scc.store_id
	AND sv.cash_drawer_id = scc.cash_drawer_id
	AND sv.open_datetime_added = scc.opening_datetime_added
	AND sv.close_datetime_added = scc.closing_datetime_added
LEFT JOIN (
	SELECT ct.store_id,
		ct.cash_drawer_id,
		ct.opening_datetime_added,
		MAX(closing_datetime_added) AS eod_time
	FROM _time_counted ct
	JOIN lake_fl_view.ultra_merchant.cash_drawer cd ON cd.cash_drawer_id = ct.cash_drawer_id
	WHERE TRIM(cd.label,' ') IN ('Left Register', 'Right Register')
	GROUP BY ct.store_id,
		ct.cash_drawer_id,
		ct.opening_datetime_added
) eod ON eod.cash_drawer_id = scc.cash_drawer_id
	AND eod.store_id = scc.store_id
	AND eod.eod_time = scc.closing_datetime_added
LEFT JOIN _time_counted eod_user ON eod_user.cash_drawer_id = eod.cash_drawer_id
	AND eod_user.store_id = eod.store_id
	AND eod_user.closing_datetime_added = eod.eod_time
	AND eod_user.opening_datetime_added = eod.opening_datetime_added
LEFT JOIN lake_fl_view.ultra_merchant.cash_deposit dep ON dep.store_id = scc.store_id
	AND scc.opening_datetime_added::DATE = dep.date_deposit
LEFT JOIN lake_fl_view.ultra_merchant.statuscode dep_status ON dep_status.statuscode = dep.statuscode
LEFT JOIN lake_fl_view.ultra_merchant.administrator dep_admin ON dep_admin.administrator_id = dep.administrator_id
where scc.eod_user is not null
and store_brand_abbr = 'FL'
GROUP BY scc.store_id,
	ds.store_full_name,
	scc.opening_datetime_added::DATE;
