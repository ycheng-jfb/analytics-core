CREATE TABLE IF NOT EXISTS reporting_prod.retail.cash_management_deposit_log(
    store_id NUMBER(38, 0) NULL,
    store_name VARCHAR(100) NULL,
    date DATE NULL,
    cash_receipt_url VARCHAR(100000) NULL,
    deposit_amount NUMBER(38, 4) NULL,
    deposit_variance NUMBER(38, 4) NULL,
    deposit_user VARCHAR(52) NULL,
    deposit_status VARCHAR(50) NULL,
    meta_create_datetime TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP::TIMESTAMP_LTZ,
    meta_update_datetime TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP::TIMESTAMP_LTZ
);

SET beg_of_month = (
    SELECT
        NVL(from_date, '2019-03-10') AS from_date
    FROM (
        SELECT MAX(date) AS max_month_date,
            ADD_MONTHS(max_month_date, -5) AS from_date
        FROM reporting_prod.retail.cash_management_deposit_log
    ) a
);

CREATE OR REPLACE TEMPORARY TABLE _opening_cash_transaction_ids AS
SELECT cash_drawer_id,
    ct.cash_transaction_id AS opening_cash_transaction_id,
    ct.datetime_added AS opening_cash_transaction_datetime,
    LEAD(ct.cash_transaction_id) OVER(PARTITION BY ct.cash_drawer_id ORDER BY ct.datetime_added) AS next_opening_cash_transaction_id,
    LEAD(ct.datetime_added) OVER(PARTITION BY ct.cash_drawer_id ORDER BY ct.datetime_added) AS next_opening_cash_transaction_datetime
FROM lake_fl_view.ultra_merchant.cash_transaction ct
WHERE ct.object = 'open drawer'
    AND ct.statuscode = 5411
    AND ct.datetime_added >= $beg_of_month;

CREATE OR REPLACE TEMPORARY TABLE _open_close_cash_transaction_ids AS
SELECT octi.cash_drawer_id,
    octi.opening_cash_transaction_id,
    octi.opening_cash_transaction_datetime,
    octi.next_opening_cash_transaction_id,
    octi.next_opening_cash_transaction_datetime,
    MAX(ct.cash_transaction_id) AS closing_cash_transaction_id,
    MAX(ct.datetime_added) AS closing_cash_transaction_datetime
FROM _opening_cash_transaction_ids octi
LEFT JOIN lake_fl_view.ultra_merchant.cash_transaction ct ON ct.cash_drawer_id = octi.cash_drawer_id
    AND ct.cash_transaction_id >= octi.opening_cash_transaction_id
    AND ct.cash_transaction_id < octi.next_opening_cash_transaction_id
WHERE ct.object = 'close drawer'
GROUP BY octi.cash_drawer_id,
    octi.opening_cash_transaction_id,
    octi.opening_cash_transaction_datetime,
    octi.next_opening_cash_transaction_id,
    octi.next_opening_cash_transaction_datetime;

CREATE OR REPLACE TEMPORARY TABLE _delete_from_table AS
SELECT cd.store_id,
    MIN(ct.datetime_added)::DATE AS delete_from_date
FROM _open_close_cash_transaction_ids occti
JOIN lake_fl_view.ultra_merchant.cash_transaction ct ON opening_cash_transaction_id = ct.cash_transaction_id
JOIN lake_fl_view.ultra_merchant.cash_drawer cd ON cd.cash_drawer_id = ct.cash_drawer_id
GROUP BY cd.store_id;

CREATE OR REPLACE TEMPORARY TABLE _variance AS
SELECT ct.cash_drawer_id,
    sccti.opening_cash_transaction_datetime AS open_datetime_added,
    sccti.closing_cash_transaction_datetime AS close_datetime_added,
    SUM(ct.amount) AS eod_variance
FROM lake_fl_view.ultra_merchant.cash_transaction ct
JOIN _open_close_cash_transaction_ids sccti ON ct.cash_drawer_id = sccti.cash_drawer_id
    AND ct.object = 'variance'
    AND ct.cash_transaction_id BETWEEN sccti.opening_cash_transaction_id AND sccti.closing_cash_transaction_id
GROUP BY ct.cash_drawer_id,
    sccti.opening_cash_transaction_datetime,
    sccti.closing_cash_transaction_datetime;

DELETE FROM reporting_prod.retail.cash_management_deposit_log cmdg
USING _delete_from_table dt
WHERE cmdg.store_id = dt.store_id
    AND cmdg.date >= dt.delete_from_date;

INSERT INTO reporting_prod.retail.cash_management_deposit_log (
    store_id,
    store_name,
    date,
    cash_receipt_url,
    deposit_amount,
    deposit_variance,
    deposit_user,
    deposit_status
)
SELECT cd.store_id,
    ds.store_full_name AS store_name,
    scc.opening_cash_transaction_datetime::DATE AS date,
    cash_receipt_url,
    MAX(dep.amount) AS deposit_amount,
    SUM(CASE WHEN TRIM(cd.label,' ')= 'Safe' THEN ct.balance - COALESCE(sv.eod_variance, 0) END) - MAX(dep.amount) AS deposit_variance,
    MAX(CONCAT(COALESCE(dep_admin.firstname,''),' ',COALESCE(dep_admin.lastname,''))) AS deposit_user,
    MAX(dep_status.label) AS deposit_status
FROM _open_close_cash_transaction_ids scc
JOIN lake_fl_view.ultra_merchant.cash_drawer cd ON cd.cash_drawer_id = scc.cash_drawer_id
JOIN  edw_prod.data_model_fl.dim_store ds ON ds.store_id = cd.store_id
LEFT JOIN lake_fl_view.ultra_merchant.cash_transaction ct ON ct.cash_transaction_id = scc.closing_cash_transaction_id
LEFT JOIN _variance sv ON sv.cash_drawer_id = scc.cash_drawer_id
    AND sv.open_datetime_added = scc.opening_cash_transaction_datetime
    AND sv.close_datetime_added = scc.closing_cash_transaction_datetime
LEFT JOIN lake_fl_view.ultra_merchant.cash_deposit dep ON dep.store_id = cd.store_id
    AND scc.opening_cash_transaction_datetime::DATE = dep.date_deposit
LEFT JOIN lake_consolidated_view.ultra_merchant.cash_deposit_receipts cdr ON dep.cash_deposit_id = cdr.cash_deposit_id
LEFT JOIN lake_fl_view.ultra_merchant.statuscode dep_status ON dep_status.statuscode = dep.statuscode
LEFT JOIN lake_fl_view.ultra_merchant.administrator dep_admin ON dep_admin.administrator_id = dep.deposited_administrator_id
where store_brand_abbr = 'FL'
GROUP BY cd.store_id,
    ds.store_full_name,
    scc.opening_cash_transaction_datetime::DATE,
    cash_receipt_url;
