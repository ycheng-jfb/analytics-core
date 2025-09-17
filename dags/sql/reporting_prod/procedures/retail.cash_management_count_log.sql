CREATE TABLE IF NOT EXISTS reporting_prod.retail.cash_management_count_log(
    store_id NUMBER(38, 0) NULL,
    store_name VARCHAR(100) NULL,
    transaction_datetime TIMESTAMP NULL,
    register VARCHAR(50) NULL,
    user VARCHAR(52) NULL,
    status VARCHAR(50) NULL,
    comment VARCHAR(8000) NULL,
    register_count NUMBER(38, 4) NULL,
    register_expected NUMBER(38, 4) NULL,
    variance NUMBER(38, 4) NULL,
    meta_create_datetime TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP::TIMESTAMP_LTZ,
    meta_update_datetime TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP::TIMESTAMP_LTZ
);
SET beg_of_month = (
    SELECT
        NVL(from_date, '2019-03-10') AS from_date
    FROM (
        SELECT MAX(transaction_datetime) AS max_month_date,
            ADD_MONTHS(max_month_date, -5) AS from_date
        FROM reporting_prod.retail.cash_management_count_log
    ) a
);
CREATE OR REPLACE TEMPORARY TABLE _opening_cash_transaction_ids AS
SELECT cash_drawer_id,
    ct.cash_transaction_id AS opening_cash_transaction_id,
    LEAD(ct.cash_transaction_id) OVER(PARTITION BY ct.cash_drawer_id ORDER BY ct.datetime_added) AS next_opening_cash_transaction_id
FROM lake_fl_view.ultra_merchant.cash_transaction ct
WHERE ct.object = 'open drawer'
    AND ct.statuscode = 5411
    AND ct.datetime_added >= $beg_of_month;

CREATE OR REPLACE TEMPORARY TABLE _open_close_cash_transaction_ids AS
SELECT octi.cash_drawer_id,
    octi.opening_cash_transaction_id,
    octi.next_opening_cash_transaction_id,
    MAX(ct.cash_transaction_id) AS closing_cash_transaction_id
FROM _opening_cash_transaction_ids octi
LEFT JOIN lake_fl_view.ultra_merchant.cash_transaction ct ON ct.cash_drawer_id = octi.cash_drawer_id
    AND ct.cash_transaction_id >= octi.opening_cash_transaction_id
    AND ct.cash_transaction_id < octi.next_opening_cash_transaction_id
WHERE ct.object = 'close drawer'
GROUP BY octi.cash_drawer_id,
    octi.opening_cash_transaction_id,
    octi.next_opening_cash_transaction_id;

CREATE OR REPLACE TEMPORARY TABLE _delete_from_table AS
SELECT cd.store_id,
    MIN(ct.datetime_added) AS delete_from_datetime
FROM _open_close_cash_transaction_ids occti
JOIN lake_fl_view.ultra_merchant.cash_transaction ct ON opening_cash_transaction_id = ct.cash_transaction_id
JOIN lake_fl_view.ultra_merchant.cash_drawer cd ON cd.cash_drawer_id = ct.cash_drawer_id
GROUP BY cd.store_id;

CREATE OR REPLACE TEMPORARY TABLE _all_metrics AS
SELECT ct.cash_transaction_id,
    ct.cash_drawer_id,
    ds.store_id,
    ds.store_full_name AS store_name,
    ct.object,
    TRIM(cd.label,' ') AS register ,
    s.label AS transaction_status,
    ct.datetime_added AS transaction_datetime,
    ct.amount,
    ct.balance AS count,
    ct.memo
FROM lake_fl_view.ultra_merchant.cash_transaction ct
JOIN lake_fl_view.ultra_merchant.cash_drawer cd ON cd.cash_drawer_id = ct.cash_drawer_id
JOIN lake_fl_view.ultra_merchant.statuscode s ON s.statuscode = ct.statuscode
JOIN edw_prod.data_model_fl.dim_store ds ON ds.store_id = cd.store_id
where store_brand_abbr = 'FL';

DELETE FROM reporting_prod.retail.cash_management_count_log cmdg
USING _delete_from_table dt
WHERE cmdg.store_id = dt.store_id
    AND cmdg.transaction_datetime >= dt.delete_from_datetime;

INSERT INTO reporting_prod.retail.cash_management_count_log (
    store_id,
    store_name,
    transaction_datetime,
    register,
    user,
    status,
    comment,
    register_count,
    register_expected,
    variance
)
SELECT i.store_id,
    i.store_name,
    IFF(i.object = 'open drawer', ct_open.datetime_added, ct_close.datetime_added) AS transaction_datetime,
    TRIM(i.register,' '),
    IFF(i.object = 'open drawer', CONCAT(a_open.firstname,' ', a_open.lastname), CONCAT(a_close.firstname,' ', a_close.lastname)) AS user,
    IFF(i.object = 'open drawer', 'Open Drawer', 'Close Drawer') AS status,
    IFF(i.object = 'variance', i.memo, '') AS comment,
    SUM(CASE WHEN i.object IN ('move', 'cash deposit') THEN 0
            WHEN i.object IN ('open drawer','close drawer') THEN i.count
            ELSE i.amount
        END) AS register_count,
    SUM(CASE WHEN i.object IN ('move', 'variance', 'cash deposit') THEN 0
            WHEN i.object IN ('open drawer','close drawer') THEN i.count
            ELSE i.amount
        END) AS register_expected,
    SUM(IFF(i.object = 'variance', i.amount, 0)) AS variance
FROM _all_metrics i
JOIN _open_close_cash_transaction_ids octi ON octi.cash_drawer_id = i.cash_drawer_id
    AND i.cash_transaction_id >= octi.opening_cash_transaction_id
    AND i.cash_transaction_id < octi.next_opening_cash_transaction_id
LEFT JOIN lake_fl_view.ultra_merchant.cash_transaction ct_open ON ct_open.cash_transaction_id = octi.opening_cash_transaction_id
LEFT JOIN lake_fl_view.ultra_merchant.cash_transaction ct_close ON ct_close.cash_transaction_id = octi.closing_cash_transaction_id
LEFT JOIN lake_fl_view.ultra_merchant.administrator a_open ON a_open.administrator_id = ct_open.administrator_id
LEFT JOIN lake_fl_view.ultra_merchant.administrator a_close ON a_close.administrator_id = ct_close.administrator_id
GROUP BY IFF(i.object = 'open drawer', ct_open.datetime_added, ct_close.datetime_added),
    IFF(i.object = 'open drawer', CONCAT(a_open.firstname, ' ', a_open.lastname), CONCAT(a_close.firstname, ' ', a_close.lastname)),
    i.store_id,
    i.store_name,
    TRIM(i.register,' '),
    IFF(i.object = 'open drawer', 'Open Drawer', 'Close Drawer'),
    IFF(i.object = 'variance', i.memo, '');
