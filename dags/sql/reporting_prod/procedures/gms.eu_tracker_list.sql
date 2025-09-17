SET current_datetime = CURRENT_TIMESTAMP();

-- Getting only DE,UK,ES,FR store business units
CREATE OR REPLACE TEMPORARY TABLE _table_store
AS
SELECT
    ds.store_id,
    ds.store_name
FROM edw_prod.data_model.dim_store ds
WHERE ds.store_region = 'EU'
AND ds.store_country IN ('DE','UK','FR','ES')
AND ds.is_core_store = 1;


CREATE OR REPLACE TEMPORARY TABLE _table_test_customers
AS
SELECT DISTINCT dc.customer_id as customer_id
FROM edw_prod.data_model.dim_customer dc
WHERE dc.email LIKE '%@justfab.com'
    OR dc.email LIKE '%@savagex.com'
    OR dc.email LIKE '%@fabletics.com';


CREATE OR REPLACE TEMPORARY TABLE _table_sessions
AS
SELECT DISTINCT
    dc.customer_id as customer_id
FROM reporting_base_prod.shared.session se
JOIN edw_prod.data_model.dim_customer dc ON dc.customer_id = se.customer_id
    AND dc.is_test_customer = 0
JOIN _table_store s ON s.store_id = dc.store_id;
select count(1) from _table_test_customers;


--CREATE CLUSTERED INDEX ix_customer_id ON #test_customers(customer_id)


DELETE FROM _table_sessions
WHERE customer_id in
    (SELECT customer_id
    from _table_test_customers);


CREATE OR REPLACE TEMPORARY TABLE _table_vips
AS
SELECT DISTINCT
    dc.customer_id as customer_id,
    mem.membership_id,
    ds.store_name
FROM _table_sessions se
JOIN edw_prod.data_model.fact_membership_event fme ON se.customer_id = fme.CUSTOMER_ID
    AND fme.membership_event_type = 'Activation'
    AND fme.is_current = 1
JOIN edw_prod.data_model.dim_customer dc ON dc.customer_id = se.customer_id   ---logged in last 3 months
JOIN _table_store ds on ds.store_id = dc.store_id
JOIN lake_consolidated_view.ultra_merchant.membership mem ON mem.customer_id = dc.customer_id;

CREATE OR REPLACE TEMPORARY TABLE _table_leads
AS
SELECT DISTINCT
    dc.customer_id as customer_id,
    mem.membership_id,
    ds.store_name
FROM _table_sessions se
JOIN edw_prod.data_model.fact_membership_event fme ON se.customer_id = fme.CUSTOMER_ID
    AND fme.membership_event_type = 'Registration'
    AND fme.is_current = 1
JOIN edw_prod.data_model.dim_customer dc ON dc.customer_id = se.customer_id   ---logged in last 3 months
JOIN _table_store ds on ds.store_id = dc.store_id
JOIN lake_consolidated_view.ultra_merchant.membership mem ON mem.customer_id = dc.customer_id;


INSERT INTO REPORTING_PROD.gms.eu_tracker_list (
    store_name,
    customer_id,
    membership_id,
    segment,
    current_datetime
)
SELECT
    store_name,
    customer_id,
    membership_id,
    'Leads',
    $current_datetime
FROM _table_leads
UNION ALL
SELECT
    store_name,
    customer_id,
    membership_id,
    'Vips',
    $current_datetime
FROM _table_vips;
