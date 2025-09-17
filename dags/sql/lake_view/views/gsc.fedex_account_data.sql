CREATE OR REPLACE VIEW lake_view.excel.fedex_account_data AS
SELECT
    customer_account,
    company_name,
    department,
    account_name,
    account_owner,
    gl_code,
    fdx01_meters,
    account_type,
    account_status,
    repurpose_date,
    comments,
    meta_create_datetime,
    meta_update_datetime
FROM lake.excel.fedex_account_data;
