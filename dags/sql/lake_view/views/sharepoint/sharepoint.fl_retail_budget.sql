CREATE OR REPLACE VIEW lake_view.sharepoint.fl_retail_budget AS
SELECT
    RIGHT('00'||store_code, 4) as store_code,
    date,
    sales,
    prelim_final,
    CONVERT_TIMEZONE('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.fl_retail_budget_sharepoint_v1.fl_retail_budget;
