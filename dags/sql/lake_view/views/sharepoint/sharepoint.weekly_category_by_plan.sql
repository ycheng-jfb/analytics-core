CREATE OR REPLACE VIEW lake_view.sharepoint.weekly_category_by_plan AS
SELECT
    business_unit,
    category,
    class,
    date::DATE date,
    forecast_total_mtd_units,
    forecast_non_activating_mtd_gm,
    forecast_activating_mtd_gm,
    forecast_total_mtd_rev,
    forecast_activating_mtd_units,
    budget_activating_mtd_units,
    forecast_total_mtd_gm_,
    budget_non_activating_mtd_units,
    budget_total_mtd_gm_,
    forecast_non_activating_mtd_rev,
    budget_non_activating_mtd_rev,
    budget_activating_mtd_rev,
    forecast_activating_mtd_rev,
    budget_non_activating_mtd_gm,
    forecast_non_activating_mtd_units,
    budget_total_mtd_units,
    budget_total_mtd_rev,
    budget_activating_mtd_gm,
    _fivetran_synced meta_create_datetime,
    _fivetran_synced meta_update_datetime
FROM lake_fivetran.central_inbound_sharepoint_v1.weekly_category_plan_ingestion_by_day_ingestion_sheet
QUALIFY ROW_NUMBER() OVER (PARTITION BY business_unit, category,class, date::DATE ORDER BY NULL) =1
