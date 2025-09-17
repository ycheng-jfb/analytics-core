CREATE OR REPLACE VIEW lake_view.sharepoint.daily_performance AS
SELECT
    daily_cash_store_type,
    budget_or_forecast,
    orders,
    promo,
    product_gross_profit,
    activating_or_non_activating,
    product_net_revenue,
    date::DATE date,
    units,
    aov,
    aur,
    cash_gross_revenue,
    auc,
    ly_promo,
    _fivetran_synced meta_create_datetime,
    _fivetran_synced meta_update_datetime
FROM lake_fivetran.central_inbound_sharepoint_v1.daily_performance_dash_ingestion_flw_na
QUALIFY ROW_NUMBER() OVER (PARTITION BY daily_cash_store_type, date::DATE, activating_or_non_activating, budget_or_forecast ORDER BY NULL) =1

union all

SELECT
    daily_cash_store_type,
    budget_or_forecast,
    orders,
    promo,
    product_gross_profit,
    activating_or_non_activating,
    product_net_revenue,
    date::DATE date,
    units,
    aov,
    aur,
    cash_gross_revenue,
    auc,
    ly_promo,
    _fivetran_synced meta_create_datetime,
    _fivetran_synced meta_update_datetime
FROM lake_fivetran.central_inbound_sharepoint_v1.daily_performance_dash_ingestion_flm_na
QUALIFY ROW_NUMBER() OVER (PARTITION BY daily_cash_store_type, date::DATE, activating_or_non_activating, budget_or_forecast ORDER BY NULL) =1

union all

SELECT
    daily_cash_store_type,
    budget_or_forecast,
    orders,
    promo,
    product_gross_profit,
    activating_or_non_activating,
    product_net_revenue,
    date::DATE date,
    units,
    aov,
    aur,
    cash_gross_revenue,
    auc,
    ly_promo,
    _fivetran_synced meta_create_datetime,
    _fivetran_synced meta_update_datetime
FROM lake_fivetran.central_inbound_sharepoint_v1.daily_performance_dash_ingestion_yt_na
QUALIFY ROW_NUMBER() OVER (PARTITION BY daily_cash_store_type, date::DATE, activating_or_non_activating, budget_or_forecast ORDER BY NULL) =1

union all

SELECT
    daily_cash_store_type,
    budget_or_forecast,
    try_to_decimal(orders, 10, 2) orders,
    promo,
    try_to_decimal(product_gross_profit, 10, 2) product_gross_profit,
    activating_or_non_activating,
    try_to_decimal(product_net_revenue, 10, 2) product_net_revenue,
    date::DATE date,
    try_to_decimal(units, 10, 2) units,
    aov,
    aur,
    cash_gross_revenue,
    auc,
    ly_promo,
    _fivetran_synced meta_create_datetime,
    _fivetran_synced meta_update_datetime
FROM lake_fivetran.central_inbound_sharepoint_v1.daily_performance_dash_ingestion_sc_na
QUALIFY ROW_NUMBER() OVER (PARTITION BY daily_cash_store_type, date::DATE, activating_or_non_activating, budget_or_forecast ORDER BY NULL) =1

union all

SELECT
    daily_cash_store_type,
    budget_or_forecast,
    orders,
    promo,
    product_gross_profit,
    activating_or_non_activating,
    product_net_revenue,
    date::DATE date,
    units,
    aov,
    aur,
    cash_gross_revenue,
    auc,
    ly_promo,
    _fivetran_synced meta_create_datetime,
    _fivetran_synced meta_update_datetime
FROM lake_fivetran.central_inbound_sharepoint_v1.daily_performance_dash_ingestion_flw_eu
QUALIFY ROW_NUMBER() OVER (PARTITION BY daily_cash_store_type, date::DATE, activating_or_non_activating, budget_or_forecast ORDER BY NULL) =1

union all

SELECT
    daily_cash_store_type,
    budget_or_forecast,
    orders,
    promo,
    product_gross_profit,
    activating_or_non_activating,
    product_net_revenue,
    date::DATE date,
    units,
    aov,
    aur,
    cash_gross_revenue,
    auc,
    ly_promo,
    _fivetran_synced meta_create_datetime,
    _fivetran_synced meta_update_datetime
FROM lake_fivetran.central_inbound_sharepoint_v1.daily_performance_dash_ingestion_flm_eu
QUALIFY ROW_NUMBER() OVER (PARTITION BY daily_cash_store_type, date::DATE, activating_or_non_activating, budget_or_forecast ORDER BY NULL) =1

union all

SELECT
    daily_cash_store_type,
    budget_or_forecast,
    orders,
    promo,
    product_gross_profit,
    activating_or_non_activating,
    product_net_revenue,
    date::DATE date,
    units,
    aov,
    aur,
    cash_gross_revenue,
    auc,
    ly_promo,
    _fivetran_synced meta_create_datetime,
    _fivetran_synced meta_update_datetime
FROM lake_fivetran.central_inbound_sharepoint_v1.daily_performance_dash_ingestion_retail_na
QUALIFY ROW_NUMBER() OVER (PARTITION BY daily_cash_store_type, date::DATE, activating_or_non_activating, budget_or_forecast ORDER BY NULL) =1
;
