from pathlib import Path

import pendulum
from airflow.models import DAG

from include.airflow.operators.snowflake import SnowflakeProcedureOperator, SnowflakeSqlOperator
from include.config import owners
from airflow.operators.empty import EmptyOperator

default_args = {
    'depends_on_past': False,
    'start_date': pendulum.datetime(2019, 1, 1, 7, tz='America/Los_Angeles'),
    'retries': 1,
    'owner': owners.jfb_analytics,
}

dag = DAG(
    dag_id="analytics_reporting_gfb_daily_primary",
    default_args=default_args,
    schedule="0 0 * * *",
    catchup=False,
)

with (dag):
    order_transactions_by_reason = SnowflakeProcedureOperator(
        procedure='reporting.order_transactions_by_reason.sql',
        database='edw_prod',
        autocommit=False,
    )

    credit_activity_waterfalls_original_cohort = SnowflakeProcedureOperator(
        procedure='shared.credit_activity_waterfalls_original_cohort.sql',
        database='reporting_prod',
        autocommit=False,
    )

    # reporting.failed_billings_final_output.sql is depend on this
    customer_failed_billings_monthly = SnowflakeProcedureOperator(
        procedure='analytics_base.customer_failed_billings_monthly.sql',
        database='edw_prod',
        autocommit=False,
    )

    billing_cycle_rates_1st_through_5th = SnowflakeProcedureOperator(
        procedure='reporting.billing_cycle_rates_1st_through_5th.sql',
        database='edw_prod',
        autocommit=False,
    )

    payment_dataset_final_output = SnowflakeProcedureOperator(
        procedure='reporting.payment_dataset_final_output.sql',
        database='edw_prod',
        autocommit=False,
    )

    # attribution.cac_by_lead_channel_daily.sql is depend on attribution.acquisition_unattributed_utm_cac.sql
    # attribution.acquisition_unattributed_utm_cac.sql is depend on this
    acquisition_media_spend_daily_agg = SnowflakeProcedureOperator(
        procedure='analytics_base.acquisition_media_spend_daily_agg.sql',
        database='edw_prod',
        autocommit=False,
    )

    channel_display_name = SnowflakeProcedureOperator(
        procedure='lkp.channel_display_name.sql',
        database='reporting_media_base_prod',
        autocommit=False,
    )

    acquisition_unattributed_utm_cac = SnowflakeProcedureOperator(
        procedure='attribution.acquisition_unattributed_utm_cac.sql',
        database='reporting_media_prod',
        autocommit=False,
    )

    cac_by_lead_channel_daily = SnowflakeProcedureOperator(
        procedure='attribution.cac_by_lead_channel_daily.sql',
        database='reporting_media_prod',
        autocommit=False,
    )

    finance_sales_ops_stg = SnowflakeProcedureOperator(
        procedure='analytics_base.finance_sales_ops_stg.sql',
        database='edw_prod',
        autocommit=False,
    )

    daily_cash_base_calc = SnowflakeProcedureOperator(
        procedure='reporting.daily_cash_base_calc.sql',
        database='edw_prod',
        autocommit=False,
    )

    daily_cash_final_output = SnowflakeProcedureOperator(
        procedure='reporting.daily_cash_final_output.sql',
        database='edw_prod',
        autocommit=False,
    )

    finance_kpi_base = SnowflakeProcedureOperator(
        procedure='reporting.finance_kpi_base.sql',
        database='edw_prod',
        autocommit=False,
    )

    finance_segment_mapping = SnowflakeProcedureOperator(
        procedure='reference.finance_segment_mapping.sql',
        database='edw_prod',
        autocommit=False,
    )

    finance_kpi_final_output = SnowflakeProcedureOperator(
        procedure='reporting.finance_kpi_final_output.sql',
        database='edw_prod',
        autocommit=False,
    )

    fact_media_cost_scrubs_split = SnowflakeProcedureOperator(
        procedure='dbo.fact_media_cost_scrubs_split.sql',
        database='reporting_media_prod',
        autocommit=False,
    )

    acquisition_unattributed_total_cac = SnowflakeProcedureOperator(
        procedure='attribution.acquisition_unattributed_total_cac.sql',
        database='reporting_media_prod',
        autocommit=False,
    )

    finance_monthly_budget_forecast_actual = SnowflakeProcedureOperator(
        procedure='reference.finance_monthly_budget_forecast_actual.sql',
        database='edw_prod',
        autocommit=False,
    )

    finance_bu_mapping = SnowflakeProcedureOperator(
        procedure='reference.finance_bu_mapping.sql',
        database='edw_prod',
        autocommit=False,
    )

    finance_budget_forecast = SnowflakeProcedureOperator(
        procedure='reference.finance_budget_forecast.sql',
        database='edw_prod',
        autocommit=False,
    )

    acquisition_budget_targets_cac = SnowflakeProcedureOperator(
        procedure='attribution.acquisition_budget_targets_cac.sql',
        database='reporting_media_prod',
        autocommit=False,
    )

    total_cac_output = SnowflakeProcedureOperator(
        procedure='attribution.total_cac_output.sql',
        database='reporting_media_prod',
        autocommit=False,
    )

    gfb059_finance_daily_sales = SnowflakeProcedureOperator(
        procedure='gfb.gfb059_finance_daily_sales.sql',
        database='reporting_prod',
        autocommit=False,
    )

    gfb055_02_outlook_gsheet_hist = SnowflakeProcedureOperator(
        procedure='gfb.gfb055_02_outlook_gsheet_hist.sql',
        database='reporting_prod',
        autocommit=False,
    )

    gfb055_01_outlook_metrics = SnowflakeProcedureOperator(
        procedure='gfb.gfb055_01_outlook_metrics.sql',
        database='reporting_prod',
        autocommit=False,
    )

    gfb055_month_end_kpi = SnowflakeProcedureOperator(
        procedure='gfb.gfb055_month_end_kpi.sql',
        database='reporting_prod',
        autocommit=False,
    )

    # media_dim_store_flm_sessions = SnowflakeProcedureOperator(
    #     procedure='dbo.media_dim_store_flm_sessions.sql',
    #     database='reporting_media_prod',
    #     autocommit=False,
    # )

    # social_channel_comparison_tiktok_reddit_click_hdyh_conversions = SnowflakeProcedureOperator(
    #     procedure='tiktok.social_channel_comparison_tiktok_reddit_click_hdyh_conversions.sql',
    #     database='reporting_media_prod',
    #     autocommit=False,
    # )

    total_cac_rolling_lead_cohorts = SnowflakeProcedureOperator(
        procedure='attribution.total_cac_rolling_lead_cohorts.sql',
        database='reporting_media_prod',
        autocommit=False,
    )

    ultra_merchant_history_product = SnowflakeProcedureOperator(
        procedure='ultra_merchant_history.product.sql',
        database='lake_consolidated',
        autocommit=False,
    )

    # advertising_spend_metrics_daily_by_ad = SnowflakeProcedureOperator(
    #     procedure='tiktok.advertising_spend_metrics_daily_by_ad.sql',
    #     database='reporting_media_base_prod',
    #     autocommit=False,
    # )

    # tiktok_metadata = SnowflakeProcedureOperator(
    #     procedure='tiktok.metadata.sql',
    #     database='reporting_media_base_prod',
    #     autocommit=False,
    # )

    # conversion_metrics_daily_by_ad = SnowflakeProcedureOperator(
    #     procedure='tiktok.conversion_metrics_daily_by_ad.sql',
    #     database='reporting_media_base_prod',
    #     autocommit=False,
    # )

    # media_channel_ui_targets_actuals = SnowflakeProcedureOperator(
    #     procedure='dbo.media_channel_ui_targets_actuals.sql',
    #     database='reporting_media_base_prod',
    #     autocommit=False,
    # )

    # tiktok_optimization_dataset = SnowflakeProcedureOperator(
    #     procedure='tiktok.tiktok_optimization_dataset.sql',
    #     database='reporting_media_prod',
    #     autocommit=False,
    # )

    customer_lifetime_value_monthly = SnowflakeProcedureOperator(
        procedure='analytics_base.customer_lifetime_value_monthly.sql',
        database='edw_prod',
        autocommit=False,
    )

    join_branch_0 = EmptyOperator(task_id="join_branch_0", trigger_rule="none_failed")

    finance_sales_ops_stg >> daily_cash_base_calc >> daily_cash_final_output

    finance_segment_mapping >> [
        finance_sales_ops_stg,
        acquisition_media_spend_daily_agg,
    ] >> finance_kpi_base >> finance_kpi_final_output

    acquisition_media_spend_daily_agg >> channel_display_name >> acquisition_unattributed_utm_cac >> cac_by_lead_channel_daily

    [
        acquisition_media_spend_daily_agg,
        finance_monthly_budget_forecast_actual,
        finance_bu_mapping,
    ] >> join_branch_0

    join_branch_0 >> [
        fact_media_cost_scrubs_split,
        finance_budget_forecast,
    ] >> acquisition_unattributed_total_cac >> acquisition_budget_targets_cac >> total_cac_output >> gfb059_finance_daily_sales >> gfb055_02_outlook_gsheet_hist >> gfb055_01_outlook_metrics >> gfb055_month_end_kpi

    # media_dim_store_flm_sessions >> social_channel_comparison_tiktok_reddit_click_hdyh_conversions

    acquisition_unattributed_total_cac >> total_cac_rolling_lead_cohorts

    # [
    #     advertising_spend_metrics_daily_by_ad,
    #     tiktok_metadata,
    #     media_channel_ui_targets_actuals,
    # ] >> conversion_metrics_daily_by_ad >> tiktok_optimization_dataset

    finance_sales_ops_stg >> customer_lifetime_value_monthly
