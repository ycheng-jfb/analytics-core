import pendulum
from airflow import DAG

from include.airflow.operators.snowflake import SnowflakeProcedureOperator, SnowflakeAlertOperator
from include.airflow.operators.sharepoint import SharepointToS3Operator
from include.airflow.operators.tableau import TableauRefreshOperator
from include.airflow.operators.snowflake_load import SnowflakeIncrementalLoadOperator
from include.config import owners, stages, conn_ids, email_lists, s3_buckets
from include.utils.snowflake import Column, CopyConfigCsv

finance_forecast_column_list = [
    Column("version", "VARCHAR(255)", uniqueness=True),
    Column("version_date", "DATE", uniqueness=True),
    Column("month", "DATE", uniqueness=True),
    Column("hyperion_store_rg", "VARCHAR(255)"),
    Column("bu", "VARCHAR(255)", uniqueness=True),
    Column("membership_credits_charged_count", "NUMERIC(19,7)"),
    Column("membership_credits_redeemed_count", "NUMERIC(19,7)"),
    Column("membership_credits_cancelled_count", "NUMERIC(19,7)"),
    Column("first_msrp_revenue", "NUMERIC(19,7)"),
    Column("first_discounts", "NUMERIC(19,7)"),
    Column("repeat_msrp_revenue", "NUMERIC(19,7)"),
    Column("repeat_discounts", "NUMERIC(19,7)"),
    Column("leads", "NUMERIC(19,7)"),
    Column("gross_cash_revenue", "NUMERIC(19,7)"),
    Column("cash_refunds", "NUMERIC(19,7)"),
    Column("chargebacks", "NUMERIC(19,7)"),
    Column("net_cash_revenue_total", "NUMERIC(19,7)"),
    Column("refunds_and_chargebacks_as_percent_of_gross_cash", "NUMERIC(19,7)"),
    Column("cash_gross_margin", "NUMERIC(19,7)"),
    Column("cash_gross_margin_percent_total", "NUMERIC(19,7)"),
    Column("media_spend", "NUMERIC(19,7)"),
    Column("cash_contribution_after_media", "NUMERIC(19,7)"),
    Column("cpl", "NUMERIC(19,7)"),
    Column("vip_cpa", "NUMERIC(19,7)"),
    Column("m1_lead_to_vip", "NUMERIC(19,7)"),
    Column("m1_vips", "NUMERIC(19,7)"),
    Column("aged_lead_conversions", "NUMERIC(19,7)"),
    Column("total_new_vips", "NUMERIC(19,7)"),
    Column("cancels", "NUMERIC(19,7)"),
    Column("net_change_in_vips", "NUMERIC(19,7)"),
    Column("ending_vips", "NUMERIC(19,7)"),
    Column("vip_growth_percent", "NUMERIC(19,7)"),
    Column("activating_gaap_gross_revenue", "NUMERIC(19,7)"),
    Column("activating_gaap_net_revenue", "NUMERIC(19,7)"),
    Column("activating_order_count", "NUMERIC(19,7)"),
    Column("activating_aov_incl_shipping_rev", "NUMERIC(19,7)"),
    Column("activating_units_per_transaction", "NUMERIC(19,7)"),
    Column("activating_discount_percent", "NUMERIC(19,7)"),
    Column("activating_gross_margin_$", "NUMERIC(19,7)"),
    Column("activating_gross_margin_percent", "NUMERIC(19,7)"),
    Column("repeat_gaap_gross_revenue", "NUMERIC(19,7)"),
    Column("repeat_gaap_net_revenue", "NUMERIC(19,7)"),
    Column("repeat_order_count", "NUMERIC(19,7)"),
    Column("repeat_aov_incl_shipping_rev", "NUMERIC(19,7)"),
    Column("repeat_units_per_transaction", "NUMERIC(19,7)"),
    Column("repeat_discount_percent", "NUMERIC(19,7)"),
    Column("repeat_gaap_gross_margin_$", "NUMERIC(19,7)"),
    Column("repeat_gaap_gross_margin_percent", "NUMERIC(19,7)"),
    Column("repeat_net_unredeemed_credit_billings", "NUMERIC(19,7)"),
    Column("repeat_refund_credits", "NUMERIC(19,7)"),
    Column("repeat_other_ad_rev_div_adaptive_adj", "NUMERIC(19,7)"),
    Column("repeat_net_cash_revenue", "NUMERIC(19,7)"),
    Column("repeat_cash_gross_margin", "NUMERIC(19,7)"),
    Column("repeat_cash_gross_margin_percent", "NUMERIC(19,7)"),
    Column("membership_credits_charged", "NUMERIC(19,7)"),
    Column("membership_credits_redeemed", "NUMERIC(19,7)"),
    Column("membership_credits_refunded_plus_chargebacks", "NUMERIC(19,7)"),
    Column("net_unredeemed_credit_billings", "NUMERIC(19,7)"),
    Column("net_unredeemed_credit_billings_as_percent_of_net_cash_revenue", "NUMERIC(19,7)"),
    Column("refunded_as_credit", "NUMERIC(19,7)"),
    Column("refund_credit_redeemed", "NUMERIC(19,7)"),
    Column("net_unredeemed_refund_credit", "NUMERIC(19,7)"),
    Column("deferred_revenue", "NUMERIC(19,7)"),
    Column("bom_vips", "NUMERIC(19,7)"),
    Column("credit_billed_percent", "NUMERIC(19,7)"),
    Column("credit_redeemed_percent", "NUMERIC(19,7)"),
    Column("credit_canceled_percent", "NUMERIC(19,7)"),
    Column("total_units_shipped", "NUMERIC(19,7)"),
    Column("repeat_cash_purchasers", "NUMERIC(19,7)"),
    Column("cash_purchase_rate", "NUMERIC(19,7)"),
    Column("o_div_vip", "NUMERIC(19,7)"),
    Column("ebitda_minus_cash", "NUMERIC(19,7)"),
    Column("repeatplusbizdev", "NUMERIC(19,7)"),
    Column("activating_units", "NUMERIC(19,7)"),
    Column("repeat_units", "NUMERIC(19,7)"),
    Column("repeat_product_revenue_incl_shipping", "NUMERIC(19,7)"),
    Column("product_cost_calc", "NUMERIC(19,7)"),
    Column("inventory_writedown", "NUMERIC(19,7)"),
    Column("total_cogs_minus_cash", "NUMERIC(19,7)"),
    Column("merchant_fees", "NUMERIC(19,7)"),
    Column("gms_variable", "NUMERIC(19,7)"),
    Column("fc_variable", "NUMERIC(19,7)"),
    Column("freight_revenue", "NUMERIC(19,7)"),
    Column("freight_out_cost", "NUMERIC(19,7)"),
    Column("outbound_shipping_supplies", "NUMERIC(19,7)"),
    Column("returns_shipping_cost", "NUMERIC(19,7)"),
    Column("product_cost_markdown", "NUMERIC(19,7)"),
    Column("repeat_shipped_order_cash_collected", "NUMERIC(19,7)"),
    Column("acquisition_margin_$", "NUMERIC(19,7)"),
    Column("acquisition_margin_percent", "NUMERIC(19,7)"),
    Column("acquisition_margin_$__div__order", "NUMERIC(19,7)"),
    Column("product_cost_cust_returns_calc", "NUMERIC(19,7)"),
    Column("reship_exch_orders_shipped", "NUMERIC(19,7)"),
    Column("reship_exch_units_shipped", "NUMERIC(19,7)"),
    Column("cash_net_revenue", "NUMERIC(19,7)"),
    Column("activating_product_gross_revenue", "NUMERIC(19,7)"),
    Column("repeat_product_gross_revenue", "NUMERIC(19,7)"),
    Column("cash_gross_margin_percent", "NUMERIC(19,7)"),
    Column("repeat_units_shipped", "NUMERIC(19,7)"),
    Column("repeat_orders_shipped", "NUMERIC(19,7)"),
    Column("advertising_expenses", "NUMERIC(19,7)"),
    Column("merchant_fee", "NUMERIC(19,7)"),
    Column("selling_expenses", "NUMERIC(19,7)"),
    Column("marketing_expenses", "NUMERIC(19,7)"),
    Column("direct_general_and_administrative", "NUMERIC(19,7)"),
    Column("total_opex", "NUMERIC(19,7)"),
    Column("product_revenue", "NUMERIC(19,7)"),
    Column("product_gross_margin", "NUMERIC(19,7)"),
    Column("cash_gross_profit", "NUMERIC(19,7)"),
    Column("retail_redemption_revenue", "NUMERIC(19,7)"),
    Column("total_cogs", "NUMERIC(19,7)"),
    Column("paid_vips", "NUMERIC(19,7)"),
    Column("cross_promo_ft_vips", "NUMERIC(19,7)"),
    Column("global_finance_organization", "NUMERIC(19,7)"),
    Column("global_ops_office_admin_and_facilities", "NUMERIC(19,7)"),
    Column("tfg_administration", "NUMERIC(19,7)"),
    Column("global_people_team", "NUMERIC(19,7)"),
    Column("legal", "NUMERIC(19,7)"),
    Column("executive", "NUMERIC(19,7)"),
    Column("non_allocable_corporate", "NUMERIC(19,7)"),
    Column("eu_media_buying", "NUMERIC(19,7)"),
    Column("eu_creative_and_marketing", "NUMERIC(19,7)"),
    Column("total_backoffice_step1", "NUMERIC(19,7)"),
    Column("net_revenue_cash", "NUMERIC(19,7)"),
    Column("selling_expense", "NUMERIC(19,7)"),
    Column("global_member_services_fixed", "NUMERIC(19,7)"),
    Column("fulfillment_centers_fixed", "NUMERIC(19,7)"),
]
finance_assumptions_column_list = [
    Column("BU", "VARCHAR(255)"),
    Column("MONTH", "DATE", uniqueness=True),
    Column("BRAND", "VARCHAR(255)"),
    Column("GENDER", "VARCHAR(255)", uniqueness=True),
    Column("REGION_TYPE", "VARCHAR(255)"),
    Column("REGION_TYPE_MAPPING", "VARCHAR(255)"),
    Column("STORE_TYPE", "VARCHAR(255)"),
    Column("SHIPPING_SUPPLIES_COST_PER_ORDER", "NUMERIC(19,7)"),
    Column("SHIPPING_COST_PER_ORDER", "NUMERIC(19,7)"),
    Column("VARIABLE_WAREHOUSE_COST_PER_ORDER", "NUMERIC(19,7)"),
    Column("VARIABLE_GMS_COST_PER_ORDER", "NUMERIC(19,7)"),
    Column("VARIABLE_PAYMENT_PROCESSING_PCT_CASH_REVENUE", "NUMERIC(19,7)"),
    Column("RETURN_SHIPPING_COST_PER_ORDER", "NUMERIC(19,7)"),
    Column("PRODUCT_MARKDOWN_PERCENT", "NUMERIC(19,7)"),
]
finance_store_mapping_column_list = [
    Column("STORE_ID", "NUMBER(38,0)", uniqueness=True),
    Column("STORE_DESC", "VARCHAR(500)"),
    Column("LEGAL_ENTITY_NAME", "VARCHAR(255)"),
    Column("COMPANY_SEG", "NUMBER(38,0)"),
    Column("COMPANY_SEG_DESC", "VARCHAR(255)"),
    Column("BRAND_SEG", "VARCHAR(255)"),
    Column("BRAND_SEG_DESC", "VARCHAR(255)"),
    Column("REGION_SEG", "VARCHAR(255)"),
    Column("REGION_SEG_DESC", "VARCHAR(500)"),
]

default_args = {
    "owner": owners.analytics_engineering,
    "start_date": pendulum.datetime(2021, 3, 29, tz='America/Los_Angeles'),
}

dag = DAG(
    dag_id="edm_inbound_fpa_monthly_forecasts_and_assumptions",
    default_args=default_args,
    catchup=False,
    schedule=None,
    max_active_tasks=2,
)

finance_forecast_s3_prefix = "lake/finance_ingestions/lake.fpa.monthly_budget_forecast_actual"
finance_store_mapping_s3_prefix = "inbound/svc_oracle_ebs/lake.oracle_ebs.retail_store_mapping"


with dag:
    to_s3 = SharepointToS3Operator(
        task_id='monthly_budget_forecast_actual_to_s3',
        sharepoint_conn_id=conn_ids.Sharepoint.nordstrom,
        source_folder_name='WADS/finance_ingestion',
        file_path='monthly_budget_forecast_actual',
        site_id='ea66f082-8dc3-4715-986f-b74d2b2aa773',
        drive_id='b!gvBm6sONFUeYb7dNKyqncwIMGuc7xGpLoECxQMjza2MrgUK4IVA7SbZLrIkJk3qM',
        is_archive_file=True,
        archive_folder='WADS/finance_ingestion/monthly_budget_forecast_actual/archive',
        bucket=s3_buckets.tsos_da_int_inbound,
        s3_key_prefix=finance_forecast_s3_prefix,
        s3_conn_id=conn_ids.S3.tsos_da_int_prod,
    )

    finance_forecast_to_snowflake = SnowflakeIncrementalLoadOperator(
        task_id="fpa_monthly_budget_forecast_actual_to_snowflake",
        snowflake_conn_id=conn_ids.Snowflake.default,
        database="lake",
        schema="fpa",
        table="monthly_budget_forecast_actual",
        column_list=finance_forecast_column_list,
        files_path=f"{stages.tsos_da_int_inbound}/{finance_forecast_s3_prefix}",
        copy_config=CopyConfigCsv(header_rows=1, date_format="MM/DD/YYYY"),
    )

    finance_ingestion_duplicate_alert = SnowflakeAlertOperator(
        task_id="alert_for_fmc_variance",
        sql_or_path="""
            SELECT
                    version_date,
                    version,
                    bu,
                    month AS financial_date,
                    'lake.fpa.monthly_budget_forecast_actual' AS object_name
            FROM lake.fpa.monthly_budget_forecast_actual
            GROUP BY version_date, version, bu, month
            HAVING COUNT(*) > 1
            UNION ALL
            SELECT
                    version_date,
                    version,
                    bu,
                    financial_date,
                    'edw_prod.reference.finance_monthly_budget_forecast_actual'
            FROM edw_prod.reference.finance_monthly_budget_forecast_actual
            GROUP BY version_date, version, bu, financial_date
            HAVING COUNT(*) > 1
            UNION ALL
            SELECT
                    version_date,
                    version,
                    bu,
                    financial_date,
                    'edw_prod.reference.finance_budget_forecast'
            FROM edw_prod.reference.finance_budget_forecast
            WHERE bu NOT IN ('16091 - Canada', '10001 - United States','41001 - United Kingdom',
                 '40001 - Germany') -- exclude regional BUs shared by multiple brands, we do not use them
            GROUP BY version_date, version, bu, financial_date, currency_type
            HAVING COUNT(*) > 1
                """,
        subject="Alert: Monthly Actual/Forecast/Budget Hyperion Ingestion Duplicates",
        body="Below are the duplicates in the monthly actual/forecast/budget ingestion process",
        downstream_action="fail",
        distribution_list=email_lists.edw_engineering,
    )

    finance_forecast_base = SnowflakeProcedureOperator(
        procedure="reference.finance_monthly_budget_forecast_actual.sql",
        database="edw_prod",
    )
    finance_forecast_calculation = SnowflakeProcedureOperator(
        procedure="reference.finance_budget_forecast.sql",
        database="edw_prod",
    )
    finance_store_mapping_to_snowflake = SnowflakeIncrementalLoadOperator(
        task_id="finance_store_mapping_to_snowflake",
        snowflake_conn_id=conn_ids.Snowflake.default,
        database="lake",
        schema="fpa",
        table="finance_store_mapping",
        column_list=finance_store_mapping_column_list,
        files_path=f"{stages.tsos_da_int_vendor}/{finance_store_mapping_s3_prefix}",
        copy_config=CopyConfigCsv(header_rows=1),
    )
    finance_assumption = SnowflakeProcedureOperator(
        procedure="reference.finance_assumption.sql",
        database="edw_prod",
    )
    snapshot_finance_assumption = SnowflakeProcedureOperator(
        procedure="snapshot.finance_assumption.sql",
        database="edw_prod",
    )
    tableau_refresh_finance_forecast_budget = TableauRefreshOperator(
        task_id="tableau_refresh_finance_forecast_budget",
        data_source_id="cd00ab78-1b90-46cc-a69b-2f1ffa19e455",
    )

    (
        to_s3
        >> finance_store_mapping_to_snowflake
        >> finance_forecast_to_snowflake
        >> finance_forecast_base
        >> finance_forecast_calculation
        >> finance_ingestion_duplicate_alert
        >> finance_assumption
        >> snapshot_finance_assumption
        >> tableau_refresh_finance_forecast_budget
    )
