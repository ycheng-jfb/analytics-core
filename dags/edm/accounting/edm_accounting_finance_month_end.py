import importlib
from typing import Any, Dict

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.dag_helpers import TFGControlOperator
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.airflow.operators.snowflake_load import CopyConfigCsv, SnowflakeCopyTableOperator
from include.airflow.operators.sqlagent import RunSqlAgent
from include.config import email_lists, owners, stages, conn_ids

default_args = {
    "start_date": pendulum.datetime(2024, 7, 1, 0, tz="America/Los_Angeles"),
    'owner': owners.central_analytics,
    "email": email_lists.central_analytics_support,
    "on_failure_callback": slack_failure_edm,
    "snowflake_conn_id": 'snowflake_accounting',
    "role": 'ETL_ACCOUNTING_PROD',
}

dag = DAG(
    dag_id='edm_accounting_finance_month_end',
    default_args=default_args,
    schedule='0 12 1 * *',
    catchup=False,
    max_active_tasks=5,
)

with dag:

    # snowflake breakage with all credit type
    breakage_fl_token_report = SnowflakeProcedureOperator(
        procedure='breakage.fabletics_token_breakage_report.sql',
        database='accounting',
    )

    breakage_fl_converted_report = SnowflakeProcedureOperator(
        procedure='breakage.fabletics_converted_credits_breakage_report.sql',
        database='accounting',
    )

    breakage_sxg_us_report = SnowflakeProcedureOperator(
        procedure='breakage.savagex_us_breakage_report.sql',
        database='accounting',
    )

    breakage_jfb_sxf_membership_report = SnowflakeProcedureOperator(
        procedure='breakage.jfb_sxf_membership_credit_breakage_report.sql',
        database='accounting',
    )

    breakage_jfb_token_report = SnowflakeProcedureOperator(
        procedure='breakage.jfb_token_breakage_report.sql',
        database='accounting',
    )

    breakage_jfb_converted_report = SnowflakeProcedureOperator(
        procedure='breakage.jfb_converted_breakage_report.sql',
        database='accounting',
    )

    tax_breakage = SnowflakeProcedureOperator(
        procedure='breakage.tax_breakage.sql', database='accounting'
    )

    breakage_ssrs_reports = RunSqlAgent(
        task_id='trigger_breakage_ssrs_reports',
        mssql_conn_id=conn_ids.MsSql.evolve01_app_airflow_rw,
        mssql_job_name='32C3252B-9BFF-4853-9DEB-A9DEB8744934',
    )

    # loyalty points report
    loyalty_points = SnowflakeProcedureOperator(
        procedure='month_end.loyalty_points.sql',
        database='accounting',
    )

    loyalty_points_balance = SnowflakeProcedureOperator(
        procedure='month_end.loyalty_points_balance.sql',
        database='accounting',
    )

    loyalty_points_accrual_to_redemption_no_refund_with_loop = SnowflakeProcedureOperator(
        procedure='month_end.loyalty_points_accrual_to_redemption_no_refund_with_loop.sql',
        database='accounting',
    )

    loyalty_points_detail = SnowflakeProcedureOperator(
        procedure='month_end.loyalty_points_detail.sql',
        database='accounting',
    )

    loyalty_points_discount = SnowflakeProcedureOperator(
        procedure='month_end.loyalty_points_discount.sql',
        database='accounting',
    )

    loyalty_items_sold_price_per_point_detail = SnowflakeProcedureOperator(
        procedure='month_end.loyalty_items_sold_price_per_point_detail.sql',
        database='accounting',
    )

    # giftco report
    giftco = SnowflakeProcedureOperator(
        procedure='month_end.fin012_giftco_rollforward.sql',
        database='accounting',
    )

    # captured transactions report
    captured_transaction = SnowflakeProcedureOperator(
        procedure='month_end.captured_transactions.sql',
        database='accounting',
    )

    # refund reports
    credit_billing_refund_waterfalls_order_detail = SnowflakeProcedureOperator(
        procedure='month_end.credit_billing_refund_waterfalls_order_detail.sql',
        database='accounting',
    )

    credit_billing_refund_waterfalls_refund_detail = SnowflakeProcedureOperator(
        procedure='month_end.credit_billing_refund_waterfalls_refund_detail.sql',
        database='accounting',
    )

    credit_billing_refund_waterfall = SnowflakeProcedureOperator(
        procedure='month_end.credit_billing_refund_waterfalls_order_refund_detail.sql',
        database='accounting',
    )

    gaap_refund_waterfalls_order_breakout_detail = SnowflakeProcedureOperator(
        procedure='month_end.gaap_refund_waterfalls_order_breakout_detail.sql',
        database='accounting',
    )

    gaap_refund_waterfalls_order_detail = SnowflakeProcedureOperator(
        procedure='month_end.gaap_refund_waterfalls_order_detail.sql',
        database='accounting',
    )

    gaap_refund_waterfalls_refund_detail = SnowflakeProcedureOperator(
        procedure='month_end.gaap_refund_waterfalls_refund_detail.sql',
        database='accounting',
    )

    gaap_refund_waterfall = SnowflakeProcedureOperator(
        procedure='month_end.gaap_refund_waterfalls_full.sql',
        database='accounting',
    )

    cash_noncash_credit_redemptions = SnowflakeProcedureOperator(
        procedure='month_end.cash_noncash_credit_redemptions.sql',
        database='accounting',
    )

    product_dollars_by_company = SnowflakeProcedureOperator(
        procedure='month_end.product_dollars_by_company_detail.sql',
        database='accounting',
    )

    shipping_revenue = SnowflakeProcedureOperator(
        procedure='month_end.shipping_revenue_detail.sql',
        database='accounting',
    )

    chargeback_waterfall = SnowflakeProcedureOperator(
        procedure='month_end.chargeback_waterfall_detail.sql',
        database='accounting',
    )

    # Tax reports
    accessory_tax = SnowflakeProcedureOperator(
        procedure='month_end.nj_mn_accessories_sales_tax_detail.sql',
        database='accounting',
    )

    na_state_tax = SnowflakeProcedureOperator(
        procedure='month_end.na_sales_tax_detail.sql',
        database='accounting',
    )

    na_sales_tax_with_warehouse = SnowflakeProcedureOperator(
        procedure='month_end.na_sales_tax_with_warehouse_detail.sql',
        database='accounting',
    )

    token_purchases_tax = SnowflakeProcedureOperator(
        procedure='month_end.token_purchases_tax.sql',
        database='accounting',
    )

    # retail reports
    retail_sales_and_refund_detail = SnowflakeProcedureOperator(
        procedure='month_end.retail_sales_and_refund_detail.sql',
        database='accounting',
    )

    retail_orders = SnowflakeProcedureOperator(
        procedure='month_end.retail_orders.sql',
        database='accounting',
    )

    retail_ship_only_sales = SnowflakeProcedureOperator(
        procedure='month_end.retail_ship_only_sales.sql',
        database='accounting',
    )

    retail_ship_only_refunds = SnowflakeProcedureOperator(
        procedure='month_end.retail_ship_only_refunds.sql',
        database='accounting',
    )

    retail_discount_order = SnowflakeProcedureOperator(
        procedure='month_end.retail_discount_order.sql',
        database='accounting',
    )

    # emp token report
    emp_token = SnowflakeProcedureOperator(
        procedure='month_end.emp_token.sql',
        database='accounting',
    )

    # emp outstanding token report
    emp_token_outstanding = SnowflakeProcedureOperator(
        procedure='month_end.emp_token_outstanding.sql',
        database='accounting',
    )

    # outstanding credit count by status report
    dates_for_fin008 = SnowflakeProcedureOperator(
        procedure='month_end.dates_for_fin008.sql',
        database='accounting',
    )

    fin008_by_customer = SnowflakeProcedureOperator(
        procedure='month_end.fin008_by_customer.sql',
        database='accounting',
    )

    fin008_outstanding_credit_count_by_status = SnowflakeProcedureOperator(
        procedure='month_end.fin008_outstanding_credit_count_by_status.sql',
        database='accounting',
    )

    # breakage data snapshot
    original_credit_activity_snapshot = SnowflakeProcedureOperator(
        procedure='breakage.original_credit_activity_snapshot.sql',
        database='accounting',
    )

    breakage_by_state_actuals = SnowflakeProcedureOperator(
        procedure='breakage.breakage_by_state_actuals.sql',
        database='accounting',
    )

    emp_token >> emp_token_outstanding
    retail_orders >> retail_ship_only_sales >> retail_ship_only_refunds
    dates_for_fin008 >> fin008_by_customer >> fin008_outstanding_credit_count_by_status
    (
        loyalty_points
        >> loyalty_points_balance
        >> loyalty_points_accrual_to_redemption_no_refund_with_loop
    )
    (
        loyalty_points
        >> loyalty_points_detail
        >> [loyalty_points_discount, loyalty_items_sold_price_per_point_detail]
    )

    (
        credit_billing_refund_waterfalls_order_detail
        >> credit_billing_refund_waterfalls_refund_detail
        >> credit_billing_refund_waterfall
    )
    (
        gaap_refund_waterfalls_order_breakout_detail
        >> gaap_refund_waterfalls_order_detail
        >> gaap_refund_waterfalls_refund_detail
        >> gaap_refund_waterfall
    )
    cash_noncash_credit_redemptions >> product_dollars_by_company

    (
        [
            breakage_fl_token_report,
            breakage_fl_converted_report,
            breakage_sxg_us_report,
            breakage_jfb_sxf_membership_report,
            breakage_jfb_token_report,
            breakage_jfb_converted_report,
        ]
        >> tax_breakage
        >> breakage_ssrs_reports
        >> original_credit_activity_snapshot
    )
