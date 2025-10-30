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
from task_configs.dag_config.edm_reporting_finance_month_end_config import table_list

default_args = {
    "start_date": pendulum.datetime(2022, 4, 1, 0, tz="America/Los_Angeles"),
    'owner': owners.central_analytics,
    "email": email_lists.data_integration_support,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id='edm_reporting_finance_month_end',
    default_args=default_args,
    schedule='0 12 1 * *',
    catchup=False,
    max_active_tasks=5,
)

s3_prefix = "/inbound/svc_oracle_ebs/lake.oracle_ebs.credits"

with dag:
    config_module = importlib.import_module(
        'task_configs.dag_config.edm_reporting_finance_month_end_config'
    )
    tfg_control = TFGControlOperator()
    to_snowflake: Dict[str, Any] = {}
    for table in table_list:
        col_list = getattr(config_module, f"{table}_column_list")
        to_snowflake[table] = SnowflakeCopyTableOperator(
            task_id=f"load_to_snowflake_{table}",
            database='reporting_base_prod',
            schema='shared',
            table=col_list.target_table_name,
            column_list=col_list.column_list,
            files_path=f"{stages.tsos_da_int_vendor}/{s3_prefix}/{table}/",
            copy_config=CopyConfigCsv(
                field_delimiter="\t",
                skip_pct=1,
            ),
        )

        to_snowflake[table]

    # loyalty point accrual redemption
    accrual_loyalty_points = SnowflakeProcedureOperator(
        procedure='credit.loyalty_points.sql', database='reporting_prod'
    )

    loyalty_points_accrual_to_redemption_no_refund_with_loop = SnowflakeProcedureOperator(
        procedure='credit.loyalty_points_accrual_to_redemption_no_refund_with_loop.sql',
        database='reporting_prod',
    )

    # snowflake breakage with all credit type
    breakage_fl_token_report = SnowflakeProcedureOperator(
        procedure='shared.fabletics_token_breakage_report.sql',
        database='reporting_base_prod',
        snapshot_enabled=True,
        snapshot_period=6,
    )

    breakage_fl_converted_report = SnowflakeProcedureOperator(
        procedure='shared.fabletics_converted_credits_breakage_report.sql',
        database='reporting_base_prod',
        snapshot_enabled=True,
        snapshot_period=6,
    )

    breakage_sxg_token_report = SnowflakeProcedureOperator(
        procedure='shared.savagex_token_breakage_report.sql',
        database='reporting_base_prod',
        snapshot_enabled=True,
        snapshot_period=6,
    )

    breakage_sxg_converted_report = SnowflakeProcedureOperator(
        procedure='shared.savagex_converted_credits_breakage_report.sql',
        database='reporting_base_prod',
        snapshot_enabled=True,
        snapshot_period=6,
    )

    breakage_sxg_us_report = SnowflakeProcedureOperator(
        procedure='shared.savagex_us_breakage_report.sql',
        database='reporting_base_prod',
        snapshot_enabled=True,
        snapshot_period=6,
    )

    breakage_jfb_sxf_membership_report = SnowflakeProcedureOperator(
        procedure='shared.jfb_sxf_membership_credit_breakage_report.sql',
        database='reporting_base_prod',
        snapshot_enabled=True,
        snapshot_period=6,
    )

    breakage_jfb_token_report = SnowflakeProcedureOperator(
        procedure='shared.jfb_token_breakage_report.sql',
        database='reporting_base_prod',
        snapshot_enabled=True,
        snapshot_period=6,
    )

    breakage_jfb_converted_report = SnowflakeProcedureOperator(
        procedure='shared.jfb_converted_breakage_report.sql',
        database='reporting_base_prod',
        snapshot_enabled=True,
        snapshot_period=6,
    )

    tax_breakage = SnowflakeProcedureOperator(
        procedure='shared.tax_breakage.sql',
        database='reporting_base_prod',
    )

    breakage_ssrs_reports = EmptyOperator(task_id='trigger_dummy_breakage_ssrs_reports')

    # giftco
    giftco = SnowflakeProcedureOperator(
        procedure='shared.fin012_giftco_rollforward.sql',
        database='reporting_base_prod',
    )

    # refund reports
    captured_transaction = SnowflakeProcedureOperator(
        procedure='shared.captured_transactions.sql',
        database='reporting_base_prod',
    )
    credit_billing_refund_waterfall = SnowflakeProcedureOperator(
        procedure='shared.credit_billing_refund_waterfall.sql',
        database='reporting_base_prod',
    )
    gaap_refund_waterfall = SnowflakeProcedureOperator(
        procedure='shared.gaap_refund_waterfalls.sql',
        database='reporting_base_prod',
    )
    product_dollars_by_company = SnowflakeProcedureOperator(
        procedure='shared.product_dollars_by_company.sql',
        database='reporting_base_prod',
    )
    shipping_revenue = SnowflakeProcedureOperator(
        procedure='shared.shipping_revenue.sql',
        database='reporting_base_prod',
    )
    chargeback_waterfall = SnowflakeProcedureOperator(
        procedure='shared.chargeback_waterfall.sql',
        database='reporting_base_prod',
    )

    # Tax reports
    accessory_tax = SnowflakeProcedureOperator(
        procedure='shared.accessory_tax.sql',
        database='reporting_base_prod',
    )
    na_state_tax = SnowflakeProcedureOperator(
        procedure='shared.na_state_tax.sql',
        database='reporting_base_prod',
    )
    na_sales_tax_with_warehouse = SnowflakeProcedureOperator(
        procedure='shared.na_sales_tax_with_warehouse_id.sql',
        database='reporting_base_prod',
    )
    token_purchases = SnowflakeProcedureOperator(
        procedure='shared.token_purchases.sql',
        database='reporting_base_prod',
    )

    # retail reports
    retail_sales_refund = SnowflakeProcedureOperator(
        procedure='shared.retail_sales_refund.sql',
        database='reporting_base_prod',
    )
    ship_only_transaction = SnowflakeProcedureOperator(
        procedure='shared.ship_only_transaction.sql',
        database='reporting_base_prod',
    )
    retail_discount_order = SnowflakeProcedureOperator(
        procedure='shared.retail_discount_orders.sql',
        database='reporting_base_prod',
    )

    # loyalty report
    other_loyalty_points_report = SnowflakeProcedureOperator(
        procedure='shared.loyalty_point_detail.sql',
        database='reporting_base_prod',
    )

    # emp token report
    emp_token = SnowflakeProcedureOperator(
        procedure='shared.emp_token.sql',
        database='reporting_base_prod',
    )

    # emp outstanding token report
    emp_token_outstanding = SnowflakeProcedureOperator(
        procedure='shared.emp_token_outstanding.sql',
        database='reporting_base_prod',
    )

    # outstanding credit count by status report
    outstanding_credit_count_by_status = SnowflakeProcedureOperator(
        procedure='shared.fin008_outstanding_credit_count_by_status.sql',
        database='reporting_prod',
    )

    # breakage data snapshot
    original_credit_activity_snapshot = SnowflakeProcedureOperator(
        procedure='shared.original_credit_activity_snapshot.sql',
        database='reporting_base_prod',
    )

    breakage_by_state_actuals = SnowflakeProcedureOperator(
        procedure='shared.breakage_by_state_actuals.sql',
        database='reporting_prod',
        snapshot_enabled=True,
        snapshot_period=6,
    )

    [
        emp_token >> [emp_token_outstanding],
        captured_transaction,
        credit_billing_refund_waterfall,
        gaap_refund_waterfall,
        product_dollars_by_company,
        shipping_revenue,
        chargeback_waterfall,
        accessory_tax,
        na_state_tax,
        na_sales_tax_with_warehouse,
        token_purchases,
        retail_sales_refund,
        ship_only_transaction,
        giftco,
        retail_discount_order,
        breakage_by_state_actuals,
        outstanding_credit_count_by_status,
        accrual_loyalty_points
        >> [loyalty_points_accrual_to_redemption_no_refund_with_loop, other_loyalty_points_report],
        [
            breakage_fl_token_report,
            breakage_fl_converted_report,
            breakage_sxg_token_report,
            breakage_sxg_converted_report,
            breakage_sxg_us_report,
            breakage_jfb_sxf_membership_report,
            breakage_jfb_token_report,
            breakage_jfb_converted_report,
        ]
        >> tax_breakage
        >> breakage_ssrs_reports
        >> original_credit_activity_snapshot,
    ]
