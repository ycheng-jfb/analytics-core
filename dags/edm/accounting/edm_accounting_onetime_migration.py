from dataclasses import dataclass
from pathlib import Path

import pendulum
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator

from include import SQL_DIR
from include.airflow.operators.snowflake import SnowflakeSqlOperator
from include.config import owners, conn_ids
from include.utils.string import unindent_auto

default_args = {
    "start_date": pendulum.datetime(2024, 8, 28, 0, tz="America/Los_Angeles"),
    "retries": 1,
    "owner": owners.data_integrations,
}

dag = DAG(
    dag_id="edm_accounting_onetime_migration",
    default_args=default_args,
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=5,
)


@dataclass
class TableConfig:
    src_schema: str
    src_table: str
    src_database: str = "reporting_base_prod"
    tgt_schema: str = None
    tgt_table: str = None
    tgt_database: str = "accounting"

    @property
    def to_snowflake(self):
        self.tgt_schema = self.tgt_schema if self.tgt_schema else self.src_schema
        self.tgt_table = self.tgt_table if self.tgt_table else self.src_table

        source_table = f"{self.src_database}.{self.src_schema}.{self.src_table}"
        target_table = f"{self.tgt_database}.{self.tgt_schema}.{self.tgt_table}"

        sf_command = f"""
                CREATE TRANSIENT TABLE IF NOT EXISTS {target_table} CLONE {source_table};
                """

        return SnowflakeSqlOperator(
            task_id=f"migrate_table_{source_table}_to_accounting",
            sql_or_path=unindent_auto(sf_command),
            snowflake_conn_id=conn_ids.Snowflake.accounting,
        )


@dataclass
class ViewConfig:
    database: str = None
    view_script: str = None

    @property
    def to_snowflake(self):
        target_view = f"{self.database}.{self.view_script}"

        script_filename = Path(SQL_DIR, self.database, "views", self.view_script)

        with open(script_filename.as_posix(), "rt") as f:
            sf_command = f.read()
            sf_command = f"USE DATABASE {self.database}; " + sf_command

        return SnowflakeSqlOperator(
            task_id=f"migrate_view_{target_view}_to_accounting",
            sql_or_path=unindent_auto(sf_command),
            snowflake_conn_id=conn_ids.Snowflake.accounting,
        )


@dataclass
class FunctionConfig:
    database: str = None
    function_script: str = None

    @property
    def to_snowflake(self):
        target_view = f"{self.database}.{self.function_script}"

        script_filename = Path(
            SQL_DIR, self.database, "functions", self.function_script
        )

        with open(script_filename.as_posix(), "rt") as f:
            sf_command = f.read()
            sf_command = f"USE DATABASE {self.database}; " + sf_command

        return SnowflakeSqlOperator(
            task_id=f"migrate_function_{target_view}_to_accounting",
            sql_or_path=unindent_auto(sf_command),
            snowflake_conn_id=conn_ids.Snowflake.accounting,
        )


table_list = [
    TableConfig(
        src_database="reporting_prod",
        src_schema="credit",
        src_table="loyalty_points",
        tgt_schema="month_end",
    ),
    TableConfig(
        src_database="reporting_prod",
        src_schema="credit",
        src_table="loyalty_points_balance",
        tgt_schema="month_end",
    ),
    TableConfig(
        src_database="reporting_prod",
        src_schema="credit",
        src_table="loyalty_points_accrual_to_redemption_no_refund_with_loop",
        tgt_schema="month_end",
    ),
    TableConfig(
        src_database="reporting_base_prod",
        src_schema="shared",
        src_table="fabletics_token_breakage_report",
        tgt_schema="breakage",
    ),
    TableConfig(
        src_database="reporting_base_prod",
        src_schema="shared",
        src_table="fabletics_token_breakage_report_reset",
        tgt_schema="breakage",
    ),
    TableConfig(
        src_database="reporting_base_prod",
        src_schema="shared",
        src_table="fabletics_converted_credits_breakage_report",
        tgt_schema="breakage",
    ),
    TableConfig(
        src_database="reporting_base_prod",
        src_schema="shared",
        src_table="savagex_us_breakage_report",
        tgt_schema="breakage",
    ),
    TableConfig(
        src_database="reporting_base_prod",
        src_schema="shared",
        src_table="jfb_sxf_membership_credit_breakage_report",
        tgt_schema="breakage",
    ),
    TableConfig(
        src_database="reporting_base_prod",
        src_schema="shared",
        src_table="jfb_token_breakage_report",
        tgt_schema="breakage",
    ),
    TableConfig(
        src_database="reporting_base_prod",
        src_schema="shared",
        src_table="jfb_converted_breakage_report",
        tgt_schema="breakage",
    ),
    TableConfig(
        src_database="reporting_base_prod",
        src_schema="shared",
        src_table="tax_breakage",
        tgt_schema="breakage",
    ),
    TableConfig(
        src_database="reporting_base_prod",
        src_schema="shared",
        src_table="tax_breakage_snapshot",
        tgt_schema="breakage",
    ),
    TableConfig(
        src_database="reporting_base_prod",
        src_schema="shared",
        src_table="fin012_giftco_rollforward",
        tgt_schema="month_end",
    ),
    TableConfig(
        src_database="reporting_base_prod",
        src_schema="shared",
        src_table="fin012_giftco_rollforward_snapshot",
        tgt_schema="month_end",
    ),
    TableConfig(
        src_database="reporting_base_prod",
        src_schema="shared",
        src_table="captured_transactions",
        tgt_schema="month_end",
    ),
    TableConfig(
        src_database="reporting_base_prod",
        src_schema="shared",
        src_table="captured_transactions_snapshot",
        tgt_schema="month_end",
    ),
    TableConfig(
        src_database="reporting_base_prod",
        src_schema="shared",
        src_table="credit_billing_refund_waterfalls_order_detail",
        tgt_schema="month_end",
    ),
    TableConfig(
        src_database="reporting_base_prod",
        src_schema="shared",
        src_table="credit_billing_refund_waterfalls_refund_detail",
        tgt_schema="month_end",
    ),
    TableConfig(
        src_database="reporting_base_prod",
        src_schema="shared",
        src_table="credit_billing_refund_waterfalls_order_refund_detail",
        tgt_schema="month_end",
    ),
    TableConfig(
        src_database="reporting_base_prod",
        src_schema="shared",
        src_table="credit_billing_refund_waterfalls_refund_detail_snapshot",
        tgt_schema="month_end",
    ),
    TableConfig(
        src_database="reporting_base_prod",
        src_schema="shared",
        src_table="gaap_refund_waterfalls_order_breakout_detail",
        tgt_schema="month_end",
    ),
    TableConfig(
        src_database="reporting_base_prod",
        src_schema="shared",
        src_table="gaap_refund_waterfalls_order_detail",
        tgt_schema="month_end",
    ),
    TableConfig(
        src_database="reporting_base_prod",
        src_schema="shared",
        src_table="gaap_refund_waterfalls_refund_detail",
        tgt_schema="month_end",
    ),
    TableConfig(
        src_database="reporting_base_prod",
        src_schema="shared",
        src_table="gaap_refund_waterfalls_full",
        tgt_schema="month_end",
    ),
    TableConfig(
        src_database="reporting_base_prod",
        src_schema="shared",
        src_table="gaap_refund_waterfalls_refund_detail_snapshot",
        tgt_schema="month_end",
    ),
    TableConfig(
        src_database="reporting_base_prod",
        src_schema="shared",
        src_table="gaap_refund_waterfalls_order_detail_snapshot",
        tgt_schema="month_end",
    ),
    TableConfig(
        src_database="reporting_base_prod",
        src_schema="shared",
        src_table="cash_noncash_credit_redemptions",
        tgt_schema="month_end",
    ),
    TableConfig(
        src_database="reporting_base_prod",
        src_schema="shared",
        src_table="product_dollars_by_company_detail",
        tgt_schema="month_end",
    ),
    TableConfig(
        src_database="reporting_base_prod",
        src_schema="shared",
        src_table="product_dollars_by_company_detail_snapshot",
        tgt_schema="month_end",
    ),
    TableConfig(
        src_database="reporting_base_prod",
        src_schema="shared",
        src_table="shipping_revenue_detail",
        tgt_schema="month_end",
    ),
    TableConfig(
        src_database="reporting_base_prod",
        src_schema="shared",
        src_table="shipping_revenue_detail_snapshot",
        tgt_schema="month_end",
    ),
    TableConfig(
        src_database="reporting_base_prod",
        src_schema="shared",
        src_table="chargeback_waterfall_detail",
        tgt_schema="month_end",
    ),
    TableConfig(
        src_database="reporting_base_prod",
        src_schema="shared",
        src_table="chargeback_waterfall_detail_snapshot",
        tgt_schema="month_end",
    ),
    TableConfig(
        src_database="reporting_base_prod",
        src_schema="shared",
        src_table="nj_mn_accessories_sales_tax_detail",
        tgt_schema="month_end",
    ),
    TableConfig(
        src_database="reporting_base_prod",
        src_schema="shared",
        src_table="nj_mn_accessories_sales_tax_detail_snapshot",
        tgt_schema="month_end",
    ),
    TableConfig(
        src_database="reporting_base_prod",
        src_schema="shared",
        src_table="na_sales_tax_detail",
        tgt_schema="month_end",
    ),
    TableConfig(
        src_database="reporting_base_prod",
        src_schema="shared",
        src_table="na_sales_tax_detail_snapshot",
        tgt_schema="month_end",
    ),
    TableConfig(
        src_database="reporting_base_prod",
        src_schema="shared",
        src_table="na_sales_tax_with_warehouse_detail",
        tgt_schema="month_end",
    ),
    TableConfig(
        src_database="reporting_base_prod",
        src_schema="shared",
        src_table="na_sales_tax_with_warehouse_detail_snapshot",
        tgt_schema="month_end",
    ),
    TableConfig(
        src_database="reporting_base_prod",
        src_schema="shared",
        src_table="token_purchases_tax",
        tgt_schema="month_end",
    ),
    TableConfig(
        src_database="reporting_base_prod",
        src_schema="shared",
        src_table="token_purchases_tax_snapshot",
        tgt_schema="month_end",
    ),
    TableConfig(
        src_database="reporting_base_prod",
        src_schema="shared",
        src_table="retail_sales_and_refund_detail",
        tgt_schema="month_end",
    ),
    TableConfig(
        src_database="reporting_base_prod",
        src_schema="shared",
        src_table="retail_sales_and_refund_detail_snapshot",
        tgt_schema="month_end",
    ),
    TableConfig(
        src_database="reporting_base_prod",
        src_schema="shared",
        src_table="retail_orders",
        tgt_schema="month_end",
    ),
    TableConfig(
        src_database="reporting_base_prod",
        src_schema="shared",
        src_table="retail_ship_only_sales",
        tgt_schema="month_end",
    ),
    TableConfig(
        src_database="reporting_base_prod",
        src_schema="shared",
        src_table="retail_ship_only_refunds",
        tgt_schema="month_end",
    ),
    TableConfig(
        src_database="reporting_base_prod",
        src_schema="shared",
        src_table="retail_ship_only_sales_snapshot",
        tgt_schema="month_end",
    ),
    TableConfig(
        src_database="reporting_base_prod",
        src_schema="shared",
        src_table="retail_ship_only_refunds_snapshot",
        tgt_schema="month_end",
    ),
    TableConfig(
        src_database="reporting_base_prod",
        src_schema="shared",
        src_table="retail_discount_order",
        tgt_schema="month_end",
    ),
    TableConfig(
        src_database="reporting_base_prod",
        src_schema="shared",
        src_table="retail_discount_order_snapshot",
        tgt_schema="month_end",
    ),
    TableConfig(
        src_database="reporting_base_prod",
        src_schema="shared",
        src_table="loyalty_points_detail",
        tgt_schema="month_end",
    ),
    TableConfig(
        src_database="reporting_base_prod",
        src_schema="shared",
        src_table="loyalty_points_discount",
        tgt_schema="month_end",
    ),
    TableConfig(
        src_database="reporting_base_prod",
        src_schema="shared",
        src_table="loyalty_items_sold_price_per_point_detail",
        tgt_schema="month_end",
    ),
    TableConfig(
        src_database="reporting_base_prod",
        src_schema="shared",
        src_table="loyalty_items_sold_price_per_point_detail_snapshot",
        tgt_schema="month_end",
    ),
    TableConfig(
        src_database="reporting_base_prod",
        src_schema="shared",
        src_table="loyalty_points_detail_snapshot",
        tgt_schema="month_end",
    ),
    TableConfig(
        src_database="reporting_base_prod",
        src_schema="shared",
        src_table="emp_token",
        tgt_schema="month_end",
    ),
    TableConfig(
        src_database="reporting_base_prod",
        src_schema="shared",
        src_table="emp_token_outstanding",
        tgt_schema="month_end",
    ),
    TableConfig(
        src_database="reporting_base_prod",
        src_schema="shared",
        src_table="emp_token_outstanding_snapshot",
        tgt_schema="month_end",
    ),
    TableConfig(
        src_database="reporting_prod",
        src_schema="shared",
        src_table="fin008_by_customer_snapshot",
        tgt_schema="month_end",
    ),
    TableConfig(
        src_database="reporting_prod",
        src_schema="shared",
        src_table="fin008_outstanding_credit_count_by_status",
        tgt_schema="month_end",
    ),
    TableConfig(
        src_database="reporting_base_prod",
        src_schema="shared",
        src_table="original_credit_activity_snapshot",
        tgt_schema="breakage",
    ),
    TableConfig(
        src_database="reporting_prod",
        src_schema="shared",
        src_table="breakage_by_state_actuals",
        tgt_schema="breakage",
    ),
]

view_list = [
    ViewConfig(
        database="accounting",
        view_script="breakage.fabletics_breakage_report_ssrs_base.sql",
    ),
    ViewConfig(
        database="accounting",
        view_script="breakage.jfb_breakage_report_ssrs_base.sql",
    ),
    ViewConfig(
        database="accounting",
        view_script="breakage.sxf_breakage_report_ssrs_base.sql",
    ),
]

function_list = [
    FunctionConfig(
        database="accounting",
        function_script="month_end.udf_correct_state_country.sql",
    ),
]

with dag:
    table_migration_complete = EmptyOperator(task_id="table_migration_completion")

    for table in table_list:
        table_to_snowflake = table.to_snowflake
        table_to_snowflake >> table_migration_complete

    for view in view_list:
        view_to_snowflake = view.to_snowflake
        table_migration_complete >> view_to_snowflake

    for function in function_list:
        function_to_snowflake = function.to_snowflake
        table_migration_complete >> function_to_snowflake
