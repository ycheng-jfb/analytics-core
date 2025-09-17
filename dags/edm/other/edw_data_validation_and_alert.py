from dataclasses import dataclass
from pathlib import Path

import pendulum
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from pandas import DataFrame

from include import SQL_DIR
from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.hooks.snowflake import SnowflakeHook
from include.airflow.operators.snowflake import (
    SnowflakeAlertOperator,
    SnowflakeProcedureOperator,
)
from include.config import email_lists, owners, conn_ids
from include.utils.snowflake import generate_query_tag_cmd

default_args = {
    "start_date": pendulum.datetime(2021, 7, 29, tz="America/Los_Angeles"),
    "owner": owners.analytics_engineering,
    "email": email_lists.engineering_support,
    "on_failure_callback": slack_failure_edm,
}


dag = DAG(
    dag_id="edw_data_validation_and_alert",
    default_args=default_args,
    schedule=None,
    catchup=False,
    max_active_tasks=2,
    max_active_runs=1,
)


def check_run_time(**kwargs):
    execution_time = kwargs["data_interval_end"].in_timezone("America/Los_Angeles")
    if execution_time.hour == 1:
        return fce_snapshot.task_id
    else:
        return []


def check_retail_lead_run_time(**kwargs):
    execution_time = kwargs["data_interval_end"].in_timezone("America/Los_Angeles")
    if execution_time.hour == 1 and execution_time.day_of_week == 1:
        return retail_leads_alert.task_id
    else:
        return []


def check_actual_landed_cost_changes_run_time(**kwargs):
    execution_time = kwargs["data_interval_end"].in_timezone("America/Los_Angeles")
    if execution_time.hour == 18:
        return actual_landed_cost_changes_alert.task_id
    else:
        return []


def check_chargeback_email_alert_run_time(**kwargs):
    execution_time = kwargs["data_interval_end"].in_timezone("America/Los_Angeles")
    if execution_time.day_of_week not in [1, 0]:
        return chargeback_email_alert.task_id
    else:
        return []


def conn_to_sf():
    snowflake_hook = SnowflakeHook(
        snowflake_conn_id=conn_ids.Snowflake.default, database="EDW_PROD"
    )
    cnx = snowflake_hook.get_conn()
    cur = cnx.cursor()
    return cur


def edw_excp_cleanup(**kwargs):
    task_instance = kwargs["task_instance"]
    query_tag = generate_query_tag_cmd(task_instance.dag_id, task_instance.task_id)
    cur = conn_to_sf()
    cur.execute(query_tag)
    cur.execute(
        """SELECT table_name FROM information_schema.tables
        WHERE table_schema = \'EXCP\';"""
    )
    df = DataFrame(cur.fetchall())
    for ind in df.index:
        script = (
            """DELETE FROM excp."""
            + df[0][ind]
            + """ WHERE NOT meta_is_current_excp AND meta_data_quality = \'error\';"""
        )
        cur.execute(script)
    cur.close()


def consolidated_emails(**kwargs):
    task_instance = kwargs["task_instance"]
    query_tag = generate_query_tag_cmd(task_instance.dag_id, task_instance.task_id)
    cur = conn_to_sf()
    # create a table with validation query and add it to the following list, then add the procedure to validation_config
    table_list = [
        "edw_credit_equivalent_count",
        "edw_exception_count",
        "dim_credit_comparison",
        "dim_credit_unknown_report_mappings",
        "exchange_rate_check_for_duplicates",
        "fact_credit_event_historical_snapshot_comparison_count",
        "fact_credit_event_comparison",
        "fact_credit_event_orphan_records_count",
        "fact_order_and_fact_return_line_missing_assumptions",
        "finance_assumptions_fact_order_mismatch",
        "finance_assumptions_fact_return_line_mismatch",
        "finance_currency_conversion_missing_currency",
        "fl_scrubs_product_taxonomy",
        "landed_cost_zero",
        "membership_level_mismatch",
        "new_online_or_unknown_store",
        "new_store_in_dim_store",
        "fact_order_test_orders_count",
        "receivables_trx_name",
        "new_vendor_in_oracle_ebs",
        "chargeback_lake_edw_comparison",
        "new_membership_brand",
        "fact_order_missing_fx_rates",
        "warehouse_outlet_order_alert",
        "return_and_rma_duplicates",
        "utm_medium_and_utm_source_duplicates",
    ]
    scripts_list = [
        f""" SELECT '{i}' AS table_name, count(1) AS count  FROM validation.{i} a
             """
        for i in table_list
    ]
    scripts_list_join = "\nUNION ALL\n".join(scripts_list)
    cur.execute(query_tag)
    cur.execute(
        "CREATE TEMP TABLE validation._temp_tables_list as " + scripts_list_join + ";"
    )
    truncate_script = """TRUNCATE TABLE validation.emails_consolidated_list"""
    subject_list = """INSERT INTO validation.emails_consolidated_list
                    SELECT b.subject as alert_name, a.table_name as validation_table_name, count
                    from validation._temp_tables_list a
                    left join reference.validation_tables_info b ON a.table_name = b.table_name;"""
    cur.execute(truncate_script)
    cur.execute(subject_list)
    cur.close()


@dataclass
class ValidationConfig:
    procedure: str
    database: str


config_list = [
    ValidationConfig(
        procedure="validation.finance_currency_conversion_missing_currency.sql",
        database="edw_prod",
    ),
    ValidationConfig(
        procedure="validation.exchange_rate_check_for_duplicates.sql",
        database="edw_prod",
    ),
    ValidationConfig(
        procedure="validation.new_online_or_unknown_store.sql",
        database="edw_prod",
    ),
    ValidationConfig(
        procedure="validation.new_store_in_dim_store.sql",
        database="edw_prod",
    ),
    ValidationConfig(
        procedure="validation.finance_assumptions_fact_order_mismatch.sql",
        database="edw_prod",
    ),
    ValidationConfig(
        procedure="validation.finance_assumptions_fact_return_line_mismatch.sql",
        database="edw_prod",
    ),
    ValidationConfig(
        procedure="validation.fact_order_and_fact_return_line_missing_assumptions.sql",
        database="edw_prod",
    ),
    ValidationConfig(
        procedure="validation.edw_exception_count.sql",
        database="edw_prod",
    ),
    ValidationConfig(
        procedure="validation.fact_order_test_orders_count.sql",
        database="edw_prod",
    ),
    ValidationConfig(
        procedure="validation.membership_level_mismatch.sql",
        database="edw_prod",
    ),
    ValidationConfig(
        procedure="validation.fact_credit_event_comparison.sql",
        database="edw_prod",
    ),
    ValidationConfig(
        procedure="validation.dim_credit_comparison.sql",
        database="edw_prod",
    ),
    ValidationConfig(
        procedure="validation.fact_credit_event_orphan_records_count.sql",
        database="edw_prod",
    ),
    ValidationConfig(
        procedure="validation.landed_cost_zero.sql",
        database="edw_prod",
    ),
    ValidationConfig(
        procedure="validation.receivables_trx_name.sql",
        database="edw_prod",
    ),
    ValidationConfig(
        procedure="validation.dim_credit_unknown_report_mappings.sql",
        database="edw_prod",
    ),
    ValidationConfig(
        procedure="validation.edw_credit_equivalent_count.sql",
        database="edw_prod",
    ),
    ValidationConfig(
        procedure="validation.fl_scrubs_product_taxonomy.sql",
        database="edw_prod",
    ),
    ValidationConfig(
        procedure="validation.new_vendor_in_oracle_ebs.sql",
        database="edw_prod",
    ),
    ValidationConfig(
        procedure="validation.chargeback_lake_edw_comparison.sql",
        database="edw_prod",
    ),
    ValidationConfig(
        procedure="validation.new_membership_brand.sql",
        database="edw_prod",
    ),
    ValidationConfig(
        procedure="validation.fact_order_missing_fx_rates.sql",
        database="edw_prod",
    ),
    ValidationConfig(
        procedure="validation.warehouse_outlet_order_alert.sql",
        database="edw_prod",
    ),
    ValidationConfig(
        procedure="validation.return_and_rma_duplicates.sql",
        database="edw_prod",
    ),
    ValidationConfig(
        procedure="validation.utm_medium_and_utm_source_duplicates.sql",
        database="edw_prod",
    ),
]

fce_validation_config = ValidationConfig(
    procedure="validation.fact_credit_event_historical_snapshot_comparison_count.sql",
    database="edw_prod",
)

retail_lead_validation_config = ValidationConfig(
    procedure="validation.fake_retail_leads_with_activations.sql",
    database="edw_prod",
)

with dag:
    cleanup_edw_excp = PythonOperator(
        task_id="data_cleanup_for_edw_excp",
        python_callable=edw_excp_cleanup,
        provide_context=True,
    )

    dummy = EmptyOperator(task_id="Dummy")

    for i in config_list:
        op = SnowflakeProcedureOperator(
            procedure=i.procedure,
            database="edw_prod",
        )
        if i.procedure == "validation.edw_exception_count.sql":
            cleanup_edw_excp >> op
        op >> dummy

    skip_fce_snapshot = BranchPythonOperator(
        python_callable=check_run_time, task_id="check_run_time"
    )

    fce_snapshot = SnowflakeProcedureOperator(
        procedure=fce_validation_config.procedure,
        database="edw_prod",
    )

    skip_retail_leads = BranchPythonOperator(
        python_callable=check_retail_lead_run_time, task_id="check_retail_lead_run_time"
    )

    skip_actual_landed_cost_changes = BranchPythonOperator(
        python_callable=check_actual_landed_cost_changes_run_time,
        task_id="check_actual_landed_cost_changes_run_time",
    )

    skip_chargeback_email_alert = BranchPythonOperator(
        python_callable=check_chargeback_email_alert_run_time,
        task_id="check_chargeback_email_alert_run_time",
    )

    chargeback_distribution_list = (
        email_lists.edw_engineering
        + email_lists.central_analytics_support
        + ["vgaddam@techstyle.com"]
    )

    chargeback_email_alert = SnowflakeAlertOperator(
        task_id="chargeback_missing_dates",
        distribution_list=chargeback_distribution_list,
        database="EDW_PROD",
        subject="EDW Alert: Chargeback Data - Missing Dates",
        body="Below is the list of missing dates in chargeback source files:",
        sql_or_path=Path(
            SQL_DIR,
            "edw_prod",
            "procedures",
            "validation.chargeback_missing_dates.sql",
        ),
    )

    retail_leads_alert = SnowflakeAlertOperator(
        task_id="fake_retail_leads_with_activations",
        distribution_list=email_lists.edw_engineering
        + email_lists.central_analytics_support,
        database="EDW_PROD",
        subject="EDW Alert: Fake retail leads with Activations",
        body="Below are the list of customers with fake retail leads with activations"
        "They are added to the reference.fake_retail_leads_with_activations."
        "The is_fake_retail_registration flag for these customers "
        "in fact registration will be updated to FALSE on the next run:",
        sql_or_path=Path(
            SQL_DIR,
            "edw_prod",
            "procedures",
            "validation.fake_retail_leads_with_activations.sql",
        ),
    )

    actual_landed_cost_changes_distribution_list = email_lists.edw_engineering + [
        "jhay@techstyle.com",
        "ralluri@techstyle.com",
        "mgarza@techstyle.com",
        "calcorta@techstyle.com",
    ]
    actual_landed_cost_changes_alert = SnowflakeAlertOperator(
        task_id="actual_landed_cost_changes",
        distribution_list=actual_landed_cost_changes_distribution_list,
        database="EDW_PROD",
        subject="EDW Alert: Actual landed cost changed",
        body="Below is the list of SKUs for the given PO number, PO line number, and year-month received, "
        "where the actual landed cost has changed compared to yesterday's data, even though it was fully landed.",
        sql_or_path=Path(
            SQL_DIR,
            "edw_prod",
            "procedures",
            "validation.actual_landed_cost_changes.sql",
        ),
    )

    consolidated_email = PythonOperator(
        task_id="email_consolidation_call",
        python_callable=consolidated_emails,
        trigger_rule="none_failed",
        provide_context=True,
    )

    consolidated_email_alert = SnowflakeAlertOperator(
        task_id="list_of_validation_results",
        distribution_list=email_lists.edw_engineering,
        database="EDW_PROD",
        subject="Alert: EDW - Consolidated Result of all Validations",
        body="Below is the list of validation tables along with their count:",
        sql_or_path=Path(
            SQL_DIR,
            "edw_prod",
            "procedures",
            "validation.emails_consolidated_list.sql",
        ),
    )

    skip_retail_leads >> retail_leads_alert
    skip_chargeback_email_alert >> chargeback_email_alert
    skip_fce_snapshot >> fce_snapshot
    skip_actual_landed_cost_changes >> actual_landed_cost_changes_alert

    (
        [
            dummy,
            retail_leads_alert,
            chargeback_email_alert,
            fce_snapshot,
            actual_landed_cost_changes_alert,
        ]
        >> consolidated_email
        >> consolidated_email_alert
    )
