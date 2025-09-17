from itertools import chain
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

from include import SQL_DIR
from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.dag_helpers import chain_tasks
from include.airflow.operators.snowflake import (
    SnowflakeAlertOperator,
    SnowflakeEdwProcedureOperator,
    SnowflakeProcedureOperator,
)
from include.config import email_lists, owners
from include.airflow.operators.tableau import TableauRefreshOperator


default_args = {
    "start_date": pendulum.datetime(2021, 4, 26, tz="America/Los_Angeles"),
    "owner": owners.data_integrations,
    "email": email_lists.engineering_support,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id="edm_analytics_base_customer_lifetime_value_monthly",
    default_args=default_args,
    schedule=None,
    catchup=False,
    max_active_tasks=2,
    max_active_runs=1,
)


def reporting_base_credit_execution_time(execution_date, context):
    run_time = context["data_interval_end"].in_timezone("America/Los_Angeles")
    return run_time - timedelta(hours=21, minutes=45)


def check_monthly_day2_to_day4(data_interval_end: pendulum.datetime):
    run_time = data_interval_end.in_timezone("America/Los_Angeles")
    if run_time.day in [2, 3, 4]:
        return "monthly_tasks_day2_to_day4_branch_out"
    else:
        return []


def check_monthly_day2(data_interval_end: pendulum.datetime):
    run_time = data_interval_end.in_timezone("America/Los_Angeles")
    if run_time.day == 2 and run_time.hour == 1:
        return "monthly_tasks_day2_branch_out"
    else:
        return []


with dag:
    check_monthly_day2_to_day4_br = BranchPythonOperator(
        task_id="check_monthly_day2_to_day4",
        python_callable=check_monthly_day2_to_day4,
    )
    monthly_tasks_day2_to_day4_branch_out = EmptyOperator(
        task_id="monthly_tasks_day2_to_day4_branch_out"
    )
    chain_tasks(check_monthly_day2_to_day4_br, monthly_tasks_day2_to_day4_branch_out)

    check_monthly_day2_br = BranchPythonOperator(
        task_id="check_monthly_day2",
        python_callable=check_monthly_day2,
    )
    monthly_tasks_day2_branch_out = EmptyOperator(
        task_id="monthly_tasks_day2_branch_out"
    )
    chain_tasks(check_monthly_day2_br, monthly_tasks_day2_branch_out)

    ltv_monthly = SnowflakeEdwProcedureOperator(
        procedure="analytics_base.customer_lifetime_value_monthly.sql",
        database="edw_prod",
        watermark_tables=[
            "edw_prod.analytics_base.finance_sales_ops",
            "edw_prod.stg.fact_activation",
        ],
        warehouse="DA_WH_ETL_HEAVY",
    )
    check_edm_edw_load_execution = ExternalTaskSensor(
        task_id="check_execution_of_edm_reporting_credits_daily",
        external_dag_id="edm_reporting_credits_daily",
        execution_date_fn=reporting_base_credit_execution_time,
        allowed_states=["success"],
        failed_states=["failed"],
        check_existence=True,
        timeout=2 * 60 * 60,
        poke_interval=60 * 10,
        mode="reschedule",
    )
    vip_tenure_final_output = SnowflakeProcedureOperator(
        procedure="reporting.vip_tenure_final_output.sql",
        database="edw_prod",
        warehouse="da_wh_edw",
    )

    tableau_fin014_vip_tenure = TableauRefreshOperator(
        task_id="trigger_tableau_fin014_vip_tenure",
        data_source_id="957d5431-2b23-4927-95e9-fdd8d086a854",
    )
    ltv_ltd = SnowflakeEdwProcedureOperator(
        procedure="analytics_base.customer_lifetime_value_ltd.sql",
        database="edw_prod",
        warehouse="DA_WH_ETL_HEAVY",
    )

    ltv_snapshot = SnowflakeProcedureOperator(
        procedure="snapshot.customer_lifetime_value_monthly.sql",
        database="edw_prod",
        warehouse="DA_WH_ETL_HEAVY",
    )

    cohort_waterfall = SnowflakeProcedureOperator(
        procedure="reporting.cohort_waterfall_final_output.sql",
        database="edw_prod",
        schema="reporting",
        warehouse="DA_WH_ETL_HEAVY",
    )

    compare_fso_vs_cltv = SnowflakeAlertOperator(
        task_id="finance_sales_ops_cltv_comparison",
        sql_or_path=Path(
            SQL_DIR,
            "edw_prod",
            "procedures",
            "validation.finance_sales_ops_cltv_comparison.sql",
        ),
        database="edw_prod",
        subject="Alert: EDW - Comparison of metrics between FSO and CLTV",
        body=(
            "Below is the list of differences between FSO and CLTV on cash gross revenue metric"
        ),
        distribution_list=[
            "yyoruk@techstyle.com",
        ],
    )
    chain_tasks(ltv_monthly, [ltv_ltd, ltv_snapshot], compare_fso_vs_cltv)
    chain_tasks(ltv_ltd, cohort_waterfall)
    chain_tasks(ltv_monthly, vip_tenure_final_output, tableau_fin014_vip_tenure)
    chain_tasks(
        monthly_tasks_day2_to_day4_branch_out,
        check_edm_edw_load_execution,
        vip_tenure_final_output,
    )
    chain_tasks(monthly_tasks_day2_branch_out, tableau_fin014_vip_tenure)
