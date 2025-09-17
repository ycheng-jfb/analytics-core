import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.snowflake import (
    SnowflakeAlertOperator,
    SnowflakeProcedureOperator,
)
from include.config import email_lists, owners
from airflow.operators.python import BranchPythonOperator

default_args = {
    "start_date": pendulum.datetime(2021, 9, 13, 7, tz="America/Los_Angeles"),
    "retries": 3,
    "owner": owners.analytics_engineering,
    "email": email_lists.edw_support,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id="edm_reporting_finance_sales_ops_validation_and_snapshot",
    default_args=default_args,
    schedule=None,
    catchup=False,
    max_active_tasks=2,
    max_active_runs=1,
)


def check_weekly_snapshot_time(**kwargs):
    execution_time = kwargs["data_interval_end"].in_timezone("America/Los_Angeles")
    if execution_time.hour == 1 and execution_time.day_of_week == 0:
        return fso_weekly_snapshot.task_id
    else:
        return []


variance_mail_sql = """
SELECT
    store_brand,
    store_country,
    store_type,
    gender,
    is_retail_vip,
    is_cross_promo,
    finance_specialty_store,
    membership_order_type_l3,
    date,
    metric,
    fso_value,
    fact_value,
    variance
FROM validation.finance_sales_ops_edw_comparison
WHERE ABS(variance) > .01 /* ignore rounding issue variances that have variances less than a penny */
ORDER BY ABS(variance) DESC
LIMIT 10;
"""

variance_mail_body = (
    """Below are the top 10 variances between FSO and fact/dim tables"""
)

with dag:
    fso_validation = SnowflakeProcedureOperator(
        procedure="validation.finance_sales_ops_edw_comparison.sql",
        database="edw_prod",
    )
    fso_snapshot = SnowflakeProcedureOperator(
        procedure="snapshot.finance_sales_ops.sql",
        database="edw_prod",
    )
    fso_snapshot_comparison = SnowflakeProcedureOperator(
        procedure="validation.finance_sales_ops_historical_snapshot_comparison.sql",
        database="edw_prod",
    )
    fso_variance_alert = SnowflakeAlertOperator(
        task_id="send_fso_variance_alert",
        sql_or_path=variance_mail_sql,
        database="edw_prod",
        distribution_list=[
            "yyoruk@techstyle.com",
            "kogata@techstyle.com",
            "dragan@techstyle.com",
            "rmukherjee@techstyle.com",
            "eschroder@JustFab.com",
            "lasplund@techstyle.com",
        ],
        subject="Alert: FSO Variance Against Fact Tables",
        body=variance_mail_body,
    )
    check_weekly_snapshot = BranchPythonOperator(
        task_id="check_weekly_snapshot_time",
        python_callable=check_weekly_snapshot_time,
    )
    fso_weekly_snapshot = SnowflakeProcedureOperator(
        procedure="snapshot.finance_sales_ops_weekly.sql",
        database="edw_prod",
    )


fso_validation >> fso_variance_alert
fso_snapshot >> fso_snapshot_comparison
check_weekly_snapshot >> fso_weekly_snapshot
