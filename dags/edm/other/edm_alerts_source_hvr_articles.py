from pathlib import Path

import pendulum
from airflow.models import DAG

from include import SQL_DIR
from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.mssql import MsSqlAlertOperator
from include.config import conn_ids, owners
from include.config.email_lists import data_integration_support

default_args = {
    "depends_on_past": False,
    "start_date": pendulum.datetime(2019, 1, 1, 7, tz="America/Los_Angeles"),
    "retries": 1,
    "owner": owners.data_integrations,
    "email": data_integration_support,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id="edm_alerts_source_hvr_articles",
    default_args=default_args,
    schedule="5 1 * * *",
    catchup=False,
    max_active_tasks=100,
    max_active_runs=1,
)


with dag:
    alert_check_for_missing_evo_articles = MsSqlAlertOperator(
        task_id="alert_check_for_missing_evolve_articles",
        mssql_conn_id=conn_ids.MsSql.evolve01_app_airflow,
        sql_or_path=Path(
            SQL_DIR, "util", "procedures", "mssql.evolve_articles_checker.sql"
        ),
        alert_type="mail",
        subject="Alert: Missing Articles on EVOLVE01 SQL Server!",
        body=(
            "The following tables are missing Articles and changes can't be picked up by HVR!"
        ),
        distribution_list=data_integration_support,
    )

    alert_check_for_missing_jfb_articles = MsSqlAlertOperator(
        task_id="alert_check_for_missing_jfb_articles",
        mssql_conn_id=conn_ids.MsSql.justfab_app_airflow,
        sql_or_path=Path(
            SQL_DIR, "util", "procedures", "mssql.jfb_articles_checker.sql"
        ),
        alert_type="mail",
        subject="Alert: Missing Articles on JFBRANDS SQL Server!",
        body=(
            "The following tables are missing Articles and changes can't be picked up by HVR!"
        ),
        distribution_list=data_integration_support,
    )

    alert_check_for_missing_fbl_articles = MsSqlAlertOperator(
        task_id="alert_check_for_missing_fbl_articles",
        mssql_conn_id=conn_ids.MsSql.fabletics_app_airflow,
        sql_or_path=Path(
            SQL_DIR, "util", "procedures", "mssql.fbl_articles_checker.sql"
        ),
        alert_type="mail",
        subject="Alert: Missing Articles on FABLETICS SQL Server!",
        body=(
            "The following tables are missing Articles and changes can't be picked up by HVR!"
        ),
        distribution_list=data_integration_support,
    )

    alert_check_for_missing_sxf_articles = MsSqlAlertOperator(
        task_id="alert_check_for_missing_sxf_articles",
        mssql_conn_id=conn_ids.MsSql.savagex_app_airflow,
        sql_or_path=Path(
            SQL_DIR, "util", "procedures", "mssql.sxf_articles_checker.sql"
        ),
        alert_type="mail",
        subject="Alert: Missing Articles on SAVAGEX SQL Server!",
        body=(
            "The following tables are missing Articles and changes can't be picked up by HVR!"
        ),
        distribution_list=data_integration_support,
    )
