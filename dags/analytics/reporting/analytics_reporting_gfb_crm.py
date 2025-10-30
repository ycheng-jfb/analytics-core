import pendulum
from airflow.models import DAG
from airflow.operators.python import BranchPythonOperator

# from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.dag_helpers import chain_tasks
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.airflow.operators.tableau import TableauRefreshOperator
from include.config import email_lists, owners

default_args = {
    "start_date": pendulum.datetime(2021, 4, 26, tz="America/Los_Angeles"),
    'owner': owners.jfb_analytics,
    "email": email_lists.analytics_support,
    # "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id='analytics_reporting_gfb_crm',
    default_args=default_args,
    schedule='15 10,13 * * *',
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
)


# def check_date_and_time(**kwargs):
#     execution_time = kwargs['data_interval_end'].in_timezone('America/Los_Angeles')
#     if execution_time.hour in (10, 13):
#         return [crm_weekly_kpi_report.task_id]
#     else:
#         return []


with dag:
    # crm_order_attribution = SnowflakeProcedureOperator(
    #     procedure='gfb.crm_order_attribution.sql',
    #     database='reporting_prod',
    #     autocommit=False,
    # )
    crm_weekly_kpi_report = SnowflakeProcedureOperator(
        procedure='gfb.crm_weekly_kpi_report.sql',
        database='reporting_prod',
    )
    # crm_master_report = SnowflakeProcedureOperator(
    #     procedure='gfb.crm_master_report.sql',
    #     database='reporting_prod',
    #     warehouse='DA_WH_ETL_HEAVY',
    # )
    # tableau_jfb_all_market_crm_master_dash = TableauRefreshOperator(
    #     task_id='trigger_jfb_all_market_crm_master_dash_tableau_extract_refresh',
    #     data_source_name='JFB All Market CRM Master Dash',
    # )
    # tableau_jfb_crm_master_dash = TableauRefreshOperator(
    #     task_id='trigger_jfb_crm_master_dash_tableau_extract_refresh',
    #     data_source_name='JFB CRM Master Dash',
    # )
    # check_run = BranchPythonOperator(
    #     python_callable=check_date_and_time, task_id='check_run_date_and_time'
    # )
    # chain_tasks(check_run, crm_weekly_kpi_report, crm_master_report)
    # crm_order_attribution,
    # crm_weekly_kpi_report >> tableau_jfb_all_market_crm_master_dash
    # crm_master_report >> tableau_jfb_crm_master_dash
