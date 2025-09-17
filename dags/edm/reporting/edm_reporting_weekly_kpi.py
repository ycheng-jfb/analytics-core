from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.dag_helpers import chain_tasks
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.airflow.operators.sqlagent import RunSqlAgent
from include.airflow.operators.tableau import TableauRefreshOperator
from include.config import email_lists, owners, conn_ids

default_args = {
    "start_date": pendulum.datetime(2021, 6, 1, 7, tz="America/Los_Angeles"),
    "owner": owners.central_analytics,
    "email": email_lists.edw_support,
}

dag = DAG(
    dag_id="edm_reporting_weekly_kpi",
    default_args=default_args,
    schedule="0 7 * * *",
    catchup=False,
    max_active_tasks=20,
    max_active_runs=1,
    on_failure_callback=[slack_failure_edm],
)

with dag:
    check_daily_agg_dag_run = ExternalTaskSensor(
        task_id="check_execution_of_edm_acquisition_media_spend_daily_agg_proc",
        external_dag_id="media_transform_cac_by_lead_channel_total_cac_report",
        external_task_id="edw_prod.analytics_base.acquisition_media_spend_daily_agg.sql",
        execution_delta=timedelta(minutes=-940),
        check_existence=True,
        timeout=7200,
        poke_interval=60 * 10,
        mode="reschedule",
    )
    check_p1_dag_run = ExternalTaskSensor(
        task_id="check_daily_cash_final_output_completion",
        external_dag_id="edm_reporting_p1",
        external_task_id="edw_prod.snapshot.daily_cash_final_output.sql",
        execution_delta=timedelta(minutes=-1095),
        timeout=7200,
        poke_interval=60 * 10,
        mode="reschedule",
        allowed_states=["success"],
    )
    run_procedure_retail_acquisition_final_output = SnowflakeProcedureOperator(
        procedure="reporting.retail_acquisition_final_output.sql", database="edw_prod"
    )
    tableau_retail_acquisitions = TableauRefreshOperator(
        task_id="tableau_retail_acquisitions",
        data_source_id="81fb402d-e44c-4c9b-9e66-f0c9b56636e6",
    )
    run_procedure_daily_cash_update = SnowflakeProcedureOperator(
        procedure="reporting.daily_cash_base_calc_update.sql", database="edw_prod"
    )
    run_procedure_daily_cash_final_output = SnowflakeProcedureOperator(
        procedure="reporting.daily_cash_final_output.sql", database="edw_prod"
    )
    run_procedure_daily_cash_final_output_snapshot = SnowflakeProcedureOperator(
        procedure="snapshot.daily_cash_final_output.sql", database="edw_prod"
    )
    weekly_kpi_report = RunSqlAgent(
        task_id="weekly_kpi",
        mssql_conn_id=conn_ids.MsSql.evolve01_app_airflow_rw,
        mssql_job_name="D4A411B9-4723-4577-912D-AD1C739B7E07",
    )
    run_procedure_token_to_giftco = SnowflakeProcedureOperator(
        procedure="shared.token_to_giftco_detail.sql", database="reporting_base_prod"
    )

    chain_tasks(
        [check_daily_agg_dag_run, check_p1_dag_run],
        run_procedure_daily_cash_update,
        run_procedure_daily_cash_final_output,
        run_procedure_daily_cash_final_output_snapshot,
        weekly_kpi_report,
        run_procedure_token_to_giftco,
    )
    chain_tasks(
        check_daily_agg_dag_run,
        run_procedure_retail_acquisition_final_output,
        tableau_retail_acquisitions,
    )
