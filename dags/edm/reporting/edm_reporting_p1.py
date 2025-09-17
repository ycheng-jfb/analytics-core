import pendulum
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator

from include.airflow.callbacks.slack import slack_failure_p1_edw
from include.airflow.dag_helpers import chain_tasks
from include.airflow.operators.dagrun_operator import TFGTriggerDagRunOperator
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.airflow.operators.sqlagent import RunSqlAgent
from include.airflow.operators.tableau import TableauRefreshOperator
from include.config import email_lists, owners, conn_ids

default_args = {
    "start_date": pendulum.datetime(2019, 1, 1, 7, tz="America/Los_Angeles"),
    "retries": 3,
    "owner": owners.central_analytics,
    "email": email_lists.edw_support,
    "on_failure_callback": slack_failure_p1_edw,
}

dag = DAG(
    dag_id="edm_reporting_p1",
    default_args=default_args,
    schedule=None,
    catchup=False,
    max_active_tasks=5,
    max_active_runs=1,
)


def get_next_execution_date(execution_date, context):
    return context["data_interval_end"]


def check_na_or_eu_time_daily_fn(data_interval_end: pendulum.datetime):
    run_time = data_interval_end.in_timezone("America/Los_Angeles")
    if run_time.hour == 1:
        return "na_tasks_branch_out"
    elif run_time.hour == 18:
        return "eu_tasks_branch_out"
    else:
        return []


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
    check_na_or_eu_time_daily = BranchPythonOperator(
        task_id="check_na_or_eu_time_daily",
        python_callable=check_na_or_eu_time_daily_fn,
    )
    na_tasks_branch_out = EmptyOperator(task_id="na_tasks_branch_out")
    eu_tasks_branch_out = EmptyOperator(task_id="eu_tasks_branch_out")
    chain_tasks(check_na_or_eu_time_daily, [na_tasks_branch_out, eu_tasks_branch_out])
    # add NA schedule tasks as downstream for na_tasks_branch_out, do the same for EU

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

    finance_segment_mapping = SnowflakeProcedureOperator(
        procedure="reference.finance_segment_mapping.sql",
        database="edw_prod",
        warehouse="da_wh_edw",
    )
    daily_cash_base = SnowflakeProcedureOperator(
        procedure="reporting.daily_cash_base_calc.sql",
        warehouse="da_wh_edw",
        database="edw_prod",
    )
    daily_cash_final_output = SnowflakeProcedureOperator(
        procedure="reporting.daily_cash_final_output.sql",
        warehouse="da_wh_edw",
        database="edw_prod",
    )
    daily_cash_final_output_snapshot = SnowflakeProcedureOperator(
        procedure="snapshot.daily_cash_final_output.sql",
        warehouse="da_wh_edw",
        database="edw_prod",
    )
    fso_daily_cash_validation = TFGTriggerDagRunOperator(
        task_id="edw_p1_reporting_data_validation_and_alerting",
        trigger_dag_id="edw_p1_reporting_data_validation_and_alerting",
        execution_date="{{ data_interval_end }}",
    )
    tableau_daily_cash_dataset = TableauRefreshOperator(
        task_id="trigger_daily_cash_dataset_tableau_extract_refresh",
        data_source_id="8c5d1472-fcb7-4f39-894e-e915de3a95d2",
    )
    tableau_daily_cash_comparison = TableauRefreshOperator(
        task_id="trigger_daily_cash_comparison_tableau_extract_refresh",
        data_source_id="dcca9a70-79e5-4310-b761-72a0156edb3b",
    )

    na_reports = RunSqlAgent(
        task_id="na_reports",
        mssql_conn_id=conn_ids.MsSql.evolve01_app_airflow_rw,
        mssql_job_name="8FFBC175-D1A8-4809-A03E-C06E75BCDCAB",
    )
    chain_tasks(na_tasks_branch_out, na_reports)
    eu_reports = RunSqlAgent(
        task_id="eu_reports",
        mssql_conn_id=conn_ids.MsSql.evolve01_app_airflow_rw,
        mssql_job_name="0705B531-DB09-49F0-812D-50CA1EA2C511",
    )
    chain_tasks(eu_tasks_branch_out, eu_reports)
    chain_tasks(
        finance_segment_mapping,
        daily_cash_base,
        daily_cash_final_output,
        daily_cash_final_output_snapshot,
        [
            tableau_daily_cash_dataset,
            tableau_daily_cash_comparison,
            fso_daily_cash_validation,
            na_reports,
            eu_reports,
        ],
    )

    finance_kpi_base = SnowflakeProcedureOperator(
        procedure="reporting.finance_kpi_base.sql",
        database="edw_prod",
        warehouse="da_wh_edw",
    )
    finance_kpi_final_output = SnowflakeProcedureOperator(
        procedure="reporting.finance_kpi_final_output.sql",
        database="edw_prod",
        warehouse="da_wh_edw",
    )
    finance_kpi_final_output_snapshot = SnowflakeProcedureOperator(
        procedure="snapshot.finance_kpi_final_output.sql",
        database="edw_prod",
        warehouse="da_wh_edw",
    )
    retail_attribution_final_output = SnowflakeProcedureOperator(
        procedure="reporting.retail_attribution_final_output.sql",
        database="edw_prod",
        warehouse="da_wh_edw",
    )
    # Refresh "FIN013 - DDD Dataset" tableau datasource
    tableau_fin013_ddd = TableauRefreshOperator(
        task_id="trigger_tableau_fin013_ddd",
        data_source_id="0e66a1cf-2190-474e-a8a5-9b15388340d8",
    )
    chain_tasks(
        na_tasks_branch_out,
        finance_kpi_base,
        finance_kpi_final_output,
        finance_kpi_final_output_snapshot,
        tableau_fin013_ddd,
    )
    chain_tasks(monthly_tasks_day2_to_day4_branch_out, finance_kpi_base)
    chain_tasks(monthly_tasks_day2_branch_out, tableau_fin013_ddd)

    lead_to_vip_waterfall_final_output = SnowflakeProcedureOperator(
        procedure="reporting.lead_to_vip_waterfall_final_output.sql",
        database="edw_prod",
        warehouse="da_wh_edw",
    )
    lead_to_vip_waterfall_ssrs = RunSqlAgent(
        task_id="lead_to_vip_waterfall_ssrs_reports",
        mssql_conn_id=conn_ids.MsSql.evolve01_app_airflow_rw,
        mssql_job_name="BFE773EE-E2CE-499A-B5AF-229EC43F2F1C",
    )
    tableau_fin015_lead_to_vip_waterfall = TableauRefreshOperator(
        task_id="trigger_tableau_fin015_lead_to_vip_waterfall",
        data_source_id="15820037-46d5-4a46-ab23-27f96c62452b",
    )
    chain_tasks(
        na_tasks_branch_out,
        lead_to_vip_waterfall_final_output,
        lead_to_vip_waterfall_ssrs,
        tableau_fin015_lead_to_vip_waterfall,
    )
    chain_tasks(
        finance_segment_mapping,
        monthly_tasks_day2_to_day4_branch_out,
        lead_to_vip_waterfall_final_output,
    )
    chain_tasks(monthly_tasks_day2_branch_out, lead_to_vip_waterfall_ssrs)

    trigger_ltv = TFGTriggerDagRunOperator(
        task_id="trigger_ltv",
        trigger_dag_id="edm_analytics_base_customer_lifetime_value_monthly",
        execution_date="{{ data_interval_end }}",
    )

    trigger_fl_and_sxf_general_catalyst = TFGTriggerDagRunOperator(
        task_id="trigger_fl_and_sxf_general_catalyst",
        trigger_dag_id="edm_reporting_fl_and_sxf_general_catalyst",
        execution_date="{{ data_interval_end }}",
    )

    chain_tasks(na_tasks_branch_out, [trigger_ltv, trigger_fl_and_sxf_general_catalyst])

    chain_tasks(monthly_tasks_day2_branch_out, retail_attribution_final_output)
