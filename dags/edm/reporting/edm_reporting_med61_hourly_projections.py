from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.utils.task_group import TaskGroup

from include.airflow.callbacks.slack import slack_failure_p1, slack_sla_miss_edm_p1
from include.airflow.operators.tableau import TableauRefreshOperator
from include.config import email_lists, owners, conn_ids

default_args = {
    "start_date": pendulum.datetime(2022, 4, 1, 7, tz="America/Los_Angeles"),
    "owner": owners.data_integrations,
    "email": email_lists.data_integration_support,
    "on_failure_callback": slack_failure_p1,
    "sla": timedelta(minutes=40),
}


def get_task_id(**kwargs):
    execution_date = kwargs["data_interval_end"].in_timezone("America/Los_Angeles")
    if 8 <= execution_date.hour <= 14 or execution_date.hour == 23:
        return [dummy_na.task_id, dummy_eu.task_id]
    if 15 <= execution_date.hour <= 22:
        return dummy_na.task_id
    elif 0 <= execution_date.hour <= 7:
        return dummy_eu.task_id
    else:
        return []


dag = DAG(
    dag_id="edm_reporting_med61_hourly_projections",
    default_args=default_args,
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=100,
    dagrun_timeout=timedelta(minutes=45),
    sla_miss_callback=slack_sla_miss_edm_p1,
)
with dag:
    dummy_na = EmptyOperator(task_id="dummy_na")
    dummy_eu = EmptyOperator(task_id="dummy_eu")

    check = BranchPythonOperator(
        task_id="check",
        python_callable=get_task_id,
    )
    med61_hourly_projections_fabletics_na = DatabricksRunNowOperator(
        task_id="med61_hourly_projections_fabletics_na",
        databricks_conn_id=conn_ids.Databricks.duplo,
        job_id="858203777852780",
        json={
            "notebook_params": {
                "store_brand_name_param": "Fabletics",
                "region_param": "NA",
            }
        },
    )

    med61_tableau_refresh_projections = TableauRefreshOperator(
        data_source_name="Real Time Hourly Projections",
        task_id="med61-tableau-refresh-projections",
        trigger_rule="none_failed_or_skipped",
    )

    with TaskGroup(group_id="region_na") as region_na:
        dummy_na >> [
            DatabricksRunNowOperator(
                task_id="med61_hourly_projections_savage_na",
                databricks_conn_id=conn_ids.Databricks.duplo,
                job_id="858203777852780",
                json={
                    "notebook_params": {
                        "store_brand_name_param": "Savage X",
                        "region_param": "NA",
                    }
                },
            ),
            DatabricksRunNowOperator(
                task_id="med61_hourly_projections_flm_na",
                databricks_conn_id=conn_ids.Databricks.duplo,
                job_id="858203777852780",
                json={
                    "notebook_params": {
                        "store_brand_name_param": "Fabletics Men",
                        "region_param": "NA",
                    }
                },
            ),
            DatabricksRunNowOperator(
                task_id="med61_hourly_projections_scb_na",
                databricks_conn_id=conn_ids.Databricks.duplo,
                job_id="858203777852780",
                json={
                    "notebook_params": {
                        "store_brand_name_param": "Fabletics Scrubs",
                        "region_param": "NA",
                    }
                },
            ),
            DatabricksRunNowOperator(
                task_id="med61_hourly_projections_yitty_na",
                databricks_conn_id=conn_ids.Databricks.duplo,
                job_id="858203777852780",
                json={
                    "notebook_params": {
                        "store_brand_name_param": "Yitty",
                        "region_param": "NA",
                    }
                },
            ),
            DatabricksRunNowOperator(
                task_id="med61_hourly_projections_justfab_na",
                databricks_conn_id=conn_ids.Databricks.duplo,
                job_id="858203777852780",
                json={
                    "notebook_params": {
                        "store_brand_name_param": "JustFab",
                        "region_param": "NA",
                    }
                },
            ),
            DatabricksRunNowOperator(
                task_id="med61_hourly_projections_shoedazzle_na",
                databricks_conn_id=conn_ids.Databricks.duplo,
                job_id="858203777852780",
                json={
                    "notebook_params": {
                        "store_brand_name_param": "ShoeDazzle",
                        "region_param": "NA",
                    }
                },
            ),
            DatabricksRunNowOperator(
                task_id="med61_hourly_projections_fabkids_na",
                databricks_conn_id=conn_ids.Databricks.duplo,
                job_id="858203777852780",
                json={
                    "notebook_params": {
                        "store_brand_name_param": "FabKids",
                        "region_param": "NA",
                    }
                },
            ),
        ]
    with TaskGroup(group_id="region_EU") as region_eu:
        dummy_eu >> [
            DatabricksRunNowOperator(
                task_id="med61_hourly_projections_fabletics_eu",
                databricks_conn_id=conn_ids.Databricks.duplo,
                job_id="858203777852780",
                json={
                    "notebook_params": {
                        "store_brand_name_param": "Fabletics",
                        "region_param": "EU",
                    }
                },
            ),
            DatabricksRunNowOperator(
                task_id="med61_hourly_projections_savage_eu",
                databricks_conn_id=conn_ids.Databricks.duplo,
                job_id="858203777852780",
                json={
                    "notebook_params": {
                        "store_brand_name_param": "Savage X",
                        "region_param": "EU",
                    }
                },
            ),
            DatabricksRunNowOperator(
                task_id="med61_hourly_projections_flm_eu",
                databricks_conn_id=conn_ids.Databricks.duplo,
                job_id="858203777852780",
                json={
                    "notebook_params": {
                        "store_brand_name_param": "Fabletics Men",
                        "region_param": "EU",
                    }
                },
            ),
            DatabricksRunNowOperator(
                task_id="med61_hourly_projections_justfab_eu",
                databricks_conn_id=conn_ids.Databricks.duplo,
                job_id="858203777852780",
                json={
                    "notebook_params": {
                        "store_brand_name_param": "JustFab",
                        "region_param": "EU",
                    }
                },
            ),
        ]

    (med61_hourly_projections_fabletics_na >> check >> [dummy_na, dummy_eu])
    [region_eu, region_na] >> med61_tableau_refresh_projections
