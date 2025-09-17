from datetime import timedelta

import pendulum
from airflow.models import DAG
from airflow.operators.python import BranchPythonOperator

from include.airflow.callbacks.slack import slack_failure_gsc
from include.airflow.dag_helpers import chain_tasks
from include.airflow.operators.dagrun_operator import TFGTriggerDagRunOperator
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.config import email_lists, owners

default_args = {
    "depends_on_past": False,
    "start_date": pendulum.datetime(2019, 1, 1, 7, tz="America/Los_Angeles"),
    "retries": 1,
    "owner": owners.global_apps_analytics,
    "email": email_lists.global_applications,
    "on_failure_callback": slack_failure_gsc,
    "execution_timeout": timedelta(hours=2),
}


dag = DAG(
    dag_id="global_apps_gsc_reporting_2hour_5am_9pm",
    default_args=default_args,
    schedule="0 5-21/2 * * *",
    catchup=False,
    max_active_tasks=4,
    max_active_runs=1,
    doc_md=__doc__,
)


def check_time(**context):
    execution_time = context["data_interval_end"].in_timezone("America/Los_Angeles")
    if execution_time.hour in [5, 11]:
        return [trigger_po_skus_data_export.task_id]
    return []


with dag:
    vw_inventory_log = SnowflakeProcedureOperator(
        database="reporting_base_prod",
        procedure="gsc.vw_inventory_log.sql",
    )
    gsc_lpn_detail_dataset = SnowflakeProcedureOperator(
        procedure="gsc.lpn_detail_dataset.sql",
        database="reporting_prod",
        autocommit=False,
    )
    gsc_return_detail_dataset = SnowflakeProcedureOperator(
        procedure="gsc.return_detail_dataset.sql",
        database="reporting_prod",
        autocommit=False,
    )
    gsc_rma_detail_dataset = SnowflakeProcedureOperator(
        procedure="gsc.rma_detail_dataset.sql",
        database="reporting_prod",
        autocommit=False,
    )

    gsc_order_dataset = SnowflakeProcedureOperator(
        procedure="gsc.order_dataset.sql", database="reporting_prod", autocommit=False
    )
    gsc_shipment_detail_dataset = SnowflakeProcedureOperator(
        procedure="gsc.shipment_detail_dataset.sql",
        database="reporting_prod",
        autocommit=False,
    )
    gsc_shipment_dataset = SnowflakeProcedureOperator(
        procedure="gsc.shipment_dataset.sql",
        database="reporting_prod",
        autocommit=False,
    )
    gsc_narvar_consolidated_milestones = SnowflakeProcedureOperator(
        procedure="gsc.narvar_consolidated_milestones.sql",
        database="reporting_base_prod",
        autocommit=False,
    )
    gsc_carrier_milestone_detail = SnowflakeProcedureOperator(
        procedure="gsc.carrier_milestone_detail.sql",
        database="reporting_base_prod",
        autocommit=False,
    )
    gsc_carrier_milestone_dataset = SnowflakeProcedureOperator(
        procedure="gsc.carrier_milestone_dataset.sql",
        database="reporting_base_prod",
        autocommit=False,
    )
    gsc_carrier_performance = SnowflakeProcedureOperator(
        procedure="gsc.carrier_performance_dataset.sql",
        database="reporting_prod",
        autocommit=False,
    )
    gsc_po_audit_trail = SnowflakeProcedureOperator(
        procedure="gsc.po_audit_trail_dataset.sql",
        database="reporting_prod",
        autocommit=False,
    )

    gsc_pulse_comment_processing = SnowflakeProcedureOperator(
        procedure="gsc.pulse_comment_processing.sql",
        database="reporting_base_prod",
        autocommit=False,
    )

    trigger_po_skus_data_export = TFGTriggerDagRunOperator(
        task_id="trigger_po_skus_data_export",
        trigger_dag_id="global_apps_gsc_reporting_2hour_po_skus_data_export",
    )

    chain_tasks(
        gsc_carrier_milestone_detail,
        gsc_carrier_milestone_dataset,
        gsc_shipment_detail_dataset,
        [gsc_shipment_dataset, gsc_narvar_consolidated_milestones],
        gsc_carrier_performance,
    )

    gsc_pulse_comment_processing >> gsc_rma_detail_dataset >> gsc_return_detail_dataset

    check_time_po_skus_data = BranchPythonOperator(
        task_id="check_time", python_callable=check_time
    )

    check_time_po_skus_data >> trigger_po_skus_data_export
