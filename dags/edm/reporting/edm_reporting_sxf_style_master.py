from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.sensors.time_delta import TimeDeltaSensor

from include.airflow.callbacks.slack import slack_failure_sxf_data_team
from include.airflow.operators.dagrun_operator import TFGTriggerDagRunOperator
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.airflow.operators.tableau import TableauRefreshOperator
from include.config import email_lists, owners

default_args = {
    "start_date": pendulum.datetime(2020, 1, 1, 7, tz="America/Los_Angeles"),
    "owner": owners.sxf_analytics,
    "email": email_lists.data_integration_support,
    "on_failure_callback": slack_failure_sxf_data_team,
}


def get_next_execution_date(execution_date, context):
    return context["data_interval_end"]


dag = DAG(
    dag_id="edm_reporting_sxf_style_master",
    default_args=default_args,
    schedule="45 5 * * *",
    catchup=False,
    max_active_tasks=20,
    max_active_runs=1,
)


with dag:
    check_edw_dag_run = ExternalTaskSensor(
        task_id="check_edm_edw_load_completion",
        external_dag_id="edm_edw_load",
        execution_delta=timedelta(minutes=-1170),
        check_existence=True,
        timeout=3600,
        poke_interval=60 * 10,
        mode="reschedule",
    )
    check_order_line_dataset_run = ExternalTaskSensor(
        task_id="check_edm_reporting_sxf_processing_completion",
        external_dag_id="edm_reporting_sxf_processing",
        external_task_id="sxf_load.reporting_prod.sxf.order_line_dataset.sql",
        execution_delta=timedelta(minutes=-555),
        check_existence=True,
        timeout=3600,
        poke_interval=60 * 10,
        mode="reschedule",
    )
    style_master = SnowflakeProcedureOperator(
        procedure="sxf.style_master.sql", database="reporting_prod"
    )

    await_shoppertrak_traffic_counter = ExternalTaskSensor(
        task_id="await_edm_inbound_shoppertrak_traffic_counter",
        external_dag_id="edm_inbound_shoppertrak_traffic_counter",
        mode="reschedule",
        poke_interval=60,
        execution_date_fn=get_next_execution_date,
        timeout=60 * 10,
    )

    trigger_shoppertrak_traffic_counter = TFGTriggerDagRunOperator(
        task_id="trigger_edm_inbound_shoppertrak_traffic_counter",
        trigger_dag_id="edm_inbound_shoppertrak_traffic_counter",
        execution_date="{{ data_interval_end }}",
    )

    retail_attribution = SnowflakeProcedureOperator(
        procedure="sxf.retail_attribution.sql", database="reporting_prod"
    )
    prebreak_size_scales = SnowflakeProcedureOperator(
        procedure="sxf.prebreak_size_scales.sql", database="reporting_prod"
    )
    sold_out_color_skus = SnowflakeProcedureOperator(
        procedure="sxf.sold_out_color_skus.sql", database="reporting_prod"
    )
    order_line_matchiness = SnowflakeProcedureOperator(
        procedure="sxf.order_line_matchiness.sql", database="reporting_prod"
    )
    category_performance = SnowflakeProcedureOperator(
        procedure="sxf.category_performance.sql", database="reporting_prod"
    )
    top_sellers_w_inventory = SnowflakeProcedureOperator(
        procedure="sxf.top_sellers_w_inventory.sql", database="reporting_prod"
    )
    top_sellers_tableau_refresh = TableauRefreshOperator(
        task_id="tableau_refresh_top_sellers_data_source",
        data_source_id="f824d361-3660-4ee3-bc88-7e090826d928",
    )
    category_performance_tableau_refresh = TableauRefreshOperator(
        task_id="tableau_refresh_category_performance_data_source",
        data_source_id="fd0c594a-bb08-46c8-924f-4af9b8b613a7",
    )
    await_category_performance_refresh = TimeDeltaSensor(
        task_id="await_category_performance_tableau_refresh",
        delta=timedelta(minutes=10),
        mode="reschedule",
    )

    category_performance_filtered_tableau_refresh = TableauRefreshOperator(
        task_id="tableau_refresh_category_performance_filtered_data_source",
        data_source_id="4194498c-5343-409f-91bd-331dfe6fabde",
    )

    retail_attribution_tableau_refresh = TableauRefreshOperator(
        task_id="tableau_refresh_retail_attribution_data_source",
        data_source_id="4c3c7e79-c553-4750-bfdb-a36111370fb9",
    )

    check_edw_dag_run >> [style_master, sold_out_color_skus]
    (
        check_edw_dag_run
        >> trigger_shoppertrak_traffic_counter
        >> await_shoppertrak_traffic_counter
        >> retail_attribution
        >> retail_attribution_tableau_refresh
    )
    check_order_line_dataset_run >> prebreak_size_scales
    (
        check_order_line_dataset_run
        >> style_master
        >> [order_line_matchiness, category_performance, top_sellers_w_inventory]
    )
    top_sellers_w_inventory >> top_sellers_tableau_refresh
    (
        category_performance
        >> category_performance_tableau_refresh
        >> await_category_performance_refresh
        >> category_performance_filtered_tableau_refresh
    )
