import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.dag_helpers import chain_tasks
from include.airflow.operators.snowflake import SnowflakeProcedureOperator

from include.airflow.operators.tableau import TableauRefreshOperator
from include.config import email_lists, owners

default_args = {
    "start_date": pendulum.datetime(2021, 4, 26, tz="America/Los_Angeles"),
    "owner": owners.global_apps_analytics,
    "email": email_lists.edw_support,
    "on_failure_callback": slack_failure_edm,
}


dag = DAG(
    dag_id="edm_reporting_merch_planning",
    default_args=default_args,
    schedule=None,
    catchup=False,
    max_active_tasks=2,
    max_active_runs=1,
)


with dag:
    sales_returns_base = SnowflakeProcedureOperator(
        procedure="analytics_base.sales_returns_agg.sql",
        database="edw_prod",
        warehouse="DA_WH_EDW",
    )
    merch_planning_item_sku = SnowflakeProcedureOperator(
        procedure="reporting.merch_planning_item_sku.sql",
        database="edw_prod",
        warehouse="DA_WH_EDW",
    )
    merch_planning_item_product_sku = SnowflakeProcedureOperator(
        procedure="reporting.merch_planning_item_product_sku.sql",
        database="edw_prod",
        warehouse="DA_WH_EDW",
    )
    merch_planning_item_final_output = SnowflakeProcedureOperator(
        procedure="reporting.merch_planning_item_final_output.sql",
        database="edw_prod",
        warehouse="DA_WH_EDW",
    )
    merch_planning_outfit = SnowflakeProcedureOperator(
        procedure="reporting.merch_planning_outfit.sql",
        database="edw_prod",
        warehouse="DA_WH_EDW",
    )
    merch_planning_outfit_final_output = SnowflakeProcedureOperator(
        procedure="reporting.merch_planning_outfit_final_output.sql",
        database="edw_prod",
        warehouse="DA_WH_EDW",
    )
    # Current Merch Planning Datasources that use CustomSQL - to be removed later
    # merch_planning_item_tableau_refresh = TableauRefreshOperator(
    #     task_id='trigger_merch_Planning_item_tableau_extract',
    #     data_source_id='9f0c20b3-027f-4505-bd5a-5766e526b723',
    # )
    # merch_planning_outfit_tableau_refresh = TableauRefreshOperator(
    #     task_id='trigger_merch_Planning_outfit_tableau_extract',
    #     data_source_id='43204c50-a975-4ed3-8bab-f2592e643fc1',
    # )
    # NEW Merch Planning Datasources that DON'T USE CustomSQL
    merch_planning_item_tableau_new_refresh = TableauRefreshOperator(
        task_id="trigger_merch_planning_item_NEW_tableau_extract",
        data_source_id="7e124c3f-90be-48ab-b54f-83f693b4693b",
    )
    merch_planning_outfit_tableau_new_refresh = TableauRefreshOperator(
        task_id="trigger_merch_planning_outfit_NEW_tableau_extract",
        data_source_id="084215bc-8554-41ce-885f-1af1ff3e2f3c",
    )
    chain_tasks(
        sales_returns_base,
        merch_planning_item_sku,
        merch_planning_item_product_sku,
        merch_planning_item_final_output,
        # old/current - to be removed later
        # merch_planning_item_tableau_refresh,
        # new
        merch_planning_item_tableau_new_refresh,
    )
    chain_tasks(
        merch_planning_outfit,
        merch_planning_outfit_final_output,
        # old/current
        # merch_planning_outfit_tableau_refresh,
        # new
        merch_planning_outfit_tableau_new_refresh,
    )
