import warnings
from pathlib import Path
from typing import Any, Dict

import pendulum
from airflow import DAG

from edm.mart.configs import get_mart_operator
from include import DAGS_DIR
from include.airflow.callbacks.slack import slack_failure_p1_edw
from include.config import owners
from include.config.email_lists import edw_support
from include.airflow.operators.dagrun_operator import TFGTriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor

default_args = {
    "start_date": pendulum.datetime(2020, 4, 19, tz="America/Los_Angeles"),
    "retries": 3,
    "owner": owners.analytics_engineering,
    "email": edw_support,
    "on_failure_callback": slack_failure_p1_edw,
}

dag = DAG(
    dag_id="edm_edw_load",
    default_args=default_args,
    schedule=None,
    catchup=False,
    max_active_tasks=18,
    max_active_runs=1,
)

mart_operators: Dict[str, Any] = {}


def get_next_execution_date(execution_date, context):
    return context["data_interval_end"]


def load_operator(table_name):
    if table_name not in mart_operators:
        mart_operators[table_name] = get_mart_operator(table_name)
    return mart_operators[table_name]


def get_all_table_names():
    table_names = []
    configs_dir = Path(DAGS_DIR, "edm", "mart", "configs")
    for file in configs_dir.rglob("*.py"):
        if "__init__.py" in file.name:
            continue
        partial_file_name = file.as_posix().replace(configs_dir.as_posix(), "")
        table_name = partial_file_name[1:-3].replace("/", ".")
        table_names.append(table_name)
    return table_names


no_dependencies = [
    # 'edw_prod.stg.dim_return_reason',
    # 'edw_prod.stg.dim_promo_history',
    # 'edw_prod.stg.fact_inventory_in_transit',
    # 'edw_prod.stg.fact_inventory_in_transit_history',
    # 'edw_prod.stg.fact_ebs_bulk_shipment',
    # 'edw_prod.stg.dim_universal_product',
    # 'edw_prod.stg.dim_address',
    "edw_prod.stg.dim_discount",
]

table_config = {
    # 'edw_prod.reference.cogs_assumptions': ['edw_prod.stg.dim_store'],
    # 'edw_prod.reference.landed_cost_by_sku': ['edw_prod.stg.dim_store'],
    # 'edw_prod.reference.na_outbound_shipping_cost': [
    #     'edw_prod.reference.store_currency',
    #     'edw_prod.stg.dim_store',
    #     'edw_prod.stg.fact_order',
    # ],
    # 'edw_prod.stg.dim_credit': ['edw_prod.reference.test_customer', 'edw_prod.stg.dim_store'],
    # 'edw_prod.stg.dim_customer_detail_history': [
    #     'edw_prod.reference.test_customer',
    #     'edw_prod.stg.fact_membership_event',
    #     'edw_prod.reference.iterable_subscription_log',
    # ],
    # 'edw_prod.stg.dim_customer': [
    #     'edw_prod.reference.test_customer',
    #     'edw_prod.reference.store_timezone',
    #     'edw_prod.stg.dim_customer_detail_history',
    #     'edw_prod.stg.dim_address',
    #     'edw_prod.stg.dim_store',
    #     'edw_prod.stg.lkp_order_classification',
    #     'edw_prod.stg.lkp_membership_state',
    # ],
    # 'edw_prod.stg.dim_item_price': ['edw_prod.stg.dim_product_price_history'],
    # 'edw_prod.stg.dim_product': ['edw_prod.stg.dim_sku'],
    # 'edw_prod.stg.lkp_membership_cancel_method': ['edw_prod.stg.fact_membership_event'],
    # 'edw_prod.stg.fact_activation': [
    #     'edw_prod.reference.test_customer',
    #     'edw_prod.stg.fact_membership_event',
    #     'edw_prod.stg.lkp_membership_cancel_method',
    # ],
    # 'edw_prod.stg.fact_activation_loyalty_tier': [
    #     'edw_prod.stg.fact_membership_event',
    #     'edw_prod.stg.fact_activation',
    # ],
    # 'edw_prod.stg.fact_chargeback': [
    #     'edw_prod.stg.dim_store',
    #     'edw_prod.stg.fact_activation',
    #     'edw_prod.stg.fact_order',
    #     'edw_prod.reference.vat_rate_history',
    #     'edw_prod.reference.store_timezone',
    #     'edw_prod.reference.store_currency',
    #     'edw_prod.stg.dim_chargeback_status',
    #     'edw_prod.reference.currency_exchange_rate',
    #     'edw_prod.reference.test_customer',
    #     'edw_prod.stg.dim_chargeback_reason',
    #     'edw_prod.stg.dim_chargeback_payment',
    # ],
    # 'edw_prod.stg.fact_customer_action': [
    #     'edw_prod.reference.store_timezone',
    #     'edw_prod.stg.fact_membership_event',
    # ],
    # 'edw_prod.stg.fact_membership_event': [
    #     'edw_prod.stg.dim_store',
    #     'edw_prod.stg.lkp_membership_state',
    #     'edw_prod.stg.lkp_order_classification',
    # ],
    # 'edw_prod.stg.fact_order': [
    #     'edw_prod.reference.currency_exchange_rate',
    #     'edw_prod.reference.store_currency',
    #     'edw_prod.reference.vat_rate_history',
    #     'edw_prod.reference.test_customer',
    #     'edw_prod.stg.dim_customer',
    #     'edw_prod.stg.dim_order_payment_status',
    #     'edw_prod.stg.dim_order_processing_status',
    #     'edw_prod.stg.dim_order_product_source',
    #     'edw_prod.stg.dim_payment',
    #     'edw_prod.stg.dim_store',
    #     'edw_prod.stg.fact_activation',
    #     'edw_prod.stg.fact_membership_event',
    #     'edw_prod.stg.fact_registration',
    #     'edw_prod.stg.lkp_order_classification',
    #     'edw_prod.stg.dim_order_customer_selected_shipping',
    # ],
    # 'edw_prod.stg.fact_order_line': [
    #     'edw_prod.reference.cogs_assumptions',
    #     'edw_prod.reference.currency_exchange_rate',
    #     'edw_prod.reference.landed_cost_by_sku',
    #     'edw_prod.reference.na_outbound_shipping_cost',
    #     'edw_prod.reference.po_sku_cost',
    #     'edw_prod.reference.store_currency',
    #     'edw_prod.stg.dim_bundle_component_history',
    #     'edw_prod.stg.dim_item_price',
    #     'edw_prod.stg.dim_order_line_status',
    #     'edw_prod.stg.dim_order_product_source',
    #     'edw_prod.stg.dim_product',
    #     'edw_prod.stg.dim_product_price_history',
    #     'edw_prod.stg.dim_product_type',
    #     'edw_prod.stg.dim_related_product',
    #     'edw_prod.stg.dim_store',
    #     'edw_prod.stg.fact_order',
    #     'edw_prod.reference.gfc_po_sku_line_lpn_mapping',
    #     'edw_prod.reference.miracle_miles_landed_cost',
    # ],
    # 'edw_prod.reference.fact_order_line_optimized': [
    #     'edw_prod.stg.fact_order_line',
    #     'edw_prod.stg.fact_order_line_product_cost',
    # ],
    # 'edw_prod.stg.fact_order_line_embroidery': [
    #     'edw_prod.stg.fact_order_line',
    #     'edw_prod.stg.fact_order_line_product_cost',
    # ],
    # 'edw_prod.stg.fact_order_discount': [
    #     'edw_prod.stg.fact_order',
    #     'edw_prod.stg.dim_discount',
    #     'edw_prod.stg.dim_discount_type',
    # ],
    # 'edw_prod.stg.fact_order_line_discount': [
    #     'edw_prod.stg.fact_order_line',
    #     'edw_prod.stg.dim_discount',
    # ],
    # 'edw_prod.stg.fact_order_product_cost': ['edw_prod.stg.fact_order_line_product_cost'],
    # 'edw_prod.stg.fact_refund': [
    #     'edw_prod.reference.currency_exchange_rate',
    #     'edw_prod.reference.store_timezone',
    #     'edw_prod.reference.vat_rate_history',
    #     'edw_prod.stg.dim_customer',
    #     'edw_prod.stg.dim_refund_comment',
    #     'edw_prod.stg.dim_refund_payment_method',
    #     'edw_prod.stg.dim_refund_reason',
    #     'edw_prod.stg.dim_refund_status',
    #     'edw_prod.stg.dim_store',
    #     'edw_prod.stg.fact_activation',
    #     'edw_prod.stg.fact_order',
    #     'edw_prod.stg.fact_chargeback',
    # ],
    # 'edw_prod.stg.fact_refund_line': [
    #     'edw_prod.stg.fact_refund',
    #     'edw_prod.stg.fact_order_line',
    #     'edw_prod.stg.dim_refund_payment_method',
    #     'edw_prod.reference.test_customer',
    # ],
    # 'edw_prod.stg.fact_registration': [
    #     'edw_prod.stg.dim_customer',
    #     'edw_prod.stg.fact_membership_event',
    # ],
    # 'edw_prod.stg.fact_return_line': [
    #     'edw_prod.reference.currency_exchange_rate',
    #     'edw_prod.reference.store_timezone',
    #     'edw_prod.reference.vat_rate_history',
    #     'edw_prod.stg.dim_customer',
    #     'edw_prod.stg.dim_return_condition',
    #     'edw_prod.stg.dim_return_status',
    #     'edw_prod.stg.dim_store',
    #     'edw_prod.stg.fact_activation',
    #     'edw_prod.stg.fact_order',
    #     'edw_prod.stg.fact_order_line',
    # ],
    # 'edw_prod.stg.fact_credit_event': [
    #     'edw_prod.stg.dim_credit',
    #     'edw_prod.stg.dim_store',
    #     'edw_prod.stg.fact_activation',
    #     'edw_prod.stg.fact_order',
    # ],
    # 'edw_prod.stg.fact_order_credit': [
    #     'edw_prod.stg.dim_credit',
    #     'edw_prod.stg.dim_store',
    #     'edw_prod.stg.fact_order',
    # ],
    # 'edw_prod.stg.lkp_membership_event_type': ['edw_prod.stg.dim_store'],
    # 'edw_prod.stg.lkp_membership_event': ['edw_prod.stg.lkp_membership_event_type'],
    # 'edw_prod.stg.lkp_membership_state': ['edw_prod.stg.lkp_membership_event'],
    # 'edw_prod.reference.po_sku_cost': ['edw_prod.reference.gsc_po_detail_dataset'],
    # 'edw_prod.stg.lkp_order_classification': [
    #     'edw_prod.stg.lkp_membership_event',
    #     'edw_prod.reference.test_customer',
    # ],
    # 'edw_prod.reference.order_date_change': ['edw_prod.stg.fact_order', 'edw_prod.stg.fact_refund'],
    # 'edw_prod.reference.order_customer_change': [
    #     'edw_prod.stg.dim_store',
    #     'edw_prod.stg.fact_order',
    # ],
    # 'edw_prod.stg.fact_order_line_product_cost': [
    #     'edw_prod.stg.fact_order_line',
    #     'edw_prod.stg.dim_product',
    #     'edw_prod.stg.dim_lpn',
    #     'edw_prod.reference.gsc_landed_cost',
    # ],
    # 'edw_prod.reference.gsc_po_detail_dataset_history': [
    #     'edw_prod.reference.gsc_po_detail_dataset',
    # ],
    # 'edw_prod.reference.gsc_landed_cost_history': [
    #     'edw_prod.reference.gsc_landed_cost',
    # ],
}
with dag:
    for table_name, dep_list in table_config.items():
        for dep_table_name in dep_list:
            dep_op = load_operator(dep_table_name)
            target_op = load_operator(table_name)
            dep_op >> target_op

    for table_name in no_dependencies:
        if table_name in mart_operators:
            raise Exception(f"table {table_name} already configured")
        load_operator(table_name)

#     trigger_edm_edw_fact_inventory = TFGTriggerDagRunOperator(
#         task_id='trigger_fact_inventory_dag',
#         trigger_dag_id='edm_edw_load_fact_inventory',
#         execution_date='{{ data_interval_end }}',
#     )
#     await_edm_edw_fact_inventory = ExternalTaskSensor(
#         task_id='await_edm_edw_fact_inventory',
#         external_dag_id='edm_edw_load_fact_inventory',
#         mode='reschedule',
#         execution_date_fn=get_next_execution_date,
#         poke_interval=60 * 5,
#     )
#
# trigger_edm_edw_fact_inventory >> await_edm_edw_fact_inventory


def validate():
    """
    Runs some checks on dag integrity

    Raises on failure
    """
    all_table_names = get_all_table_names()
    unimported_tables = [x for x in all_table_names if x not in mart_operators]

    if unimported_tables:
        for t in unimported_tables:
            warnings.warn(f"edw table {t} unimported")
        raise Exception("unscheduled tables found")

    def dict_is_sorted(dict_):
        sorted_dict = {}
        for table in sorted(dict_.keys()):
            sorted_dict[table] = sorted(dict_[table])
        return sorted_dict == dict_

    if not dict_is_sorted(table_config):
        raise Exception("table config is not sorted")


if __name__ == "__main__":
    validate()
