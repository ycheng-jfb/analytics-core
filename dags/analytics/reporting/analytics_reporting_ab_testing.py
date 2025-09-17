from datetime import timedelta

import pendulum
from airflow.models import DAG
from airflow.operators.python import BranchPythonOperator
from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.dag_helpers import chain_tasks
from include.airflow.operators.dagrun_operator import TFGTriggerDagRunOperator
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.config import owners, s3_buckets, stages
from include.config.email_lists import analytics_support

default_args = {
    'start_date': pendulum.datetime(2019, 1, 1, 7, tz='America/Los_Angeles'),
    'retries': 0,
    'owner': owners.wads,
    'email': analytics_support,
    'on_failure_callback': slack_failure_edm,
}

database = "reporting_prod"
schema = "shared"
table = "ab_test_metadata_import_oof"
stage_name = stages.tsos_da_int_inbound
bucket_name = s3_buckets.tsos_da_int_inbound
date_param = "{{macros.datetime.now().strftime('%Y%m%d')}}"
s3_prefix = f"lake/{schema}.{table}/{schema}.{table}_{date_param}.tsv.gz"

dag = DAG(
    dag_id="analytics_reporting_ab_testing",
    default_args=default_args,
    schedule=None,
    catchup=False,
    max_active_tasks=10,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=4),
)


def get_next_execution_date(execution_date, context):
    return context['data_interval_end']


def check_execution_time(**kwargs):
    execution_time = kwargs['data_interval_end'].in_timezone('America/Los_Angeles')
    if execution_time.hour == 5:
        return s20_session_ab_test_vip_ltv.task_id
    else:
        return []


with dag:
    s1_ab_test_cms_metadata = SnowflakeProcedureOperator(
        procedure='shared.ab_test_cms_metadata.sql',
        database='reporting_prod',
    )  # 1

    s1_ab_test_cms_metadata_filter = SnowflakeProcedureOperator(
        procedure='shared.ab_test_cms_metadata_filtered.sql',
        database='reporting_prod',
    )  # filter on #1 DA-22858

    s2_ab_test_cms_groups = SnowflakeProcedureOperator(
        procedure='shared.ab_test_cms_groups.sql',
        database='reporting_prod',
    )  # 2

    s3_ab_test_cms_metadata_scaffold = SnowflakeProcedureOperator(
        procedure='shared.ab_test_cms_metadata_scaffold.sql',
        database='reporting_prod',
    )  # 3

    s3_ab_test_cms_metadata_drafts = SnowflakeProcedureOperator(
        procedure='shared.ab_test_cms_metadata_drafts.sql',
        database='reporting_prod',
    )  # using ab_test_cms_metadata_scaffold as source

    s4_session_ab_test_cms_framework = SnowflakeProcedureOperator(
        procedure='shared.session_ab_test_cms_framework.sql',
        database='reporting_base_prod',
    )  # 4

    s5_session_ab_test_non_framework = SnowflakeProcedureOperator(
        procedure='shared.session_ab_test_non_framework.sql',
        database='reporting_base_prod',
    )  # 5

    s6_session_ab_test_sorting_hat = SnowflakeProcedureOperator(
        procedure='shared.session_ab_test_sorting_hat.sql',
        database='reporting_base_prod',
    )  # 6

    s7_session_ab_test_metadata = SnowflakeProcedureOperator(
        procedure='shared.session_ab_test_metadata.sql',
        database='reporting_base_prod',
    )  # 7

    s8_session_ab_test_flags = SnowflakeProcedureOperator(
        procedure='shared.session_ab_test_flags.sql',
        database='reporting_base_prod',
    )  # 8

    s9_session_ab_test_membership_cancels = SnowflakeProcedureOperator(
        procedure='shared.session_ab_test_membership_cancels.sql',
        database='reporting_base_prod',
    )  # 9

    s10_session_ab_test_orders = SnowflakeProcedureOperator(
        procedure='shared.session_ab_test_orders.sql',
        database='reporting_base_prod',
    )  # 10

    s11_session_order_ab_test_detail = SnowflakeProcedureOperator(
        procedure='shared.session_order_ab_test_detail.sql',
        database='reporting_base_prod',
    )  # 11

    s11_session_order_ab_test_detail_optimized = SnowflakeProcedureOperator(
        procedure='shared.session_order_ab_test_detail_optimized.sql',
        database='reporting_base_prod',
    )  # 11

    s12_ab_test_final = SnowflakeProcedureOperator(
        procedure='shared.ab_test_final.sql',
        database='reporting_prod',
    )  # 12

    s13_ab_test_metadata_final = SnowflakeProcedureOperator(
        procedure='shared.ab_test_metadata_final.sql',
        database='reporting_prod',
    )  # 13

    s14_payment_shipping_type_ab_test_final = SnowflakeProcedureOperator(
        procedure='shared.payment_shipping_type_ab_test_final.sql',
        database='reporting_prod',
    )  # 14

    s15_psource_promo_revenue_ab_test_final = SnowflakeProcedureOperator(
        procedure='shared.psource_promo_revenue_ab_test_final.sql',
        database='reporting_prod',
    )  # 15

    s16_session_ab_test_builder = SnowflakeProcedureOperator(
        procedure='shared.session_ab_test_builder.sql',
        database='reporting_base_prod',
    )  # 16

    s19_session_ab_test_orders_psource = SnowflakeProcedureOperator(
        procedure='shared.session_ab_test_orders_psource.sql',
        database='reporting_base_prod',
    )  # 19
    check_s20_session_ab_test_vip_ltv = BranchPythonOperator(
        python_callable=check_execution_time, task_id='check_session_ab_test_vip_ltv_execution_time'
    )
    s20_session_ab_test_vip_ltv = SnowflakeProcedureOperator(
        procedure='shared.ab_test_vip_ltv.sql',
        database='reporting_prod',
    )  # 20
    s21_session_ab_test_builder_grid_psources = SnowflakeProcedureOperator(
        procedure='shared.ab_test_builder_grid_psources.sql',
        database='reporting_prod',
    )  # 21
    s22_session_abt_session_ab_test_builder_grid_clicks = SnowflakeProcedureOperator(
        procedure='shared.abt_session_ab_test_builder_grid_clicks.sql',
        database='reporting_base_prod',
    )  # 22
    # s23_session_ab_test_funnel = SnowflakeProcedureOperator(
    #     procedure='shared.session_ab_test_funnel.sql',
    #     database='reporting_base_prod',
    # )  # 23
    s24_ab_test_funnel = SnowflakeProcedureOperator(
        procedure='shared.ab_test_funnel.sql',
        database='reporting_prod',
    )  # 24

    chain_tasks(
        s1_ab_test_cms_metadata,
        s3_ab_test_cms_metadata_scaffold,
        s3_ab_test_cms_metadata_drafts,
        s4_session_ab_test_cms_framework,
        s7_session_ab_test_metadata,
        s21_session_ab_test_builder_grid_psources,
        s22_session_abt_session_ab_test_builder_grid_clicks,
        [
            s8_session_ab_test_flags,
            s9_session_ab_test_membership_cancels,
            s10_session_ab_test_orders,
        ],
    )

    chain_tasks(
        [s5_session_ab_test_non_framework, s6_session_ab_test_sorting_hat],
        s16_session_ab_test_builder,
        s7_session_ab_test_metadata,
    )
    chain_tasks(
        s1_ab_test_cms_metadata,
        s1_ab_test_cms_metadata_filter,
        [
            s2_ab_test_cms_groups,
            s7_session_ab_test_metadata,
            s8_session_ab_test_flags,
            s9_session_ab_test_membership_cancels,
            s10_session_ab_test_orders,
        ],
        s19_session_ab_test_orders_psource,
        s11_session_order_ab_test_detail,
        [
            s12_ab_test_final,
            s14_payment_shipping_type_ab_test_final,
            s15_psource_promo_revenue_ab_test_final,
            # s23_session_ab_test_funnel,
            s24_ab_test_funnel,
        ],
    )

    chain_tasks([s12_ab_test_final, s7_session_ab_test_metadata], s13_ab_test_metadata_final)
    chain_tasks(s19_session_ab_test_orders_psource, s11_session_order_ab_test_detail_optimized)
    chain_tasks(
        s15_psource_promo_revenue_ab_test_final,
        check_s20_session_ab_test_vip_ltv,
        s20_session_ab_test_vip_ltv,
    )
    trigger_data_science_image_testing = TFGTriggerDagRunOperator(
        task_id=f"trigger_data_science_image_testing",
        trigger_dag_id="data_science_image_testing",
        execution_date='{{ data_interval_end }}',
    )
    chain_tasks(s15_psource_promo_revenue_ab_test_final, trigger_data_science_image_testing)
