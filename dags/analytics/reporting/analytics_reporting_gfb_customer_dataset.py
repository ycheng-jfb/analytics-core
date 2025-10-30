import pendulum
from airflow.models import DAG

from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.airflow.operators.tableau import TableauRefreshOperator
from include.config import owners
from include.config.email_lists import analytics_support

default_args = {
    'depends_on_past': False,
    'start_date': pendulum.datetime(2019, 1, 1, 7, tz='America/Los_Angeles'),
    'retries': 1,
    'owner': owners.jfb_analytics,
    'email': analytics_support,
}


dag = DAG(
    dag_id="analytics_reporting_gfb_customer_dataset",
    default_args=default_args,
    schedule="40 4 1 * *",
    max_active_tasks=15,
    catchup=False,
)


with dag:
    gfb_customer_dataset_base = SnowflakeProcedureOperator(
        procedure='gfb.gfb_customer_dataset_base.sql',
        database='reporting_prod',
        autocommit=False,
    )

    gfb_customer_dataset_cycle_action = SnowflakeProcedureOperator(
        procedure='gfb.gfb_customer_dataset_cycle_action.sql',
        database='reporting_prod',
        autocommit=False,
    )

    gfb_customer_dataset_email_action = SnowflakeProcedureOperator(
        procedure='gfb.gfb_customer_dataset_email_action.sql',
        database='reporting_prod',
        autocommit=False,
    )

    gfb_customer_dataset_gms_actions = SnowflakeProcedureOperator(
        procedure='gfb.gfb_customer_dataset_gms_actions.sql',
        database='reporting_prod',
        autocommit=False,
    )

    gfb_customer_dataset_last_order = SnowflakeProcedureOperator(
        procedure='gfb.gfb_customer_dataset_last_order.sql',
        database='reporting_prod',
        autocommit=False,
    )

    gfb_customer_dataset_loyalty_action = SnowflakeProcedureOperator(
        procedure='gfb.gfb_customer_dataset_loyalty_action.sql',
        database='reporting_prod',
        autocommit=False,
    )

    gfb_customer_dataset_misc_action = SnowflakeProcedureOperator(
        procedure='gfb.gfb_customer_dataset_misc_action.sql',
        database='reporting_prod',
        autocommit=False,
    )

    gfb_customer_dataset_preorder_action = SnowflakeProcedureOperator(
        procedure='gfb.gfb_customer_dataset_preorder_action.sql',
        database='reporting_prod',
        autocommit=False,
    )

    gfb_customer_dataset_product_purchases = SnowflakeProcedureOperator(
        procedure='gfb.gfb_customer_dataset_product_purchases.sql',
        database='reporting_prod',
        autocommit=False,
    )

    gfb_customer_dataset_product_review = SnowflakeProcedureOperator(
        procedure='gfb.gfb_customer_dataset_product_review.sql',
        database='reporting_prod',
        autocommit=False,
    )

    gfb_customer_dataset_promo_usage = SnowflakeProcedureOperator(
        procedure='gfb.gfb_customer_dataset_promo_usage.sql',
        database='reporting_prod',
        autocommit=False,
    )

    gfb_customer_dataset_psources = SnowflakeProcedureOperator(
        procedure='gfb.gfb_customer_dataset_psources.sql',
        database='reporting_prod',
        autocommit=False,
    )

    gfb_customer_dataset_final = SnowflakeProcedureOperator(
        procedure='gfb.gfb_customer_dataset_final.sql',
        database='reporting_prod',
        autocommit=False,
    )

    # layer 1
    (
        gfb_customer_dataset_base
        >> [
            gfb_customer_dataset_cycle_action,
            gfb_customer_dataset_email_action,
            gfb_customer_dataset_gms_actions,
            gfb_customer_dataset_last_order,
            gfb_customer_dataset_loyalty_action,
            gfb_customer_dataset_misc_action,
            gfb_customer_dataset_preorder_action,
            gfb_customer_dataset_product_purchases,
            gfb_customer_dataset_product_review,
            gfb_customer_dataset_promo_usage,
            gfb_customer_dataset_psources,
        ]
        >> gfb_customer_dataset_final
    )
