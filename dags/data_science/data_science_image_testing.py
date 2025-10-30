import pendulum
from airflow.models import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

from include.airflow.callbacks.slack import SlackFailureCallback
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.config import conn_ids, owners
from include.airflow.dag_helpers import chain_tasks
from include.airflow.operators.tableau import TableauRefreshOperator


default_args = {
    "start_date": pendulum.datetime(2024, 8, 1, 7, tz="America/Los_Angeles"),
    'owner': owners.data_science,
    "email": ['ddieter@TechStyle.com', 'JHuff@TechStyle.com', 'DMayer@TechStyle.com'],
    "on_failure_callback": SlackFailureCallback('slack_alert_data_science'),
}

dag = DAG(
    dag_id="data_science_image_testing",
    default_args=default_args,
    schedule=None,
    catchup=False,
    max_active_tasks=4,
    max_active_runs=1,
)

with dag:
    image_testing_sam_spi = SnowflakeProcedureOperator(
        procedure='data_science.image_testing_sam_spi.sql',
        database='reporting_prod',
    )
    image_testing_sam_testable_spi = SnowflakeProcedureOperator(
        procedure='data_science.image_testing_sam_testable_spi.sql',
        database='reporting_prod',
    )
    image_testing_image_tests_extract = SnowflakeProcedureOperator(
        procedure='data_science.image_testing_image_tests_extract.sql',
        database='reporting_prod',
    )
    image_testing_request_image_test = SnowflakeProcedureOperator(
        procedure='data_science.image_testing_request_image_test.sql',
        database='reporting_prod',
    )
    dbx_image_testing_estimation_job = DatabricksRunNowOperator(
        task_id="dbx_image_testing_estimation_job",
        job_id='1064056821833979',
        databricks_conn_id=conn_ids.Databricks.duplo,
    )
    image_tests_with_current_urls = SnowflakeProcedureOperator(
        procedure='shared.image_tests_with_current_urls.sql',
        database='reporting_base_prod',
    )
    session_ab_test_ds_image_sort = SnowflakeProcedureOperator(
        procedure='shared.session_ab_test_ds_image_sort.sql',
        database='reporting_base_prod',
    )  # 17
    ab_test_ds_image_sort = SnowflakeProcedureOperator(
        procedure='shared.ab_test_ds_image_sort.sql',
        database='reporting_prod',
    )  # 18
    session_ab_test_ds_image_test = SnowflakeProcedureOperator(
        procedure='shared.session_ab_test_ds_image_test.sql',
        database='reporting_base_prod',
    )  # 19
    ab_test_ds_image_testing = SnowflakeProcedureOperator(
        procedure='shared.ab_test_ds_image_testing.sql',
        database='reporting_prod',
    )  # 20
    ab_test_metadata_ds_image_testing = SnowflakeProcedureOperator(
        procedure='shared.ab_test_metadata_ds_image_testing.sql',
        database='reporting_prod',
    )  # 21

    ab_test_future_ds_image_test = SnowflakeProcedureOperator(
        procedure='shared.ab_test_future_ds_image_test.sql',
        database='reporting_prod',
    )  # 22

    chain_tasks(
        session_ab_test_ds_image_sort, [ab_test_ds_image_sort, dbx_image_testing_estimation_job]
    )
    chain_tasks(
        image_testing_sam_spi,
        image_testing_sam_testable_spi,
        image_testing_image_tests_extract,
        image_testing_request_image_test,  # backup scripts, needed?
        dbx_image_testing_estimation_job,
        image_tests_with_current_urls,
    )
    chain_tasks(
        session_ab_test_ds_image_test,
        ab_test_ds_image_testing,
        [ab_test_metadata_ds_image_testing, ab_test_future_ds_image_test],
    )

    trigger_tableau_image_test_refresh = TableauRefreshOperator(
        task_id='tableau_image_test_refresh',
        data_source_name='TFG020L - ABT Image Test',
        wait_till_complete=True,
    )  # 23
    trigger_tableau_image_metadata_refresh = TableauRefreshOperator(
        task_id='tableau_image_metadata_refresh',
        data_source_name='TFG020J - ABT Image Test Metadata',
        wait_till_complete=True,
    )  # 24
    trigger_tableau_image_future_test = TableauRefreshOperator(
        task_id='tableau_image_future_test_refresh',
        data_source_name='TFG020K - ABT Image Test Future Tests',
        wait_till_complete=True,
    )  # 25

    ab_test_ds_image_testing >> trigger_tableau_image_test_refresh
    ab_test_metadata_ds_image_testing >> trigger_tableau_image_metadata_refresh
    ab_test_future_ds_image_test >> trigger_tableau_image_future_test
