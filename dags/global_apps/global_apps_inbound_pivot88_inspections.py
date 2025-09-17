import pendulum
from airflow.models import DAG
from datetime import timedelta
from include.airflow.callbacks.slack import slack_failure_gsc
from include.airflow.operators.pivot88 import (
    Pivot88InspectionsOperator,
    Pivot88AdvancedDetails,
)
from include.airflow.operators.snowflake_load import SnowflakeIncrementalLoadOperator
from include.config import (
    conn_ids,
    email_lists,
    owners,
    s3_buckets,
    snowflake_roles,
    stages,
)
from include.utils.snowflake import CopyConfigJson, CopyConfigCsv
from task_configs.dag_config.pivot88 import (
    column_list_inspections,
    column_list_adv_details,
)

default_args = {
    "start_date": pendulum.datetime(2020, 12, 11, tz="America/Los_Angeles"),
    "retries": 0,
    "owner": owners.data_integrations,
    "email": email_lists.global_applications,
    "on_failure_callback": slack_failure_gsc,
    "execution_timeout": timedelta(hours=1),
}

dag = DAG(
    dag_id="global_apps_inbound_pivot88_inspections",
    default_args=default_args,
    schedule="20 5 * * *",
    catchup=False,
)

database = "lake"
schema = "pivot88"
yr_mth = "{{macros.datetime.now().strftime('%Y%m')}}"

with dag:
    table_in = "inspections"
    s3_prefix = f"lake/{database}.{schema}.{table_in}/v2"
    to_s3_inspections = Pivot88InspectionsOperator(
        task_id="to_s3_inspections",
        bucket=s3_buckets.tsos_da_int_inbound,
        key=f"{s3_prefix}/{yr_mth}/{schema}_{table_in}_{{{{ ts_nodash }}}}.json.gz",
        process_name=f"{database}.{schema}.{table_in}",
        namespace="pivot88_inspections",
        s3_conn_id=conn_ids.S3.tsos_da_int_prod,
    )

    to_snowflake_inspections = SnowflakeIncrementalLoadOperator(
        task_id="to_snowflake",
        database=database,
        schema=schema,
        table=table_in,
        staging_database="lake_stg",
        snowflake_conn_id=conn_ids.Snowflake.default,
        role=snowflake_roles.etl_service_account,
        column_list=column_list_inspections,
        files_path=f"{stages.tsos_da_int_inbound}/{s3_prefix}/",
        copy_config=CopyConfigJson(),
    )

    table_adv = "advanced_details"
    s3_prefix = f"lake/{database}.{schema}.{table_adv}/v1"
    to_s3_adv_details = Pivot88AdvancedDetails(
        task_id="to_s3_adv_details",
        bucket=s3_buckets.tsos_da_int_inbound,
        key=f"{s3_prefix}/{yr_mth}/{schema}_{table_adv}_{{{{ ts_nodash }}}}.csv.gz",
        process_name=f"{database}.{schema}.{table_adv}",
        namespace="pivot88_advanced_details",
        s3_conn_id=conn_ids.S3.tsos_da_int_prod,
        column_list=[x.source_name for x in column_list_adv_details],
        write_header=True,
    )

    to_snowflake_adv_details = SnowflakeIncrementalLoadOperator(
        task_id="to_snowflake_adv_details",
        database=database,
        schema=schema,
        table=table_adv,
        staging_database="lake_stg",
        snowflake_conn_id=conn_ids.Snowflake.default,
        role=snowflake_roles.etl_service_account,
        column_list=column_list_adv_details,
        files_path=f"{stages.tsos_da_int_inbound}/{s3_prefix}/",
        copy_config=CopyConfigCsv(field_delimiter="\t", header_rows=1),
    )

    (
        to_s3_inspections
        >> to_snowflake_inspections
        >> to_s3_adv_details
        >> to_snowflake_adv_details
    )
