import pendulum
from airflow.models import DAG
from airflow.operators.python import BranchPythonOperator
from include.airflow.callbacks.slack import SlackFailureCallback
from include.airflow.operators.snowflake import SnowflakeProcedureOperator, TableDependencyTzLtz
from include.airflow.operators.snowflake_load import (
    SnowflakeIncrementalLoadOperator,
    SnowflakeTruncateAndLoadOperator,
)
from include.airflow.operators.stella import StellaToS3Operator
from include.config import conn_ids, email_lists, owners, s3_buckets, stages
from include.utils.snowflake import Column, CopyConfigJson

column_list = [
    Column('uuid', 'STRING', uniqueness=True),
    Column('sequence_id', 'BIGINT'),
    Column('branding', 'STRING'),
    Column('channel', 'STRING'),
    Column('ext_interaction_id', 'STRING'),
    Column('external_url', 'STRING'),
    Column('language', 'STRING'),
    Column('survey_id', 'BIGINT'),
    Column('survey_name', 'STRING'),
    Column('tags', 'ARRAY'),
    Column('request_created_at', 'TIMESTAMP_LTZ(9)'),
    Column('request_delivery_status', 'STRING'),
    Column('request_sent_at', 'TIMESTAMP_LTZ(9)'),
    Column('requested_via', 'STRING'),
    Column('response_received_at', 'TIMESTAMP_LTZ(9)'),
    Column('reward_eligible', 'BOOLEAN'),
    Column('reward_name', 'STRING'),
    Column('marketing', 'OBJECT'),
    Column('employee', 'OBJECT'),
    Column('team_leader', 'OBJECT'),
    Column('customer', 'OBJECT'),
    Column('custom_properties', 'OBJECT'),
    Column('answers', 'OBJECT'),
    Column('disputed', 'BOOLEAN', default_value=False),
]

default_args = {
    'start_date': pendulum.datetime(2020, 10, 12, tz="America/Los_Angeles"),
    'retries': 3,
    'owner': owners.data_integrations,
    'email': email_lists.data_integration_support,
    'on_failure_callback': SlackFailureCallback('slack_alert_gsc'),
}

dag = DAG(
    dag_id="global_apps_inbound_stella_feedback",
    default_args=default_args,
    schedule="0 4 * * *",
    catchup=False,
    max_active_tasks=100,
    max_active_runs=1,
)

s3_prefix = 'lake/lake.stella.feedback/reporting_prod/v3'
s3_prefix_dispute_backfill = 'lake/lake.stella.feedback/backfill_dispute/v3'

with dag:
    stella_to_s3 = StellaToS3Operator(
        task_id="stella_to_s3",
        from_date="{{(macros.datetime.utcnow() - macros.timedelta(days=3))"
        ".strftime('%Y-%m-%dT%H:%M:%S.%f+00:00')}}",
        to_date="{{macros.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f+00:00')}}",
        key=f"{s3_prefix}/{{macros.datetime.utcnow().strftime('%Y%m%dT%H%M%S')}}.ndjson.gz",
        bucket=s3_buckets.tsos_da_int_inbound,
        s3_conn_id=conn_ids.S3.tsos_da_int_prod,
    )

    to_snowflake = SnowflakeIncrementalLoadOperator(
        task_id='stella_s3_to_snowflake',
        database='lake',
        schema='stella',
        table='feedback',
        staging_database='lake_stg',
        snowflake_conn_id=conn_ids.Snowflake.default,
        column_list=column_list,
        files_path=f"{stages.tsos_da_int_inbound}/{s3_prefix}/",
        copy_config=CopyConfigJson(),
    )

    snowflake_transform = SnowflakeProcedureOperator(
        database='reporting_prod',
        procedure='gms.stella_feedback.sql',
        watermark_tables=[
            TableDependencyTzLtz(
                table_name="lake.stella.feedback",
                column_name='meta_update_datetime',
            )
        ],
        warehouse='DA_WH_ETL_LIGHT',
    )

    stella_to_s3_dispute_backfill = StellaToS3Operator(
        task_id="stella_to_s3_dispute_backfill",
        from_date="{{(macros.datetime.utcnow() - macros.timedelta(months=3))"
        ".strftime('%Y-%m-%dT%H:%M:%S.%f+00:00')}}",
        to_date="{{macros.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f+00:00')}}",
        key=f"{s3_prefix_dispute_backfill}/{{macros.datetime.utcnow().strftime('%Y%m%d')}}/{{"
        f"macros.datetime.utcnow().strftime('%Y%m%dT%H%M%S')}}.ndjson.gz",
        bucket=s3_buckets.tsos_da_int_inbound,
        s3_conn_id=conn_ids.S3.tsos_da_int_prod,
    )

    to_snowflake_dispute_backfill = SnowflakeTruncateAndLoadOperator(
        task_id='stella_s3_to_snowflake_dispute_backfill',
        database='lake',
        schema='stella',
        table='feedback_backfill',
        staging_database='lake_stg',
        snowflake_conn_id=conn_ids.Snowflake.default,
        column_list=column_list,
        files_path=f"{stages.tsos_da_int_inbound}/{s3_prefix_dispute_backfill}/{{macros.datetime"
        f".utcnow().strftime('%Y%m%d')}}/",
        copy_config=CopyConfigJson(),
    )

    stella_feedback_update_dispute_records = SnowflakeProcedureOperator(
        database='reporting_prod',
        procedure='gms.stella_feedback_update_dispute_records.sql',
        warehouse='DA_WH_ETL_LIGHT',
    )

    def check_run_time(**kwargs):
        execution_time = kwargs['data_interval_end'].in_timezone('America/Los_Angeles')
        if execution_time.day == 15:
            return stella_to_s3_dispute_backfill.task_id
        else:
            return snowflake_transform.task_id

    check_for_disputed_records = BranchPythonOperator(
        python_callable=check_run_time, task_id='check_for_disputed_records'
    )

    stella_to_s3 >> to_snowflake >> check_for_disputed_records

    # if execution_time.day == 15
    check_for_disputed_records >> stella_to_s3_dispute_backfill >> to_snowflake_dispute_backfill
    to_snowflake_dispute_backfill >> stella_feedback_update_dispute_records >> snowflake_transform

    # else
    check_for_disputed_records >> snowflake_transform
