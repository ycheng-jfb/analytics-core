import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_gsc
from include.airflow.operators.genesys import GenesysParticipantsToS3Operator
from include.airflow.operators.snowflake_load import SnowflakeIncrementalLoadOperator
from include.config import conn_ids, email_lists, owners, s3_buckets, snowflake_roles, stages
from include.utils.snowflake import Column, CopyConfigJson

column_list = [
    Column('conversation_id', 'STRING', uniqueness=True),
    Column('conversation_start', 'TIMESTAMP_LTZ(3)'),
    Column('conversation_end', 'TIMESTAMP_LTZ(3)'),
    Column('participant_id', 'VARCHAR', uniqueness=True),
    Column('participant_name', 'VARCHAR'),
    Column('purposecustomer', 'VARCHAR'),
    Column('first_name', 'VARCHAR'),
    Column('last_name', 'VARCHAR'),
    Column('email', 'VARCHAR'),
    Column('phone_number', 'VARCHAR'),
    Column('address_street', 'VARCHAR'),
    Column('address_city', 'VARCHAR'),
    Column('address_state', 'VARCHAR'),
    Column('address_postal_code', 'VARCHAR'),
    Column('legacy_routing_target_queue_address', 'VARCHAR'),
    Column('gdf_intent_par_data', 'VARCHAR'),
    Column('queue_id', 'VARCHAR'),
    Column('brand_url_set', 'VARCHAR'),
    Column('parameters', 'VARCHAR'),
    Column('queue_default', 'VARCHAR'),
    Column('cancellation_reason', 'VARCHAR'),
    Column('endpoint_url_token', 'VARCHAR'),
    Column('default_queue', 'VARCHAR'),
    Column('language', 'VARCHAR'),
    Column('session_id', 'VARCHAR'),
    Column('locale', 'VARCHAR'),
    Column('intent', 'VARCHAR'),
    Column('user_id', 'VARCHAR'),
    Column('script_id', 'VARCHAR'),
    Column('store_group_id', 'VARCHAR'),
    Column('customer_id', 'VARCHAR'),
    Column('genesys_language', 'VARCHAR'),
    Column('name', 'VARCHAR'),
    Column('brand', 'VARCHAR'),
    Column('attribute_email', 'VARCHAR'),
    Column('goals', 'VARCHAR'),
    Column('purecloud', 'VARCHAR'),
    Column('custom_field1', 'VARCHAR'),
    Column('custom_field2', 'VARCHAR'),
    Column('custom_field3', 'VARCHAR'),
    Column('custom_field4', 'VARCHAR'),
    Column('custom_field1_label', 'VARCHAR'),
    Column('custom_field2_label', 'VARCHAR'),
    Column('custom_field3_label', 'VARCHAR'),
    Column('custom_field4_label', 'VARCHAR'),
]


default_args = {
    'start_date': pendulum.datetime(2021, 7, 27, tz="America/Los_Angeles"),
    'retries': 3,
    'owner': owners.data_integrations,
    'email': email_lists.data_integration_support,
    'on_failure_callback': slack_failure_gsc,
}


dag = DAG(
    dag_id="global_apps_inbound_genesys_participants",
    default_args=default_args,
    schedule="1 1,15 * * *",
    catchup=False,
    max_active_tasks=100,
    max_active_runs=1,
)

s3_prefix = 'lake/genesys.participants/v3/'

with dag:
    to_s3 = GenesysParticipantsToS3Operator(
        task_id="genesys_participants_to_s3",
        from_date="{{ macros.tfgdt.utcnow().add(days=-5) }}",
        to_date="{{ macros.tfgdt.utcnow() }}",
        key=s3_prefix + "{{ ts_nodash }}",
        bucket=s3_buckets.tsos_da_int_inbound,
        genesys_conn_id=conn_ids.Genesys.genesys_bond,
        s3_conn_id=conn_ids.S3.tsos_da_int_prod,
        fail_on_no_rows=False,
    )
    to_snowflake = SnowflakeIncrementalLoadOperator(
        task_id='genesys_participants_s3_to_snowflake',
        database='lake',
        schema='genesys',
        table='participants',
        staging_database='lake_stg',
        snowflake_conn_id=conn_ids.Snowflake.default,
        role=snowflake_roles.etl_service_account,
        column_list=column_list,
        files_path=f"{stages.tsos_da_int_inbound}/{s3_prefix}",
        copy_config=CopyConfigJson(),
    )

    to_s3 >> to_snowflake
