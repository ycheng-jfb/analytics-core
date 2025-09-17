import lazy_object_proxy
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.creatoriq import CreatorIQPatchOperator
from include.airflow.operators.dagrun_operator import TFGTriggerDagRunOperator
from include.airflow.operators.excel_smb import SheetConfig
from include.airflow.operators.mssql_acquisition import MsSqlTableToS3CsvOperator
from include.airflow.operators.snowflake import SnowflakeSqlOperator
from include.airflow.operators.snowflake_export import SnowflakeToS3Operator
from include.airflow.operators.snowflake_load import SnowflakeCopyOperator
from include.config import conn_ids, owners, s3_buckets, snowflake_roles
from include.config import stages
from include.utils.acquisition.table_config import TableConfig as AcquisitionTableConfig
from include.utils.snowflake import Column

default_args = {
    'start_date': pendulum.datetime(2019, 11, 19, tz='America/Los_Angeles'),
    'retries': 0,
    'owner': owners.data_integrations,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id='airflow_test_dag',
    default_args=default_args,
    schedule='0 0 * * *',
    catchup=False,
    max_active_tasks=1000,
    max_active_runs=1,
)

column_list = [
    Column("brand", "VARCHAR(55)", uniqueness=True, source_name="Brand"),
]

config = {
    "fields": [
        "account_id",
        "spend",
        "date_start",
        "date_stop",
    ],
    "time_range": {"since": "2021-04-29", "until": "2021-04-30"},
    "level": "ad",
    "time_increment": "1",
    "breakdowns": "hourly_stats_aggregated_by_advertiser_time_zone",
}
column_list_fb = [
    Column("account_id", "VARCHAR"),
    Column("spend", "DECIMAL(38, 8)"),
    Column("date_start", "DATE", uniqueness=True),
    Column("date_stop", "DATE", uniqueness=True),
    Column("hourly_stats_aggregated_by_advertiser_time_zone", "VARCHAR", uniqueness=True),
    Column("api_call_timestamp", "TIMESTAMP_NTZ(3)", delta_column=1),
]

# copy-paste-test-check
# table_config = TableConfig(
#     database='edw',
#     schema='dbo',
#     table='dim_address',
#     target_database='work',
#     target_schema='dbo',
#     initial_load_value='2021-05-03',
#     watermark_column='meta_update_datetime',
#     source_column_mapping={
#         'src_meta_create_datetime': 'meta_create_datetime',
#         'src_meta_update_datetime': 'meta_update_datetime',
#     },
#     column_list=[
#         Column('address_key', 'INT', uniqueness=True),
#         Column('address_id', 'INT'),
#         Column('street_address1', 'VARCHAR(50)'),
#         Column('street_address2', 'VARCHAR(35)'),
#         Column('city', 'VARCHAR(35)'),
#         Column('state', 'VARCHAR(25)'),
#         Column('zip_code', 'VARCHAR(25)'),
#         Column('country_code', 'VARCHAR(3)'),
#         Column('src_meta_create_datetime', 'TIMESTAMP_NTZ(7)', delta_column=1),
#         Column('src_meta_update_datetime', 'TIMESTAMP_NTZ(7)', delta_column=0),
#         Column('meta_create_package_exec_id', 'INT'),
#         Column('meta_update_package_exec_id', 'INT'),
#         Column('meta_row_hash_type1', 'BINARY(20)'),
#         Column('meta_row_hash_type2', 'BINARY(20)'),
#         Column('meta_effective_start_datetime', 'TIMESTAMP_NTZ(7)'),
#         Column('meta_effective_end_datetime', 'TIMESTAMP_NTZ(7)'),
#         Column('meta_is_current', 'BOOLEAN'),
#         Column('meta_data_quality_id', 'VARCHAR(36)'),
#     ],
#     split_files=True,
# )
# acquisition process check
upsert_table_config = AcquisitionTableConfig(
    database='ultrawarehouse',
    schema='dbo',
    table='outbound_plan_log',
    column_list=[
        Column('outbound_plan_log_id', 'INT', uniqueness=True),
        Column('outbound_plan_id', 'INT'),
        Column('run_datetime', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
    is_full_upsert=True,
)

acquisition_table_config = AcquisitionTableConfig(
    database='ultrawarehouse',
    schema='dbo',
    table='outbound_preallocation',
    watermark_column='datetime_modified',
    column_list=[
        Column('outbound_preallocation_id', 'INT', uniqueness=True),
        Column('warehouse_id', 'INT'),
        Column('case_id', 'INT'),
        Column('lpn_id', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=1),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)', delta_column=0),
        Column('preallocation_type', 'VARCHAR(255)'),
    ],
)

sheets = [
    SheetConfig(
        sheet_name='test',
        schema='excel',
        table='test_table',
        header_rows=1,
        column_list=[
            Column('comparison_type', 'STRING', uniqueness=True),
            Column('issued_datetime', 'DATETIME'),
            Column('month', 'DATE', uniqueness=True),
            Column('bu', 'STRING', uniqueness=True),
            Column('membership_credit_charged_count', 'NUMBER(19,4)'),
            Column('update_timestamp', 'TIMESTAMP_LTZ(3)', delta_column=True),
        ],
        add_meta_cols={'update_timestamp': lazy_object_proxy.Proxy(pendulum.DateTime.utcnow)},
    ),
]


def get_next_execution_date(execution_date, context):
    return context['data_interval_end']


with dag:
    sql_test_cmd = f"""
    COPY INTO lake_stg.tableau.users_by_group_stg (
        group_name,
        group_id,
        user_name,
        user_id,
        user_auth_setting,
        user_last_login,
        user_site_role,
        user_language,
        user_locale,
        user_external_auth_user_id,
        updated_at
    )
    FROM '{'@lake_stg.public.tsos_da_int_inbound_dev'}/lake/lake.tableau.users_by_group/v2/'
    FILE_FORMAT=(
        TYPE = CSV,
        FIELD_DELIMITER = '\t',
        RECORD_DELIMITER = '\n',
        FIELD_OPTIONALLY_ENCLOSED_BY = '"',
        SKIP_HEADER = 0,
        ESCAPE_UNENCLOSED_FIELD = NONE,
        NULL_IF = ('')
    )
    ON_ERROR = 'SKIP_FILE_1%'
    ;
    """  # Temporarily fixed stage name

    to_snowflake = SnowflakeCopyOperator(
        task_id='test-no-file-copy-messaging',
        snowflake_conn_id=conn_ids.Snowflake.default.value,
        role=snowflake_roles.etl_service_account,
        sql_or_path=sql_test_cmd,
        email_to_when_no_file='jlorence@techstyle.com',
    )

    snowflake_op = SnowflakeSqlOperator(
        task_id='test-snowflake-op',
        sql_or_path='select current_database() as my_db',
        database='reporting',
    )

    to_s3 = MsSqlTableToS3CsvOperator(
        bucket='tsos-da-int-inbound-dev',  # Hardcoded temporarily to resolve prod conn_id issue
        key='test/test_dag/s3_op_test.tsv.gz',
        s3_conn_id=conn_ids.S3.tsos_da_int_dev,
        mssql_conn_id=conn_ids.MsSql.bento_prod_reader_app_airflow,
        src_schema='dbo',
        src_table='store',
        src_database='ultramerchant',
        watermark_process_name='test-process',
        watermark_namespace='test-dag',
        task_id='test-mssql-s3',
    )

    snowflake_to_S3 = SnowflakeToS3Operator(
        task_id="snowflake_to_S3",
        sql_or_path="SELECT * FROM med_db_staging.export.first_party_lists limit 10",
        s3_conn_id=conn_ids.S3.tsos_da_int_dev,
        bucket='tsos-da-int-inbound-dev',  # Hardcoded temporarily to resolve prod conn_id issue
        key="test/test_dag/SnowflakeToS3Operator_test.csv.gz",
        field_delimiter=",",
        compression="gzip",
        header=True,
        initial_load_value="2019-10-06T20:23:15.824000-07:00",
    )

    op1 = CreatorIQPatchOperator(
        task_id="patch_campaign_ids",
        sql_cmd="""
        select distinct campaignid from med_db_staging.creatoriq.campaign_list
        where campaignname ilike '%%hdyh%%' and
        startdate = date_trunc(month,current_date()::timestamp_ltz)
        """,
    )

    s3_prefix = 'test/conversations'
    column_list = [
        Column("brand", "VARCHAR(55)", uniqueness=True, source_name="Brand"),
    ]

    # copy_paste_to_s3 = table_config.to_s3_operator
    #
    # copy_paste_to_snowflake = table_config.to_snowflake_operator
    #
    # copy_paste_to_s3 >> copy_paste_to_snowflake

    acq_to_s3 = acquisition_table_config.to_s3_operator
    acq_to_s3_upsert = upsert_table_config.to_s3_operator
    acq_to_snowflake = acquisition_table_config.to_snowflake_operator
    acq_to_snowflake_upsert = upsert_table_config.to_snowflake_operator

    acq_to_s3 >> acq_to_snowflake
    acq_to_s3_upsert >> acq_to_snowflake_upsert

    # Uncomment when have dev conn_id setup
    # excel_ingestion = ExcelSMBToS3Operator(
    #     task_id='test_excel_to_s3',
    #     smb_path='Inbound/airflow.test_dag/test_file.xlsx',
    #     share_name='BI',
    #     bucket=s3_buckets.tfg_edm,
    #     s3_conn_id=,
    #     smb_conn_id=conn_ids.SMB.nas01,
    #     is_archive_file=True,
    #     sheet_configs=sheets,
    #     remove_header_new_lines=True,
    # )
    trigger_child_dag = TFGTriggerDagRunOperator(
        task_id="trigger_test_child_dag",
        trigger_dag_id="airflow_test_child_dag",
        execution_date='{{ data_interval_end }}',
        skip_downstream_if_paused=True,
    )
    await_child_test_child_dag = ExternalTaskSensor(
        task_id="await_child_test_child_dag",
        external_dag_id="airflow_test_child_dag",
        mode='reschedule',
        execution_date_fn=get_next_execution_date,
        poke_interval=60 * 5,
    )
    trigger_child_dag >> await_child_test_child_dag

    def print_args(next_exec_date, pst_now):
        print('next execution date:', next_exec_date)
        print('macros tfgdt:', pst_now)

    test_template_macros = PythonOperator(
        task_id='test_template_macros',
        python_callable=print_args,
        op_args=['{{data_interval_end}}', "{{ macros.tfgdt.pstnow().date() }}"],
    )
