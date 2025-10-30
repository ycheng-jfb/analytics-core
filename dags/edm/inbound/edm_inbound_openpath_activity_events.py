from datetime import timedelta

import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.openpath import OpenpathActivityEventsToS3Operator
from include.airflow.operators.snowflake_load import (
    Column,
    CopyConfigCsv,
    SnowflakeIncrementalLoadOperator,
)
from include.config import email_lists, owners, s3_buckets, stages, conn_ids


schema = "openpath"
table = "access_activity_events"
s3_prefix = f"media/{schema}.{table}/daily"

column_list = [
    Column('acu_id', "NUMBER", source_name='acuId', uniqueness=True),
    Column(
        'category',
        "VARCHAR",
        source_name='category',
    ),
    Column(
        'source_name',
        "VARCHAR",
        source_name='sourceName',
    ),
    Column(
        'sub_category',
        "VARCHAR",
        source_name='subCategory',
    ),
    Column('time_iso_string', "TIMESTAMP_TZ", source_name='timeIsoString', uniqueness=True),
    Column(
        'reader_port_number',
        "NUMBER",
        source_name='readerPortNumber',
    ),
    Column(
        'reader_id',
        "NUMBER",
        source_name='readerId',
    ),
    Column('user_id', "NUMBER", source_name='userId', uniqueness=True),
    Column(
        'user_org_id',
        "NUMBER",
        source_name='userOrgId',
    ),
    Column(
        'credential_type_model_name',
        "VARCHAR",
        source_name='credentialTypeModelName',
    ),
    Column(
        'credential_detail',
        "VARCHAR",
        source_name='credentialDetail',
    ),
    Column(
        'card_fields',
        "VARCHAR",
        source_name='cardFields',
    ),
    Column(
        'credential_subtype',
        "VARCHAR",
        source_name='credentialSubtype',
    ),
    Column(
        'request_type',
        "VARCHAR",
        source_name='requestType',
    ),
    Column(
        'result',
        "VARCHAR",
        source_name='result',
    ),
    Column(
        'result_description',
        "VARCHAR",
        source_name='resultDescription',
    ),
    Column(
        'location',
        "VARCHAR",
        source_name='location',
    ),
    Column(
        'user_first_name',
        "VARCHAR",
        source_name='userFirstName',
    ),
    Column(
        'user_middle_name',
        "VARCHAR",
        source_name='userMiddleName',
    ),
    Column(
        'user_last_name',
        "VARCHAR",
        source_name='userLastName',
    ),
    Column(
        'user_name',
        "VARCHAR",
        source_name='userName',
    ),
    Column(
        'user_email',
        "VARCHAR",
        source_name='userEmail',
    ),
    Column(
        'user_external_id',
        "VARCHAR",
        source_name='userExternalId',
    ),
    Column(
        'user_idp_name',
        "VARCHAR",
        source_name='userIdpName',
    ),
    Column(
        'credential_type_name',
        "VARCHAR",
        source_name='credentialTypeName',
    ),
    Column(
        'entry_name',
        "VARCHAR",
        source_name='entryName',
    ),
    Column('zone_id', "NUMBER", source_name='zoneId', uniqueness=True),
    Column(
        'zone_name',
        "VARCHAR",
        source_name='zoneName',
    ),
    Column(
        'site_id',
        "NUMBER",
        source_name='siteId',
    ),
    Column(
        'site_name',
        "VARCHAR",
        source_name='siteName',
    ),
    Column("updated_at", "TIMESTAMP_LTZ", delta_column=True),
]


default_args = {
    "start_date": pendulum.datetime(2021, 8, 23, 7, tz="America/Los_Angeles"),
    "retries": 1,
    'owner': owners.data_integrations,
    "email": email_lists.data_integration_support,
    "on_failure_callback": slack_failure_edm,
    "execution_timeout": timedelta(hours=1),
}


dag = DAG(
    dag_id=f"edm_inbound_openpath_activity_events",
    default_args=default_args,
    schedule="0 12 * * *",
    catchup=False,
    max_active_runs=1,
)


with dag:
    date_param: str = "{{ data_interval_start.strftime('%Y%m%dT%H%M%S%z')[0:-3] }}"
    start_time = "{{ (prev_data_interval_start_success or data_interval_start).strftime('%Y-%m-%dT%H:%M:%S%z')}}"
    end_time = "{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%S%z') }}"

    to_snowflake = SnowflakeIncrementalLoadOperator(
        task_id="openpath_activity_events_to_snowflake",
        database="lake",
        schema=schema,
        table=table,
        column_list=column_list,
        files_path=f"{stages.tsos_da_int_inbound}/{s3_prefix}",
        copy_config=CopyConfigCsv(field_delimiter='\t', header_rows=0, skip_pct=3),
    )
    get_data = OpenpathActivityEventsToS3Operator(
        task_id=f"openpath_activity_events_to_s3",
        bucket=s3_buckets.tsos_da_int_inbound,
        key=f"{s3_prefix}/openpath_{date_param}.tsv.gz",
        column_list=[x.source_name for x in column_list],
        s3_conn_id=conn_ids.S3.tsos_da_int_prod,
        req_params={
            "org_id": 3419,
            "endpoint": 'activity/events',
            "start_time": start_time,
            "end_time": end_time,
        },
    )
    get_data >> to_snowflake
