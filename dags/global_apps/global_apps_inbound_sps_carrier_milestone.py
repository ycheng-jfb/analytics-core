"""
SPS deposits many small files to SFTP.

To make the snowflake ``COPY`` less costly, the files for each run are written to tehir own
subdirectory in s3 derived from ``ts_nodash``.

"""

import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_gsc
from include.airflow.operators.sftp_to_s3 import SFTPToS3BatchZipOperator
from include.airflow.operators.snowflake import SnowflakeProcedureOperator, TableDependencyTzLtz
from include.airflow.operators.snowflake_load import SnowflakeCopyOperator
from include.config import conn_ids, email_lists, owners, s3_buckets, snowflake_roles, stages

default_args = {
    'start_date': pendulum.datetime(2020, 1, 1, tz='America/Los_Angeles'),
    'retries': 1,
    'owner': owners.data_integrations,
    'email': email_lists.data_integration_support,
    'on_failure_callback': slack_failure_gsc,
}


dag = DAG(
    dag_id='global_apps_inbound_sps_carrier_milestone',
    default_args=default_args,
    schedule='0 * * * *',
    catchup=False,
    max_active_tasks=6,
    max_active_runs=1,
    doc_md=__doc__,
)

s3_prefix = "lake/sps.carrier_milestone/v2/{{ ts_nodash.replace('T', '/') }}"

with dag:
    query = f"""
        COPY INTO lake.sps.carrier_milestone
        FROM (
            SELECT
                get_ignore_case($1:header, 'scac') AS scac,
                get_ignore_case($1:header, 'shipper_identification') AS shipper_identification,
                get_ignore_case($1:header, 'shipper_reference') AS shipper_reference,
                to_date(get_ignore_case($1:header, 'ship_date')::VARCHAR,'yyyymmdd') AS ship_date,
                get_ignore_case($1:header, 'address') AS address,
                get_ignore_case($1:header, 'reference') AS reference,
                get_ignore_case($1:header, 'shipment') AS shipment,
                metadata$filename,
                to_timestamp(split_part(split_part(metadata$filename, '/', -1), '_', 2), 'YYYYMMDDHH24MISS') AS filename_datetime,
                current_timestamp AS meta_create_datetime
            FROM '{stages.tsos_da_int_inbound}/{s3_prefix}/'
        )
        FILE_FORMAT = (
            TYPE='JSON'
            STRIP_NULL_VALUES=TRUE
        )
        ON_ERROR = 'continue';
    """  # noqa: E501

    to_s3 = SFTPToS3BatchZipOperator(
        task_id='sftp_sps_carrier_milestone',
        s3_prefix=s3_prefix,
        s3_bucket=s3_buckets.tsos_da_int_inbound,
        sftp_conn_id=conn_ids.SFTP.sftp_sps_commercevan,
        s3_conn_id=conn_ids.S3.tsos_da_int_prod,
        files_per_batch=1,
        remote_dir='/out',
        remove_remote_files=True,
    )

    to_snowflake = SnowflakeCopyOperator(
        task_id='carrier_milestone_lake_load',
        snowflake_conn_id=conn_ids.Snowflake.default,
        role=snowflake_roles.etl_service_account,
        sql_or_path=query,
        database='lake',
        schema='sps',
    )

    carrier_milestone = SnowflakeProcedureOperator(
        procedure='sps.carrier_milestone.sql',
        database='reporting_prod',
        watermark_tables=[
            TableDependencyTzLtz(
                table_name='lake.sps.carrier_milestone',
                column_name='meta_create_datetime',
            )
        ],
        warehouse='DA_WH_ETL_LIGHT',
    )

    to_s3 >> to_snowflake >> carrier_milestone
