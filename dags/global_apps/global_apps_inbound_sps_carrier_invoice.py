"""
SPS deposits many small files to SFTP.

To make the snowflake ``COPY`` less costly, the files for each run are written to tehir own
subdirectory in s3 derived from ``ts_nodash``.

"""

import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_gsc
from include.airflow.operators.sftp_to_s3 import SFTPToS3BatchOperator
from include.airflow.operators.snowflake import (
    SnowflakeProcedureOperator,
    TableDependencyTzLtz,
)
from include.airflow.operators.snowflake_load import SnowflakeCopyOperator
from include.config import (
    conn_ids,
    email_lists,
    owners,
    s3_buckets,
    snowflake_roles,
    stages,
)

default_args = {
    "start_date": pendulum.datetime(2020, 1, 1, tz="America/Los_Angeles"),
    "retries": 1,
    "owner": owners.data_integrations,
    "email": email_lists.data_integration_support,
    "on_failure_callback": slack_failure_gsc,
}


dag = DAG(
    dag_id="global_apps_inbound_sps_carrier_invoice",
    default_args=default_args,
    schedule="0 8 * * *",
    catchup=False,
    max_active_tasks=6,
    max_active_runs=1,
    doc_md=__doc__,
)

s3_prefix = "lake/sps.carrier_invoice/v2/{{ ts_nodash }}"

with dag:
    query = f"""
        COPY INTO lake.sps.carrier_invoice
        FROM (
            SELECT
                get_ignore_case($1:header, 'invoice_number') AS invoice_number,
                get_ignore_case($1:header, 'payment_method') AS payment_method,
                TO_DATE(get_ignore_case($1:header, 'invoice_date')::VARCHAR,
                'yyyymmdd') AS invoice_date,
                get_ignore_case($1:header, 'net_amount_due') AS net_amount_due,
                get_ignore_case($1:header, 'invoice_type') AS invoice_type,
                get_ignore_case($1:header, 'scac') AS scac,
                to_date(get_ignore_case($1:header, 'date_current')::VARCHAR,
                'yyyymmdd') AS date_current,
                get_ignore_case($1:header, 'settlement_option') AS settlement_option,
                get_ignore_case($1:header, 'currency_code') AS currency_code,
                get_ignore_case($1:header, 'terms_type') AS terms_type,
                get_ignore_case($1:header, 'terms_basis_date')::VARCHAR AS terms_basis_date,
                to_date(get_ignore_case($1:header, 'terms_net_due_date')::VARCHAR,
                'yyyymmdd') AS terms_net_due_date,
                get_ignore_case($1:header, 'terms_net_days') AS terms_net_days,
                get_ignore_case($1:header, 'invoice_reference') AS invoice_reference,
                get_ignore_case($1:header, 'invoice_note') AS invoice_note,
                get_ignore_case($1:header, 'account_number') AS account_number,
                get_ignore_case($1:header, 'package_count') AS package_count,
                get_ignore_case($1:header, 'total_charged') AS total_charged,
                get_ignore_case($1:header, 'sac_code') AS sac_code,
                get_ignore_case($1:header, 'billing') AS billing,
                get_ignore_case($1:header, 'package') AS package,
                get_ignore_case($1:header, 'reference') AS reference,
                metadata$filename AS file_name,
                TO_TIMESTAMP(split_part(metadata$filename, '_', 7),
                'yyyymmddhh24miss') AS datetime_added,
                current_timestamp AS meta_create_datetime
            FROM '{stages.tsos_da_int_inbound}/{s3_prefix}/'
        )
        FILE_FORMAT = (
            TYPE='JSON'
            STRIP_NULL_VALUES=TRUE
        )
        ON_ERROR = 'continue';
    """

    to_s3 = SFTPToS3BatchOperator(
        task_id="sftp_sps_carrier_invoice",
        s3_prefix=s3_prefix,
        s3_bucket=s3_buckets.tsos_da_int_inbound,
        sftp_conn_id=conn_ids.SFTP.sftp_sps_spscommerce,
        s3_conn_id=conn_ids.S3.tsos_da_int_prod,
        files_per_batch=100,
        remote_dir="/out",
        file_pattern="*.json",
        remove_remote_files=True,
    )

    to_snowflake = SnowflakeCopyOperator(
        task_id="carrier_lake_load",
        snowflake_conn_id=conn_ids.Snowflake.default,
        role=snowflake_roles.etl_service_account,
        sql_or_path=query,
        database="lake",
        schema="sps",
    )
    carrier_invoice = SnowflakeProcedureOperator(
        database="reporting_prod",
        procedure="sps.carrier_invoice.sql",
        watermark_tables=[
            TableDependencyTzLtz(
                table_name="lake.sps.carrier_invoice",
                column_name="meta_create_datetime",
            )
        ],
        warehouse="DA_WH_ETL_LIGHT",
    )
    carrier_invoice_billing = SnowflakeProcedureOperator(
        database="reporting_prod",
        procedure="sps.carrier_invoice_billing.sql",
        watermark_tables=[
            TableDependencyTzLtz(
                table_name="lake.sps.carrier_invoice",
                column_name="meta_create_datetime",
            )
        ],
        warehouse="DA_WH_ETL_LIGHT",
    )
    carrier_invoice_package = SnowflakeProcedureOperator(
        database="reporting_prod",
        procedure="sps.carrier_invoice_package.sql",
        watermark_tables=[
            TableDependencyTzLtz(
                table_name="lake.sps.carrier_invoice",
                column_name="meta_create_datetime",
            )
        ],
        warehouse="DA_WH_ETL_LIGHT",
    )
    carrier_invoice_package_charge = SnowflakeProcedureOperator(
        database="reporting_prod",
        procedure="sps.carrier_invoice_package_charge.sql",
        watermark_tables=[
            TableDependencyTzLtz(
                table_name="lake.sps.carrier_invoice",
                column_name="meta_create_datetime",
            )
        ],
        warehouse="DA_WH_ETL_LIGHT",
    )
    carrier_invoice_package_shipment = SnowflakeProcedureOperator(
        database="reporting_prod",
        procedure="sps.carrier_invoice_package_shipment.sql",
        watermark_tables=[
            TableDependencyTzLtz(
                table_name="lake.sps.carrier_invoice",
                column_name="meta_create_datetime",
            )
        ],
        warehouse="DA_WH_ETL_LIGHT",
    )
    carrier_invoice_package_milestone = SnowflakeProcedureOperator(
        database="reporting_prod",
        procedure="sps.carrier_invoice_package_milestone.sql",
        watermark_tables=[
            TableDependencyTzLtz(
                table_name="lake.sps.carrier_invoice",
                column_name="meta_create_datetime",
            )
        ],
        warehouse="DA_WH_ETL_LIGHT",
    )

    (
        to_s3
        >> to_snowflake
        >> carrier_invoice
        >> [carrier_invoice_billing, carrier_invoice_package]
    )
    carrier_invoice_package >> [
        carrier_invoice_package_charge,
        carrier_invoice_package_shipment,
    ]
    carrier_invoice_package_shipment >> carrier_invoice_package_milestone
