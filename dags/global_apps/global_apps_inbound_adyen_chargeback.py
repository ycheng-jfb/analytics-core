import pendulum
from airflow import DAG

from include.airflow.callbacks.slack import SlackFailureCallback
from include.airflow.operators.adyen import AdyenToS3Operator
from include.airflow.operators.snowflake_load import SnowflakeIncrementalLoadOperator
from include.config import conn_ids, email_lists, owners, s3_buckets, stages
from include.utils.snowflake import Column, CopyConfigCsv

default_args = {
    "start_date": pendulum.datetime(2020, 12, 1, tz="America/Los_Angeles"),
    "retries": 1,
    "owner": owners.data_integrations,
    "email": email_lists.global_applications_gms,
    "on_failure_callback": SlackFailureCallback("slack_alert_gsc"),
}
dag = DAG(
    dag_id="global_apps_inbound_adyen_chargeback",
    default_args=default_args,
    schedule="0 6 1 * *",
    catchup=False,
    max_active_tasks=20,
)

column_list = [
    Column("bu", "string", uniqueness=True),
    Column("report_date", "TIMESTAMP_NTZ(9)", uniqueness=True),
    Column("cb_count", "float"),
    Column("cb_amount", "float"),
    Column("cb_reversed_count", "float"),
    Column("cb_reversed_amount", "float"),
    Column("second_cb_count", "float"),
    Column("second_cb_amount", "float"),
    Column("paypal_refund_count", "float"),
    Column("paypal_refund_amount", "float"),
]


last_month = (
    "{{macros.datetime.now().year}}_{{'12' if macros.datetime.now().month == 1 "
    "else ((macros.datetime.now().month - 1)|string).zfill(2)}}"
)
s3_prefix = f"lake/lake.gms.adyen_chargeback/v2/{last_month}"
with dag:
    adyen_to_s3 = AdyenToS3Operator(
        task_id="adyen_to_s3",
        report_name=f"monthly_finance_report_{last_month}.xls",
        output_table="reporting.gms.adyen_chargeback",
        s3_conn_id=conn_ids.S3.tsos_da_int_prod,
        key=f"{s3_prefix}/{{macros.datetime.utcnow().strftime('%Y%m%dT%H%M%S')}}.csv.gz",
        bucket=s3_buckets.tsos_da_int_inbound,
    )
    to_snowflake = SnowflakeIncrementalLoadOperator(
        task_id="adyen_s3_to_snowflake",
        database="lake",
        schema="gms",
        table="adyen_chargeback",
        staging_database="lake_stg",
        view_database="lake_view",
        snowflake_conn_id=conn_ids.Snowflake.default,
        column_list=column_list,
        files_path=f"{stages.tsos_da_int_inbound}/{s3_prefix}/",
        copy_config=CopyConfigCsv(header_rows=1),
    )
    adyen_to_s3 >> to_snowflake
