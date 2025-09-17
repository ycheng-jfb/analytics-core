import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.airflow.operators.snowflake_export import SnowflakeToSFTPOperator
from include.config import conn_ids, owners
from include.config.email_lists import data_integration_support

default_args = {
    "depends_on_past": False,
    "start_date": pendulum.datetime(2024, 10, 1, 7, tz="America/Los_Angeles"),
    "retries": 1,
    'owner': owners.data_integrations,
    "email": data_integration_support,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id='edm_outbound_case_auto_tagging_sftp_callminer',
    default_args=default_args,
    schedule='0 19 * * *',
    catchup=False,
    max_active_tasks=1,
    max_active_runs=1,
)
export_sql = """
SET low_watermark = %(low_watermark)s :: TIMESTAMP_LTZ;
select
    CONVERSATION_ID,
    CUSTOMER_ID,
    STORE_NAME_REGION,
    STORE_BRAND_ABBR,
    STORE_COUNTRY,
    CASE_MANUALLY_CREATED,
    CONCATENATED_TAGS,
FROM reporting_prod.gms.callminer_case_auto_tagging
where meta_create_datetime > $low_watermark
"""

date_param = "{{macros.tfgdt.to_pst(macros.datetime.now()).strftime('%Y%m%d')}}"

with dag:
    case_auto_tagging = SnowflakeProcedureOperator(
        database="reporting_prod",
        procedure="gms.callminer_case_auto_tagging.sql",
        watermark_tables=['reporting_prod.gms.callminer_case_auto_tagging'],
        initial_load_value='2024-07-01',
    )

    export_to_sftp = SnowflakeToSFTPOperator(
        task_id="case_auto_tagging_sftp_callminer",
        sql_or_path=export_sql,
        sftp_conn_id=conn_ids.SFTP.sftp_callminer,
        sftp_dir=f"AutoTagging/",
        filename=f"case_auto_tagging_{date_param}.csv",
        field_delimiter="|",
        header=True,
        watermark_tables=['reporting_prod.gms.callminer_case_auto_tagging'],
        initial_load_value='2024-07-01',
    )

    case_auto_tagging >> export_to_sftp
