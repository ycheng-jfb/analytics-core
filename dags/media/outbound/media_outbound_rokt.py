import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_media
from include.airflow.operators.snowflake_export import SnowflakeToSFTPOperator
from include.config import owners
from include.config.conn_ids import SFTP
from include.config.email_lists import airflow_media_support

data_interval_start = "{{ ts_nodash }}"


default_args = {
    "depends_on_past": False,
    "start_date": pendulum.datetime(2024, 2, 20, tz="America/Los_Angeles"),
    'owner': owners.media_analytics,
    "email": airflow_media_support,
    "on_failure_callback": slack_failure_media,
}


dag = DAG(
    dag_id="media_outbound_rokt",
    default_args=default_args,
    schedule="30 7 * * *",
    catchup=False,
    max_active_tasks=1,
    max_active_runs=1,
)
sql = """   select sha2(email) as email
            from reporting_media_base_prod.dbo.vips_audience
            where cancellation_local_datetime > current_timestamp()
                and store_brand_abbr = 'FL'
                and store_region = 'NA'
"""

with dag:
    post_to_sftp = SnowflakeToSFTPOperator(
        task_id=f"post_to_sftp",
        sql_or_path=sql,
        sftp_conn_id=SFTP.sftp_rokt,
        filename=f"techstyle_fl_vips_{data_interval_start}.csv",
        sftp_dir=f"/upload/custom-audience/include/AdvertiserDatabase",
        field_delimiter=',',
    )
