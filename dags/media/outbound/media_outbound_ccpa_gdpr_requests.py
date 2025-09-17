from collections import namedtuple

import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_media
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.airflow.operators.snowflake_export import SnowflakeToSFTPOperator
from include.config import owners
from include.config.conn_ids import SFTP
from include.config.email_lists import airflow_media_support

data_interval_start = "{{ data_interval_end.strftime('%Y%m%d') }}"

default_args = {
    "depends_on_past": False,
    "start_date": pendulum.datetime(2019, 9, 16, tz="America/Los_Angeles"),
    'owner': owners.media_analytics,
    "email": airflow_media_support,
    "on_failure_callback": slack_failure_media,
}


dag = DAG(
    dag_id="media_outbound_ccpa_gdpr_requests",
    default_args=default_args,
    schedule="30 1 * * *",
    catchup=False,
    max_active_tasks=1,
    max_active_runs=1,
)

SftpPath = namedtuple('SftpPath', ['name'])
res = [SftpPath('gdpr'), SftpPath('ccpa')]

with dag:
    execute_ccpa_gdpr_requests = SnowflakeProcedureOperator(
        procedure='dbo.ccpa_gdpr_requests.sql',
        database='reporting_media_prod',
    )
    for item in res:
        if item.name == 'gdpr':
            sql = """
                    SELECT edw_prod.stg.udf_unconcat_brand(customer_id),
                           email1,
                           email2,
                           phonenumber1,
                           store_id,
                           store_brand_abbr,
                           store_country,
                           store_region,
                           date_requested,
                           datetime_completed,
                           request_source,
                           request_type,
                           status
                    FROM reporting_media_prod.dbo.ccpa_gdpr_requests
                    WHERE (region_id = '1' or (region_id='2' and store_region = 'EU'))
                        and date_requested >=DATEADD(MONTH, -3, current_date());
                        """
        else:
            sql = """
                    SELECT edw_prod.stg.udf_unconcat_brand(customer_id),
                           email1,
                           email2,
                           phonenumber1,
                           store_id,
                           store_brand_abbr,
                           store_country,
                           store_region,
                           date_requested,
                           datetime_completed,
                           request_source,
                           request_type,
                           status
                    FROM reporting_media_prod.dbo.ccpa_gdpr_requests
                    WHERE region_id = '2' and store_region != 'EU'
                        and date_requested >=DATEADD(MONTH, -3, current_date());
                        """
        post_to_sftp = SnowflakeToSFTPOperator(
            task_id=f"{item.name}_requests_to_sftp",
            sql_or_path=sql,
            sftp_conn_id=SFTP.sftp_techstyle_optout,
            filename=f"techstyle_opt_out_deletions_{data_interval_start}.csv",
            sftp_dir=f"/secure_reports/{item.name}_requests/",
            field_delimiter=',',
            header=True,
        )
        execute_ccpa_gdpr_requests >> post_to_sftp
