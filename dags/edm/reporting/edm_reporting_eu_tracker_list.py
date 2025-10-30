import pendulum
from airflow import DAG

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.airflow.operators.snowflake_export import SnowflakeToWindowsShareOperator
from include.config import owners, conn_ids
from include.config.email_lists import data_integration_support

default_args = {
    "start_date": pendulum.datetime(2022, 1, 30, tz="America/Los_Angeles"),
    'owner': owners.data_integrations,
    "email": data_integration_support,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id='edm_reporting_eu_tracker_list',
    default_args=default_args,
    schedule="20 4 * * 2",
    catchup=False,
    max_active_tasks=1,
    max_active_runs=1,
)

query = """
SET current_datetime =
(SELECT MAX(current_datetime)
FROM reporting_prod.gms.eu_tracker_list
WHERE segment = '{SEGMENT_TYPE}');

SELECT
customer_id,
membership_id
FROM reporting_prod.gms.eu_tracker_list
WHERE current_datetime = $current_datetime
AND segment = '{SEGMENT_TYPE}'
AND store_name = '{BRAND}';
"""

segment_types = ['Leads', 'Vips']
brands = [
    'Fabletics UK',
    'Fabletics ES',
    'Fabletics DE',
    'Fabletics FR',
    'JustFab ES',
    'JustFab DE',
    'JustFab FR',
    'JustFab UK',
]

yr_mth_dy = "{{macros.datetime.now().strftime('%Y%m%d')}}"

with dag:
    eu_tracker_list = SnowflakeProcedureOperator(
        procedure='gms.eu_tracker_list.sql', database='reporting_prod'
    )

    to_sftps_tasks = []

    for segment in segment_types:
        for brand in brands:
            snowflake_to_sftp = SnowflakeToWindowsShareOperator(
                task_id=f"snowflake_to_sftp_{segment}_{brand.replace(' ', '_')}",
                sql_or_path=query.format(SEGMENT_TYPE=segment, BRAND=brand),
                smb_conn_id=conn_ids.SMB.nas01,
                filename=f"EU Tracker List {segment}_{yr_mth_dy}.csv",
                shared_dir=f"/Reporting/EUTrackerReports/{brand}",
                share_name='BI',
                field_delimiter=",",
                header=True,
                dialect='excel',
            )
            to_sftps_tasks.append(snowflake_to_sftp)

    eu_tracker_list >> to_sftps_tasks
