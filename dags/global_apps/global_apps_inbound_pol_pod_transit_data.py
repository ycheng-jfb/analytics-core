import pendulum
from airflow import DAG

from include.airflow.callbacks.slack import slack_failure_gsc
from include.airflow.operators.excel_smb import ExcelSMBToS3BatchOperator, SheetConfig
from include.airflow.operators.snowflake_load import CopyConfigCsv, SnowflakeTruncateAndLoadOperator
from include.config import conn_ids, email_lists, owners, s3_buckets, stages
from include.utils.snowflake import Column

default_args = {
    "start_date": pendulum.datetime(2022, 10, 10, 7, tz="America/Los_Angeles"),
    "owner": owners.data_integrations,
    "email": email_lists.data_integration_support,
    "on_failure_callback": slack_failure_gsc,
}

dag = DAG(
    dag_id='global_apps_inbound_pol_pod_transit_data',
    default_args=default_args,
    schedule='0 */2 * * *',
    catchup=False,
)

column_list = [
    Column('traffic_mode', 'VARCHAR', source_name='TRAFFIC_MODE'),
    Column('pol_city', 'VARCHAR', source_name='POL_CITY'),
    Column('pol_state_country', 'VARCHAR', source_name='POL_STATE_COUNTRY'),
    Column('pod_city', 'VARCHAR', source_name='POD_CITY'),
    Column('pod_state_country', 'VARCHAR', source_name='POD_STATE_COUNTRY'),
    Column('fc_code', 'VARCHAR', source_name='FC_CODE'),
    Column('null_value', 'INT', source_name='NULL_VALUE'),
    Column('full_supply_chain', 'INT', source_name='FULL_SUPPLY_CHAIN'),
    Column('cargo_received', 'INT', source_name='CARGO_RECEIVED'),
    Column('vessel_departed_no_eta', 'INT', source_name='VESSEL_DEPARTED_NO_ETA'),
    Column('vessel_departed_with_eta', 'INT', source_name='VESSEL_DEPARTED_WITH_ETA'),
    Column('vessel_arrived', 'INT', source_name='VESSEL_ARRIVED'),
    Column('transload_departed', 'INT', source_name='TRANSLOAD_DEPARTED'),
]

sheets = {
    "sheet": SheetConfig(
        sheet_name='Sheet 1',
        schema='excel',
        table='pol_pod_transit_data',
        s3_replace=False,
        header_rows=1,
        column_list=column_list,
    )
}

s3_key = 'lake/excel.pol_pod_transit_data/v1'

with dag:
    pol_pod_transit_data_to_s3 = ExcelSMBToS3BatchOperator(
        task_id="pol_pod_transit_data_to_s3",
        smb_dir='Inbound/airflow.pol_pod_transit',
        share_name='BI',
        file_pattern_list=['*.xls*'],
        bucket=s3_buckets.tsos_da_int_inbound,
        s3_conn_id=conn_ids.S3.tsos_da_int_prod,
        smb_conn_id=conn_ids.SMB.nas01,
        is_archive_file=False,
        sheet_configs=[sheets['sheet']],
        remove_header_new_lines=True,
        skip_downstream_if_no_files=True,
    )
    pol_pod_transit_data_to_snowflake = SnowflakeTruncateAndLoadOperator(
        task_id='pol_pod_transit_data_to_snowflake',
        database='lake',
        schema='excel',
        table='pol_pod_transit_data',
        staging_database='lake_stg',
        view_database='lake_view',
        column_list=column_list,
        files_path=f'{stages.tsos_da_int_inbound}/{s3_key}',
        copy_config=CopyConfigCsv(field_delimiter='|', header_rows=1),
    )

    pol_pod_transit_data_to_s3 >> pol_pod_transit_data_to_snowflake
