import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_gsc
from include.airflow.operators.email_to_smb import EmailToSMBOperator
from include.airflow.operators.excel_smb import ExcelSMBToS3BatchOperator, SheetConfig
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.airflow.operators.snowflake_load import SnowflakeInsertOperator
from include.airflow.operators.snowflake_to_mssql import SnowflakeToMsSqlBCPOperator
from include.config import conn_ids, email_lists, owners, s3_buckets, stages
from include.utils.snowflake import Column, CopyConfigCsv

default_args = {
    'start_date': pendulum.datetime(2023, 9, 1, tz='America/Los_Angeles'),
    'retries': 2,
    'owner': owners.data_integrations,
    'email': email_lists.data_integration_support,
    'on_failure_callback': slack_failure_gsc,
}

dag = DAG(
    dag_id='global_apps_inbound_cucb_chargeback',
    default_args=default_args,
    schedule="0 10 * * 6",
    catchup=False,
    max_active_runs=1,
)

sheets = {
    "exception_report_by_create_date": {
        "sheet_config": SheetConfig(
            sheet_name='Report1',
            schema='excel',
            table='cucb_exception_report_by_date',
            header_rows=3,
            column_list=[
                Column("exception_subject", "VARCHAR", source_name="Exception Subject"),
                Column("ref_type", "VARCHAR", source_name="REF_TYPE", uniqueness=True),
                Column("ref_num", "VARCHAR", source_name="REF_NUM", uniqueness=True),
                Column("po", "VARCHAR", source_name="PO#"),
                Column("item", "VARCHAR", source_name="ITEM#"),
                Column("exception_name", "VARCHAR", source_name="EXCEPTION_NAME"),
                Column("excep_detail", "VARCHAR", source_name="EXCEP_DETAIL"),
                Column("status", "VARCHAR", source_name="STATUS"),
                Column("create_by_user_name", "VARCHAR", source_name="CREATE_BY_USER_NAME"),
                Column("create_time", "TIMESTAMP", source_name="CREATE_TIME", delta_column=0),
                Column(
                    "last_update_time", "TIMESTAMP", source_name="LAST_UPDATE_TIME", delta_column=1
                ),
                Column("last_update_from", "VARCHAR", source_name="LAST_UPDATE_FROM"),
                Column("division_name", "VARCHAR", source_name="DIVISION_NAME"),
                Column("last_reply", "VARCHAR", source_name="Last Reply"),
            ],
        ),
        "custom_select": "SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9,"
        "TO_TIMESTAMP_LTZ($10), TO_TIMESTAMP_LTZ($11), $12, $13, $14 "
        "FROM '{files_path}'",
    },
}

with dag:
    to_snowflake_waived_po_data = SnowflakeProcedureOperator(
        procedure='excel.cucb_waived_po_data.sql', database='lake'
    )

    cucb_chargeback_to_mssql = SnowflakeToMsSqlBCPOperator(
        task_id='cucb_chargeback_to_mssql',
        snowflake_database="lake",
        snowflake_schema="excel",
        snowflake_table="waived_po_data",
        mssql_target_table='waived_po_data',
        mssql_target_database='ultrawarehouse',
        mssql_target_schema='rpt',
        mssql_conn_id=conn_ids.MsSql.evolve01_app_airflow_rw,
        unique_columns=[
            'po_number',
            'container',
            'reply_option',
            'create_time',
            'ref_type_number',
            'event_notes_oid',
        ],
        watermark_column='meta_update_datetime',
    )

    for file, sheet_config_details in sheets.items():
        sheet_config = sheet_config_details["sheet_config"]

        to_smb = EmailToSMBOperator(
            task_id=f"{file}_email_to_smb",
            remote_path=f"Inbound/airflow.lightload",
            smb_conn_id=conn_ids.SMB.nas01,
            from_address="@podium",
            resource_address="lightloads@techstyle.com",
            share_name="BI",
            subjects=["TechStyle Lightbox Exception Report by Create Date"],
            file_extensions=["xls", "xlsx"],
        )

        to_s3 = ExcelSMBToS3BatchOperator(
            task_id=f"{file}_smb_to_s3",
            smb_dir=f"Inbound/airflow.lightload",
            share_name="BI",
            file_pattern_list=[f"*.xls*"],
            bucket=s3_buckets.tsos_da_int_inbound,
            s3_conn_id=conn_ids.S3.tsos_da_int_prod,
            smb_conn_id=conn_ids.SMB.nas01,
            is_archive_file=True,
            sheet_configs=[sheet_config],
            remove_header_new_lines=True,
            skip_downstream_if_no_files=True,
        )

        files_path = (
            f'{stages.tsos_da_int_inbound}/inbound/{sheet_config.schema}.{sheet_config.table}/'
            f'{sheet_config.default_schema_version}/'
        )

        to_snowflake = SnowflakeInsertOperator(
            task_id=f"{file}_to_snowflake",
            files_path=files_path,
            database="lake",
            staging_database="lake_stg",
            view_database="lake_view",
            schema=sheet_config.schema,
            table=sheet_config.table,
            column_list=sheet_config.column_list,
            copy_config=CopyConfigCsv(
                field_delimiter="|",
                record_delimiter="\n",
                header_rows=sheet_config.header_rows,
                skip_pct=1,
            ),
            custom_select=sheet_config_details["custom_select"].format(files_path=files_path),
        )

        to_smb >> to_s3 >> to_snowflake >> to_snowflake_waived_po_data >> cucb_chargeback_to_mssql
