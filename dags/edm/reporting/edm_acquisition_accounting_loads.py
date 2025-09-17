import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.mssql import MsSqlToS3Operator
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.airflow.operators.snowflake_load import (
    Column,
    CopyConfigCsv,
    SnowflakeTruncateAndLoadOperator,
)
from include.config import conn_ids, owners, s3_buckets, stages
from include.config.email_lists import data_integration_support
from include.utils.acquisition.table_config import TableConfig

stage_name = stages.tsos_da_int_inbound
bucket_name = s3_buckets.tsos_da_int_inbound
date_param = "{{ data_interval_start.strftime('%Y%m%dT%H%M%S%f')[0:-3] }}"

default_args = {
    "start_date": pendulum.datetime(2022, 1, 28, 7, tz="America/Los_Angeles"),
    "retries": 1,
    "owner": owners.central_analytics,
    "email": data_integration_support,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id="edm_acquisition_accounting_loads",
    default_args=default_args,
    schedule=None,
    catchup=False,
    max_active_runs=1,
)

table_config = [
    TableConfig(
        database="ultramerchant",
        schema="dbo",
        table="vw_membership_token_snapshot",
        column_list=[
            Column("date", "DATE"),
            Column("membership_token_id", "INT"),
            Column("amount", "NUMBER(19, 4)"),
            Column("balance", "NUMBER(19, 4)"),
            Column("accounting_balance", "NUMBER(19, 4)"),
            Column("statuscode", "INT"),
        ],
    ),
    TableConfig(
        database="ultramerchant",
        schema="dbo",
        table="vw_store_credit_snapshot",
        column_list=[
            Column("date", "DATE"),
            Column("store_credit_id", "INT"),
            Column("amount", "NUMBER(19, 4)"),
            Column("balance", "NUMBER(19, 4)"),
            Column("accounting_balance", "NUMBER(19, 4)"),
            Column("statuscode", "INT"),
        ],
    ),
]

brand_database_conn_mapping = {
    "JFB": {"ultramerchant": conn_ids.MsSql.justfab_app_airflow},
    "FL": {"ultramerchant": conn_ids.MsSql.fabletics_app_airflow},
    "SXF": {"ultramerchant": conn_ids.MsSql.savagex_app_airflow},
}

with dag:
    cash_and_non_cash_credit_recon = SnowflakeProcedureOperator(
        procedure="shared.oracle_edw_reconciliation.sql",
        database="reporting_base_prod",
    )

    oracle_files_ingestion = SnowflakeProcedureOperator(
        procedure="shared.oracle_files_ingestion.sql",
        database="reporting_base_prod",
    )

    oracle_files_ingestion >> cash_and_non_cash_credit_recon

    for cfg in table_config:
        to_snowflake = SnowflakeTruncateAndLoadOperator(
            task_id=f"{cfg.table}_to_snowflake",
            database="reporting_prod",
            schema=cfg.schema,
            table=cfg.table.replace("vw_", ""),
            files_path=f"{stage_name}/lake/{cfg.schema}.{cfg.table}/v3",
            column_list=cfg.column_list,
            copy_config=CopyConfigCsv(field_delimiter="\t"),
        )

        for brand, database_connection_map in brand_database_conn_mapping.items():
            mssql_conn_id = database_connection_map[cfg.database.lower()]

            to_s3 = MsSqlToS3Operator(
                task_id=f"{cfg.table}_{brand}_to_s3",
                sql=f"SELECT * FROM {cfg.database}.{cfg.schema}.{cfg.table}",
                bucket=s3_buckets.tsos_da_int_inbound,
                key=f"lake/{cfg.schema}.{cfg.table}/v3/{cfg.table}_{brand}_{date_param}.csv.gz",
                mssql_conn_id=mssql_conn_id,
                s3_conn_id=conn_ids.S3.tsos_da_int_prod,
                s3_replace=True,
            )

            to_s3 >> to_snowflake >> cash_and_non_cash_credit_recon
