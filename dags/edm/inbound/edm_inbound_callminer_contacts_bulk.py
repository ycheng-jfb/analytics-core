from datetime import datetime, timedelta
from dateutil import parser

import pendulum
from airflow.decorators import task
from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.models.process_state import ProcessState
from include.airflow.operators.callminer_contacts_bulk import CallMinerBulkContacts
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.airflow.operators.snowflake_load import SnowflakeIncrementalLoadOperator
from include.config import conn_ids, email_lists, owners, s3_buckets, snowflake_roles, stages
from include.utils.snowflake import CopyConfigCsv
from task_configs.dag_config.callminer_config import contacts_bulk_config

from airflow import DAG


default_args = {
    "start_date": pendulum.datetime(2022, 10, 31, tz="America/Los_Angeles"),
    "owner": owners.data_integrations,
    "email": email_lists.data_integration_support,
    'on_failure_callback': slack_failure_edm,
}

dag = DAG(
    dag_id="edm_inbound_callminer_contacts_bulk",
    default_args=default_args,
    schedule="0 5 * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
)

yr_mth = "{{macros.datetime.now().strftime('%Y%m')}}"
with dag:

    @task
    def check_pending_jobs(config_val, initial_load_value):
        job_status = CallMinerBulkContacts.check_job_status(
            config_val.table, 'callminer_bulk_default'
        )

        task_state_val = ProcessState.get_value(
            namespace=config_val.schema, process_name=config_val.table
        )
        low_watermark = task_state_val or initial_load_value
        new_data = list(
            filter(
                lambda x: x["Status"] == "Completed" and x["JobCompletionTime"] > low_watermark,
                job_status.json(),
            )
        )

        return [
            {
                "s3_key": (
                    f'{config_val.s3_prefix}/'
                    f'{job_completion_time.strftime("%Y%m")}/'
                    f'{config_val.schema}_{config_val.table}_'
                    f'{job_completion_time.strftime("%Y%m%dT%H%M%S")}.csv.gz'
                )
            }
            for job in new_data
            if (job_completion_time := parser.isoparse(job["JobCompletionTime"]))
        ]

    to_snowflake_list = []
    for config in contacts_bulk_config:
        to_s3 = CallMinerBulkContacts.partial(
            task_id=f'{config.table}_to_s3',
            s3_bucket=s3_buckets.tsos_da_int_inbound,
            s3_conn_id=conn_ids.S3.tsos_da_int_prod,
            callminer_bulk_conn_id='callminer_bulk_default',
            namespace=config.schema,
            process_name=config.table,
            initial_load_value="1900-01-01T00:00:00.000Z",
            max_active_tis_per_dagrun=1,
        ).expand_kwargs(check_pending_jobs(config, "1900-01-01T00:00:00.000Z"))

        if config.table == 'contacts_new':
            custom_select = f"""SELECT $1,$2,$73,$264,$16,$60,$15,$72,$28,$88,$86,$23,$24,$27,$77,$1,$76,$13,$21,
                                        $12,$136,$61,$73,$32,$34,$44,$94,$91,$54,$11,$33,$1,$1,$31,$89,$93,$90,$30,
                                        $29,$92,$17,$9,$51,$53,$81,$43,$42,$10,$1,$46,$4,$63,$57,$83,$64,$14,$25,$82,
                                        $66,$75,$55,$40,$45,$48,$50,$52,$67,$36,$22,$38,$18,$26,$49,$41,$87,$5,$39,
                                        $19,$58,$7,$85,$3,$74,$59,$20,$8,$56,$35,$62,$37,$47,$65,$6,$84
                                 FROM '{stages.tsos_da_int_inbound}/{config.s3_prefix}/'"""

            to_snowflake = SnowflakeIncrementalLoadOperator(
                task_id=f'{config.table}_to_snowflake',
                database=config.database,
                schema=config.schema,
                table=config.table,
                staging_database='lake_stg',
                snowflake_conn_id=conn_ids.Snowflake.default,
                role=snowflake_roles.etl_service_account,
                column_list=config.column_list,
                files_path=f'{stages.tsos_da_int_inbound}/{config.s3_prefix}/',
                copy_config=CopyConfigCsv(header_rows=1, null_if="'null'"),
                custom_select=custom_select,
            )

        else:

            to_snowflake = SnowflakeIncrementalLoadOperator(
                task_id=f'{config.table}_to_snowflake',
                database=config.database,
                schema=config.schema,
                table=config.table,
                staging_database='lake_stg',
                snowflake_conn_id=conn_ids.Snowflake.default,
                role=snowflake_roles.etl_service_account,
                column_list=config.column_list,
                files_path=f'{stages.tsos_da_int_inbound}/{config.s3_prefix}/',
                copy_config=CopyConfigCsv(header_rows=1, null_if="'null'"),
            )

        to_s3 >> to_snowflake
        to_snowflake_list.append(to_snowflake)

    contact_categories_bulk = SnowflakeProcedureOperator(
        procedure='callminer.contact_categories_bulk.sql',
        database='lake',
        warehouse='DA_WH_ETL_LIGHT',
    )

    contact_scores_bulk = SnowflakeProcedureOperator(
        procedure='callminer.contact_scores_bulk.sql',
        database='lake',
        warehouse='DA_WH_ETL_LIGHT',
    )

    contact_score_components_bulk = SnowflakeProcedureOperator(
        procedure='callminer.contact_score_components_bulk.sql',
        database='lake',
        warehouse='DA_WH_ETL_LIGHT',
    )

    to_snowflake_list >> contact_categories_bulk
    to_snowflake_list >> contact_scores_bulk
    to_snowflake_list >> contact_score_components_bulk
