from pathlib import Path

import pendulum
from airflow.models import DAG
from include.config import owners
from include.config.email_lists import data_integration_support
from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.snowflake import TableDependencyTzLtz, SnowflakeProcedureOperator

default_args = {
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024, 6, 20, 0, tz='America/Los_Angeles'),
    'retries': 1,
    'owner': owners.data_integrations,
    'email': data_integration_support,
    'on_failure_callback': slack_failure_edm,
}

dag = DAG(
    dag_id="edm_outbound_iterable_orphan_contacts_feed",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    schedule='0 1 * * *',
)

with dag:
    orphan_contacts = SnowflakeProcedureOperator(
        procedure='public.orphan_contacts.sql',
        database='iterable',
        watermark_tables=[
            TableDependencyTzLtz(
                table_name='campaign_event_data.org_3223.users',
                column_name='profile_updated_at',
            )
        ],
    )
