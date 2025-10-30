from collections import namedtuple
from datetime import timedelta

import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_media_p1
from include.airflow.operators.google_export import GoogleConversionsExportOperator
from include.config import owners
from include.config.email_lists import airflow_media_support

default_args = {
    "depends_on_past": False,
    "start_date": pendulum.datetime(2020, 12, 17, 7, tz="America/Los_Angeles"),
    "retries": 1,
    'owner': owners.media_analytics,
    "email": airflow_media_support,
    "email_on_failure": True,
    "email_on_retry": True,
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(hours=1),
}

Config = namedtuple('Config', ['customer_id', 'conversion_action_id', 'store', 'country'])
stores = [
    Config('6680131322', '6513560615', 'FL', 'US'),
    Config('8923721765', '6513334200', 'FL', 'US'),
]

dag = DAG(
    dag_id="media_outbound_google_offline_conversions",
    default_args=default_args,
    schedule="0 10 * * 2",
    catchup=False,
    max_active_tasks=1,
    max_active_runs=1,
    on_failure_callback=[slack_failure_media_p1],
)


with dag:
    for store in stores:
        post_data = GoogleConversionsExportOperator(
            task_id=f"upload_to_google_{store.customer_id}",
            customer_id=store.customer_id,
            conversion_action_id=store.conversion_action_id,
            store=store.store,
            country=store.country,
            initial_load_value="2025-02-04",
            watermark_tables=['edw_prod.data_model.fact_order'],
        )
