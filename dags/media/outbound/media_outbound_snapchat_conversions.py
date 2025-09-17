from collections import namedtuple
from datetime import timedelta

import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_media_p1
from include.airflow.operators.snapchat import SnapchatConversionsExportOperator
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
    "on_failure_callback": slack_failure_media_p1,
    "execution_timeout": timedelta(hours=1),
}

Config = namedtuple('Config', ['store', 'country', 'store_in_schema', 'store_in_table', 'pixel_id'])

items = [
    Config('FL', 'US', 'FL', 'FABLETICS', 'b83b6a19-6c28-4c65-9bab-fd54803e895e'),
    Config('YTY', 'US', 'FL', 'FABLETICS', '03de62a5-5c0b-45a7-9585-9d41924a8804'),
    Config('FK', 'US', 'FK', 'FABKIDS', 'df25cb39-19b9-47fb-89c7-ce526c9d5bde'),
    Config('JF', 'US', 'JF', 'JUSTFAB', '1759d9e1-1b3b-4419-9d21-9ef522ef0fd1'),
]

dag = DAG(
    dag_id="media_outbound_snapchat_conversions",
    default_args=default_args,
    schedule=None,
    catchup=False,
    max_active_tasks=1,
    max_active_runs=1,
)


with dag:
    for item in items:
        upload_to_snapchat = SnapchatConversionsExportOperator(
            task_id=f'upload_{item.store}_data_to_snapchat',
            pixel_code=item.pixel_id,
            store=item.store + item.country,
            initial_load_value="2022-11-01 00:00:00 +00:00",
            watermark_tables=['reporting_media_base_prod.dbo.conversions'],
        )
