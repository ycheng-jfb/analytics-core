from collections import namedtuple

import pendulum
from airflow.models import DAG
from datetime import timedelta

from include.airflow.callbacks.slack import slack_failure_media_p1
from include.airflow.operators.facebook_export import FacebookOfflineConversionsExportOperator
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
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
    "execution_timeout": timedelta(hours=2),
}

Config = namedtuple('Config', ['store', 'country', 'pixel_id'])
items = [
    Config('SX', ["'US'"], '528161084251376'),
    Config('FL', ["'US'"], '218843068308551'),
    # Config('FL', ["'CA'"], '537344443131231'),
    # Config('YTY', ["'US'"], '715289052842650'),
    # Config('JF', ["'US'"], '686991808018907'),
    # Config('JF', ["'CA'"], '265813750504874'),
    # Config('SD', ["'US'"], '279505792226308'),
    # Config('FK', ["'US'"], '212920452242866'),
    # Config(
    #     'FL',
    #     ["'UK'", "'SE'", "'NL'", "'FR'", "'ES'", "'DK'", "'DE'"],
    #     '2194587504139935',
    # ),
    # Config(
    #     'JF',
    #     ["'UK'", "'SE'", "'NL'", "'FR'", "'ES'", "'DK'", "'DE'"],
    #     '741318252734475',
    # ),
    # Config('SX', ["'UK'", "'EU'", "'FR'", "'ES'", "'DE'"], '1653034964810714'),
]

dag = DAG(
    dag_id="media_outbound_facebook_offline_conversions",
    default_args=default_args,
    schedule="15 3,11,19 * * *",
    catchup=False,
    max_active_tasks=1,
    max_active_runs=1,
)


with dag:
    sql_task = SnowflakeProcedureOperator(
        procedure='dbo.facebook_offline_conversion.sql',
        database='reporting_media_prod',
        initial_load_value="2023-07-01 00:00:00 +00:00",
        watermark_tables=['edw_prod.data_model.fact_order'],
    )

    for item in items:
        upload_task = FacebookOfflineConversionsExportOperator(
            task_id=f"upload_{item.pixel_id}_to_facebook",
            initial_load_value="2023-07-01 09:00:00 +00:00",
            watermark_tables=['reporting_media_prod.dbo.facebook_offline_conversion'],
            pixel_id=item.pixel_id,
            store=item.store,
            country=item.country,
        )
        sql_task >> upload_task
