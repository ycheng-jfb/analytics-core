from collections import namedtuple
from datetime import timedelta

import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_media_p1
from include.airflow.operators.facebook_export import (
    SegmentFacebookConversionsExportOperator,
)
from include.config import owners
from include.config.email_lists import airflow_media_support

default_args = {
    "depends_on_past": False,
    "start_date": pendulum.datetime(2020, 12, 17, 7, tz="America/Los_Angeles"),
    "retries": 1,
    "owner": owners.media_analytics,
    "email": airflow_media_support,
    "email_on_failure": True,
    "email_on_retry": True,
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(hours=1),
}

Config = namedtuple(
    "Config",
    ["store", "country", "store_in_schema", "store_in_table", "pixel_id", "region"],
)
items = [
    Config("SX", ["'US'"], "SXF", "SXF", "528161084251376", "NA"),
    Config("FL", ["'US'"], "FL", "FABLETICS", "218843068308551", "NA"),
    Config("FL", ["'CA'"], "FL", "FABLETICS", "537344443131231", "NA"),
    Config("YTY", ["'US'"], "FL", "FABLETICS", "715289052842650", "NA"),
    Config("JF", ["'US'"], "GFB", "JUSTFAB", "686991808018907", "NA"),
    # Config('JF', ["'CA'"], 'GFB', 'JUSTFAB', '265813750504874', 'NA'),
    Config("SD", ["'US'"], "GFB", "SHOEDAZZLE", "279505792226308", "NA"),
    Config("FK", ["'US'"], "GFB", "FABKIDS", "212920452242866", "NA"),
    Config(
        "FL",
        ["'UK'", "'SE'", "'NL'", "'FR'", "'ES'", "'DK'", "'DE'"],
        "FL",
        "FABLETICS",
        "2194587504139935",
        "EU",
    ),
    Config(
        "JF",
        ["'UK'", "'SE'", "'NL'", "'FR'", "'ES'", "'DK'", "'DE'"],
        "GFB",
        "JUSTFAB",
        "741318252734475",
        "EU",
    ),
    Config(
        "SX",
        ["'UK'", "'EU'", "'FR'", "'ES'", "'DE'"],
        "SXF",
        "SXF",
        "1653034964810714",
        "EU",
    ),
]

dag = DAG(
    dag_id="media_outbound_segment_facebook_conversions",
    default_args=default_args,
    schedule=None,
    catchup=False,
    max_active_tasks=1,
    max_active_runs=1,
    on_failure_callback=[slack_failure_media_p1],
)


with dag:
    for item in items:
        upload_task = SegmentFacebookConversionsExportOperator(
            task_id=f"upload_{item.pixel_id}_to_facebook",
            initial_load_value="2022-10-03 09:00:00 +00:00",
            watermark_tables=["reporting_media_base_prod.dbo.conversions"],
            pixel_id=item.pixel_id,
            store=item.store,
            country=item.country,
        )
