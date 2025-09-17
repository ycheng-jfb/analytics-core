from collections import namedtuple
from datetime import timedelta

import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_media_p1
from include.airflow.operators.tiktok import TiktokConversionsExportOperator
from include.config.email_lists import airflow_media_support
from include.config import owners

default_args = {
    "start_date": pendulum.datetime(2022, 6, 21, 1, tz="America/Los_Angeles"),
    "email": airflow_media_support,
    "retries": 2,
    "owner": owners.media_analytics,
    "depends_on_past": False,
    "email_on_retry": True,
    "email_on_failure": False,
    "on_failure_callback": slack_failure_media_p1,
    "execution_timeout": timedelta(hours=1),
}

Config = namedtuple(
    "Config",
    [
        "store",
        "store_brand_abbr",
        "country",
        "store_in_schema",
        "store_in_table",
        "pixel_code",
    ],
)
items = [
    Config("SX", "SX", "US", "sxf", "sxf", "BUPLSSQCBTHKOOVLIDRG"),
    Config("YTY", "YTY", "US", "fl", "fabletics", "C91KO8R3NCHVEF8P7LG0"),
    Config("FLW", "FL", "US", "fl", "fabletics", "BVA0EMV154NVE7MOM6LG"),
    Config("FLM", "FL", "US", "fl", "fabletics", "BVGDR27QSG1UAS59TUFG"),
    Config("FK", "FK", "US", "gfb", "fabkids", "C70C3GJEQEH3MA3MHDN0"),
    Config("JF", "JF", "US", "gfb", "justfab", "C014CKT2UGFR4ILSOPVG"),
    Config("SD", "SD", "US", "gfb", "shoedazzle", "C1MCUA1T0U322RQQA9H0"),
    Config("FLW", "FL", "UK", "fl", "fabletics", "C79LR9DISHGQ0VHEC21G"),
    Config("FLM", "FL", "UK", "fl", "fabletics", "C79LQN8A2TFP9AP719S0"),
    Config("SX", "SX", "UK", "sxf", "sxf", "C0DBUNL2JJH938MD5AN0"),
    Config("JF", "JF", "UK", "gfb", "justfab", "C7A8GU9R5NMD330C0HUG"),
    Config("FLW", "FL", "FR", "fl", "fabletics", "C3M8U4VB3D4L4OG4PGG0"),
    Config("FLM", "FL", "FR", "fl", "fabletics", "C3M8GINB3D4L4OG4PE90"),
    Config("SX", "SX", "FR", "sxf", "sxf", "C2RUAVS98FMAUGDPF72G"),
    Config("JF", "JF", "FR", "gfb", "justfab", "C205998QDRQBNKO1O0MG"),
    Config("FLW", "FL", "DE", "fl", "fabletics", "C3M854FB3D4L4OG4PC7G"),
    Config("FLM", "FL", "DE", "fl", "fabletics", "C3M80G7B3D4L4OG4PB4G"),
    Config("SX", "SX", "DE", "sxf", "sxf", "C2RUA54APG3ML5LKD5G0"),
    # Config('JF', 'JF', 'DE', 'gfb', 'justfab', ''),
]

dag = DAG(
    dag_id="media_outbound_tiktok_conversions",
    schedule=None,
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
)

with dag:
    for item in items:
        upload_to_tiktok = TiktokConversionsExportOperator(
            task_id=f"upload_{item.store}_{item.country}_data_to_tiktok",
            pixel_code=item.pixel_code,
            store=item.store_brand_abbr + item.country,
            initial_load_value="2022-04-01 00:00:00 +00:00",
            watermark_tables=["reporting_media_base_prod.dbo.conversions"],
        )
