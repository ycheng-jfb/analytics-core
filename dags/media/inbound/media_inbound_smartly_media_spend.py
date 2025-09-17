from datetime import timedelta

import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_media, slack_sla_miss_media
from include.airflow.operators.smartly import SmartlyToS3Operator
from include.airflow.operators.snowflake_load import (
    Column,
    SnowflakeIncrementalLoadOperator,
)
from include.config import owners, stages, s3_buckets, conn_ids
from include.config.email_lists import airflow_media_support
from include.utils.snowflake import CopyConfigCsv

table = "media_spend"
schema = "smartly"
table_name = f"{schema}.{table}"
dag_id = f"media_inbound_{schema}_{table}"
stage_name = stages.tsos_da_int_inbound
exec_date = "{{ ds }}"
s3_bucket = s3_buckets.tsos_da_int_inbound
s3_prefix = f"media/{table_name}/v2"
file_name = f"{table_name}_{exec_date}"

column_list = [
    Column("date", "DATE", uniqueness=True),
    Column("ad_id", "VARCHAR", uniqueness=True),
    Column("platform", "VARCHAR"),
    Column("position", "VARCHAR"),
    Column("facebook_objective", "VARCHAR"),
    Column("optimization_goal", "VARCHAR"),
    Column("billing_event", "VARCHAR"),
    Column(
        "genders",
        "VARCHAR",
    ),
    Column("custom_audiences", "VARCHAR"),
    Column("excluded_custom_audiences", "VARCHAR"),
    Column("audience_code", "VARCHAR"),
    Column("interests", "VARCHAR"),
    Column("connections", "VARCHAR"),
    Column(
        "mobile_os",
        "VARCHAR",
    ),
    Column(
        "mobile_device",
        "VARCHAR",
    ),
    Column(
        "creative_type",
        "VARCHAR",
    ),
    Column(
        "post_type",
        "VARCHAR",
    ),
    Column("link_to_post", "VARCHAR"),
    Column("link_to_instagram_post", "VARCHAR"),
    Column("ad_study_id", "VARCHAR"),
    Column("ad_study_name", "VARCHAR"),
    Column("ad_study_cell_id", "VARCHAR"),
    Column(
        "ad_study_cell_name",
        "VARCHAR",
    ),
    Column(
        "base_url",
        "VARCHAR",
    ),
    Column(
        "url_tags",
        "VARCHAR",
    ),
    Column("text", "VARCHAR"),
    Column("behaviors", "VARCHAR"),
    Column("app_destination", "VARCHAR"),
    Column("ad_set_name", "VARCHAR"),
    Column("ad_name", "VARCHAR"),
    Column("campaign_name", "VARCHAR"),
    Column("spent", "DECIMAL(38,8)"),
    Column("s2s_lead", "INT"),
    Column("s2s_lead_vip_24h", "INT"),
    Column("s2s_in_session_vip", "INT"),
]

account_ids = [
    "5bce12b6b5fbbc6c08344303",
    "570e51ad58e7ab5b5c8b456c",
    "570e576058e7abec708b4579",
    "570e58111aa29209758b4567",
    "570e65ec58e7ab89288b4569",
    "570f5edeb9449fc9498b4569",
    "570f668eb9449f795b8b4569",
    "570f66e2b9449fc35c8b4567",
    "5714a93db9449f80028b456f",
    "5717291c58e7abc2658b4567",
    "571de5cf58e7abde048b4567",
    "571de5e758e7ab2c058b4569",
    "571e019f1aa29255018b457d",
    # "5720ad4858e7abec028b456e",
    "572b189d1aa29213618b456d",
    "576a82661aa292e5168b456a",
    "576bf4071aa292d34c8b4575",
    "578fc68a1aa292db478b4568",
    "57b36d1e58e7ab90698b4568",
    "57b36d8058e7ab866c8b4567",
    "57b36d9e58e7abdb6c8b4567",
    "57ed4e961aa292221c8b4567",
    "57ee7efd7bff8539668b4567",
    "57f60c5eb9449f78498b4567",
    "5804ce6d1aa292305a8b4567",
    "5812571e7bff856d088b4567",
    "581257457bff85b20a8b4567",
    "586282647bff850d618b4567",
    "5873fe6e1aa292594e8b4567",
    "58b5631d7bff8575158b4567",
    "58b564bd7bff85df5a8b4567",
    "58fe63c4b5fbbc71aa7b0763",
    "58fe67deb5fbbc1a7f5077e2",
    "591cdfc0c7e28c42fe420b43",
    "59544502b5fbbc66f27c51b2",
    "5a0441bba5ec6c622822e253",
    "5a0441e2a5ec6c6b6d16dfa2",
    "5ade15edf3fd8f5f8d0eda72",
    "5b22314493fa1f15fb14a473",
    "5b2231b993fa1f20244ec673",
    "5b22320393fa1f24c85f7716",
    "5b224561a5ec6c47ac2a8723",
    "5b8fbe75a5ec6c747645b7b6",
    "5b92bd80c7e28c0b8d527d66",
    "5ba1955fb5fbbc1b177aaee2",
    "5baeaea8c7e28c589f788253",
    "5baeaf1ec7e28c5c222546a5",
    "5bff1605f3fd8f47f329b954",
    "5bff192081d8c14b7350e313",
    "5c2d0f8a5474ea6e866ccc03",
    "5c19755793fa1f4e694e24f3",
    "5c2e594081d8c1639e2693a3",
    "5dc34107c478391be94dc273",
    "5dc3410ed2185a0a2570b383",
    "5dcde5bb253cb527cf07db43",
    "5df8bac79a541b30cb25ae32",
    "5e064b9acbf3e53f9064d4c3",
    "5dc6017b6389671e3354aa52",
    "5e151d1e2d3f034dda7718a3",
    "5e151fb9306fc609a45c5a73",
    "5e335e5ba227db2c13072e23",
    "5d0973cae67f1d299a761852",
    "5e29918b2eebe53d9b380043",
    "5e29979236d652140a0e9882",
    "5dfb990704d53d10e72132b3",
    "5e29e2731a754551b270d043",
    "5e29e388a20ccb0faf5a42f3",
    "5e29e345927eaf245f324c43",
    "5e335f18180ab1459111a0d3",
    "5ebd850a0e253be54e437110",
    "5ee967ae2a140db8594e0870",
    "5ee117280ee4d835d8225710",
    "5f36fd878974714298366a20",
    "5e7a4806b5b8476a755a38f3",
    "5f524e20398997efe9690040",
    "5e9775fc4d30644a432027c0",
    "5f6bea78df403f18b132d9d0",
    "5f8a0b16d8a0baea8d1916f0",
    "5f875cf596a0d9bf4174b320",
    "5fb6e259c57bfb38731b5d40",
    # "5fb582b88b9cc65eed647110",
    "5fd188f6a87aca46d97952c0",
    "600f3f6c2699f9b17b3a5c90",
    "606c9f72ea1cccbc57508710",
    "6079fe980a915648fb6193f0",
    "607d95932d70c680655d9f80",
    "609172ac320f96913d370d80",
    "606f1b8c0f1119eb0b42a630",
    "60b9d47df34aa128911bb990",
    "60b8fab56810f04e4f1e0ab0",
    "61bb197d0a121c8ae374f6d0",
    "61bb2405e6f663986d6cd250",
    "61bb23a2068c9340fc2e7e50",
    "623b5ea2874283a0742a2fb0",
    "623a518a5ae76cfb774a6510",
    "63effbf20b8174471f1107b0",
]

config = {
    "stats": "last_7_days",
    "attribution": '{"click":"1d_click","view":false}',
    "metrics": ",".join(
        [
            "spent",
            "custom.58b881557bff85632f8b4567",
            "custom.5995e38eb5fbbc11da0383e5",
            "custom.5914ad0ab5fbbc0296244a83",
        ]
    ),
    "use_distillery": "false",
    "filter_type": "$and",
    "filters": "[]",
    "groupby": ",".join(
        [
            "date",
            "id",
            "targeting.publisher_platforms",
            "targeting.facebook_positions",
            "fb_objective",
            "bid.optimization_goal",
            "bid.billing_event",
            "targeting.genders",
            "targeting.custom_audiences",
            "targeting.excluded_custom_audiences",
            "targeting._code",
            "targeting.interests",
            "targeting.connections",
            "targeting.user_os",
            "targeting.user_device",
            "creative_meta.type",
            "creative_meta.post_type",
            "creative_meta.post_fb_link",
            "creative_meta.instagram_permalink_url",
            "ad_study_cells.ad_study_id",
            "ad_study_cells.ad_study_name",
            "ad_study_cells.ad_study_cell_id",
            "ad_study_cells.ad_study_cell_name",
            "creative_meta.link",
            "creative_meta.url_tags",
            "creative_meta.text",
            "targeting.behaviors",
            "creative_meta.call_to_action_app_destination",
            "adgroup_name",
            "name",
            "campaign_name",
        ]
    ),
    "meta": ",".join(["account_id", "campaign_id"]),
    "format": "csv",
    "csv_col_separator": ",",
    "csv_dec_separator": ".",
}

default_args = {
    "start_date": pendulum.datetime(2019, 4, 3, 7, tz="America/Los_Angeles"),
    "retries": 1,
    "owner": owners.media_analytics,
    "email": airflow_media_support,
    "on_failure_callback": slack_failure_media,
    "sla": timedelta(hours=2),
    "priority_weight": 15,
    "execution_timeout": timedelta(hours=3),
}


dag = DAG(
    dag_id=dag_id,
    default_args=default_args,
    schedule="0 2 * * *",
    catchup=False,
    max_active_tasks=20,
    max_active_runs=1,
    sla_miss_callback=slack_sla_miss_media,
)

with dag:
    to_snowflake = SnowflakeIncrementalLoadOperator(
        task_id="load_to_snowflake",
        table=table,
        database="lake",
        schema="smartly",
        column_list=column_list,
        files_path=f"{stage_name}/{s3_prefix}",
        copy_config=CopyConfigCsv(field_delimiter=",", header_rows=1, skip_pct=5),
    )
    batch_size = 20
    for x in range(0, len(account_ids), batch_size):
        get_data = SmartlyToS3Operator(
            task_id=f"load_to_s3_{x}_{x+batch_size}",
            report_params=config,
            key=f"{s3_prefix}/{file_name}_{x}_{x+batch_size}.csv.gz",
            execution_timeout=timedelta(minutes=10),
            bucket=s3_bucket,
            account_ids=account_ids[x : x + batch_size],
            s3_conn_id=conn_ids.S3.tsos_da_int_prod,
        )
        get_data >> to_snowflake
