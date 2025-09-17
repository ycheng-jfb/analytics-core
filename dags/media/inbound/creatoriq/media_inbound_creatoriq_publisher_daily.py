import xml.etree.ElementTree as ET
from typing import List

import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_media
from include.airflow.operators.creatoriq import CreatorIQToS3Operator
from include.airflow.operators.snowflake_load import Column, SnowflakeScdOperator
from include.config import owners, s3_buckets, stages, conn_ids
from include.config.email_lists import airflow_media_support
from include.utils.snowflake import CopyConfigCsv

TABLE = "publisher"
SCHEMA = "creatoriq"

S3_PREFIX = "media/creatoriq/publisher/v2"
date_param = "{{ data_interval_start.strftime('%Y%m%dT%H%M%S%f')[0:-3] }}"
column_list = [
    Column("NetworkPublisherId", "INT", uniqueness=True),
    Column("PublisherId", "INT", uniqueness=True),
    Column("LogoURL", "VARCHAR", type_2=True),
    Column("ThumbLogoURL", "VARCHAR", type_2=True),
    Column("PublisherName", "VARCHAR", type_2=True),
    Column("FlagshipProperty", "VARCHAR", type_2=True),
    Column("FlagshipPropertyTitle", "VARCHAR", type_2=True),
    Column("FlagshipSocialNetwork", "VARCHAR", type_2=True),
    Column("Description", "VARCHAR", type_2=True),
    Column("Categories", "VARCHAR", type_2=True),
    Column("Size", "VARCHAR", type_2=True),
    Column("YT_Subscribers", "INT", type_2=True),
    Column("YT_AverageMonthlyViews", "NUMBER(18,6)", type_2=True),
    Column("AccountManagerName", "VARCHAR", type_2=True),
    Column("DateLastContact", "DATETIME", type_2=True),
    Column("Email", "VARCHAR", type_2=True),
    Column("PhoneNumber1", "VARCHAR", type_2=True),
    Column("StreetAddress", "VARCHAR", type_2=True),
    Column("City", "VARCHAR", type_2=True),
    Column("State", "VARCHAR", type_2=True),
    Column("ZipCode", "INT", type_2=True),
    Column("Country", "VARCHAR", type_2=True),
    Column("FB_Likes", "INT", type_2=True),
    Column("TW_Followers", "INT", type_2=True),
    Column("IN_Followers", "INT", type_2=True),
    Column("VN_Followers", "INT", type_2=True),
    Column("GP_Followers", "INT", type_2=True),
    Column("TB_Followers", "INT", type_2=True),
    Column("PN_Followers", "INT", type_2=True),
    Column("SC_Followers", "INT", type_2=True),
    Column("WB_Followers", "INT", type_2=True),
    Column("NumberOfChannels", "INT", type_2=True),
    Column("FB_NumOfAccounts", "INT", type_2=True),
    Column("IN_NumOfAccounts", "INT", type_2=True),
    Column("TW_NumOfAccounts", "INT", type_2=True),
    Column("GP_NumOfAccounts", "INT", type_2=True),
    Column("VN_NumOfAccounts", "INT", type_2=True),
    Column("PN_NumOfAccounts", "INT", type_2=True),
    Column("WB_NumOfAccounts", "INT", type_2=True),
    Column("TB_NumOfAccounts", "INT", type_2=True),
    Column("SC_NumOfAccounts", "INT", type_2=True),
    Column("AvgLastEngagementPerVideoYT", "NUMBER(18,6)", type_2=True),
    Column("AvgLastEngagementPerVideoSocial", "NUMBER(18,6)", type_2=True),
    Column("AvgLastEngagementPerVideoTotal", "NUMBER(18,6)", type_2=True),
    Column("Sentiment", "NUMBER(16,8)", type_2=True),
    Column("TotalSubscribers", "INT", type_2=True),
    Column("FlagshipEngagement", "NUMBER(18,6)", type_2=True),
    Column("Network", "VARCHAR", uniqueness=True),
    Column("SocialNetworkId", "VARCHAR", type_2=True),
    Column("NetworkUser", "VARCHAR", type_2=True),
    Column("AccountWeight", "INT", type_2=True),
    Column("WebReach", "INT", type_2=True),
    Column("Title", "VARCHAR", type_2=True),
    Column("Followers", "INT", type_2=True),
    Column("LinkedBy", "VARCHAR", type_2=True),
    Column("LinkedAt", "VARCHAR", type_2=True),
    Column("isVerifiedBySocialNetwork", "INT", type_2=True),
    Column("Linked", "INT", type_2=True),
    Column("Unlinked", "INT", type_2=True),
    Column("LinkBroken", "INT", type_2=True),
    Column("Business", "INT", type_2=True),
    Column("TokenInvalidDate", "DATETIME", type_2=True),
    Column("BoostableTotal", "INT", type_2=True),
    Column("DateSocialStatsUpdated", "TIMESTAMP_LTZ", type_1=True, delta_column=True),
]

DEFAULT_ARGS = {
    "start_date": pendulum.datetime(2020, 11, 13, 0, tz="America/Los_Angeles"),
    "retries": 1,
    "owner": owners.media_analytics,
    "email": airflow_media_support,
    "on_failure_callback": slack_failure_media,
}

dag = DAG(
    dag_id=f"media_inbound_{SCHEMA}_{TABLE}_daily",
    default_args=DEFAULT_ARGS,
    schedule="0 10 * * *",
    catchup=False,
    max_active_tasks=20,
    max_active_runs=1,
)


class CreatorIQPullPublishersToS3Operator(CreatorIQToS3Operator):
    """
    This operator expends CreatorIQToS3Operator to override get_rows method
    This operator pulls all the publishers from view endpoint.
    returned Json conatins XML for SocialAccountsList column which is parsed to pull multiple rows from it.
    """

    def parse_xml(self, xml_str: str, xml_cols: List[str]):
        """

        Args:
            xml_str: string which contains xml data
            xml_cols: columns that needs to be pulled from xml data

        Returns:
            a dict with specified columns

        """
        tree = ET.fromstring("<root>" + (xml_str or "") + "</root>")
        for row in tree:
            yield {x.tag: x.text for x in row if x.tag in xml_cols}

    def get_rows(self):
        xml_cols = [
            "Network",
            "SocialNetworkId",
            "NetworkUser",
            "AccountWeight",
            "WebReach",
            "Title",
            "Followers",
            "LinkedBy",
            "LinkedAt",
            "isVerifiedBySocialNetwork",
            "Linked",
            "Unlinked",
            "LinkBroken",
            "Business",
            "TokenInvalidDate",
            "BoostableTotal",
            "DateSocialStatsUpdated",
        ]

        params = {
            "view": "PublishersSocial/Summary",
            "requestData[take]": 1000,
            "requestData[skip]": 0,
            "requestData[sort][0][field]": "PublisherName",
            "requestData[sort][0][dir]": "asc",
        }
        while True:
            response = self.hook.make_request(
                url="https://api.creatoriq.com/rest/view/data", params=params
            )
            result = response.json()["results"]
            for record in result:
                xml_str = record.pop("SocialAccountsList")
                for row in self.parse_xml(xml_str, xml_cols):
                    yield self.merge_dicts(record, row)
            if len(result) < params["requestData[take]"]:
                break
            else:
                print(
                    f"Pulled:{params['requestData[take]']} and skipped:{params['requestData[skip]']}"
                )
                params["requestData[skip]"] += params["requestData[take]"]


with dag:
    to_snowflake = SnowflakeScdOperator(
        task_id="load_to_snowflake",
        database="LAKE",
        schema=SCHEMA,
        table=TABLE,
        column_list=column_list,
        files_path=f"{stages.tsos_da_int_inbound}/{S3_PREFIX}",
        copy_config=CopyConfigCsv(field_delimiter="\t", header_rows=0, skip_pct=1),
    )
    op = CreatorIQPullPublishersToS3Operator(
        task_id="get_ciq_data",
        bucket=s3_buckets.tsos_da_int_inbound,
        key=f"{S3_PREFIX}/{SCHEMA}_{TABLE}_{date_param}.tsv.gz",
        column_list=[x.name for x in column_list],
        s3_conn_id=conn_ids.S3.tsos_da_int_prod,
    )

    op >> to_snowflake
