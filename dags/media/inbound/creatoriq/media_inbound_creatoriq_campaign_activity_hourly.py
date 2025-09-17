import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_media
from include.airflow.operators.creatoriq import CreatorIQToS3Operator
from include.airflow.operators.snowflake_load import Column, CopyConfigCsv, SnowflakeScdOperator
from include.config import owners, s3_buckets, stages
from include.config.email_lists import airflow_media_support
from include.config import conn_ids

table = "campaign_activity"
schema = "creatoriq"
date_param = "{{ data_interval_start.strftime('%Y%m%dT%H%M%S%f')[0:-3] }}"
S3_PREFIX = "media/creatoriq/campaign/campaign_activity/v2"

stage_name = stages.tsos_da_int_inbound
bucket_name = s3_buckets.tsos_da_int_inbound

column_list = [
    Column("Id", "INT", uniqueness=True),
    Column("CampaignId", "INT", uniqueness=True),
    Column("SocialNetwork", "VARCHAR", type_2=True),
    Column("SocialNetworkId", "VARCHAR", type_2=True),
    Column("PublisherId", "INT", uniqueness=True),
    Column("UserName", "VARCHAR", type_2=True),
    Column("Type", "VARCHAR", type_2=True),
    Column("PostId", "VARCHAR", type_2=True),
    Column("Score", "INT", type_2=True),
    Column("EarnedMedia", "NUMBER(19, 4)", type_2=True),
    Column("ActualReach", "VARCHAR", type_2=True),
    Column("NetworkReach", "VARCHAR", type_2=True),
    Column("EstimatedReach", "NUMBER(19, 4)", type_2=True),
    Column("Reach", "NUMBER(19, 4)", type_2=True),
    Column("ExtendedReach", "VARCHAR", type_2=True),
    Column("Likes", "INT", type_2=True),
    Column("Comments", "INT", type_2=True),
    Column("Retweets", "INT", type_2=True),
    Column("Views", "INT", type_2=True),
    Column("MinutesWatched", "NUMBER(19, 4)", type_2=True),
    Column("PostKey", "VARCHAR", type_2=True),
    Column("LinkId", "VARCHAR", type_2=True),
    Column("Status", "VARCHAR", type_2=True),
    Column("ApprovedBy", "VARCHAR", type_2=True),
    Column("SuspendedBy", "VARCHAR", type_2=True),
    Column("UpdatedBy", "VARCHAR", type_2=True),
    Column("PostType", "VARCHAR", type_2=True),
    Column("TotalShares", "VARCHAR", type_2=True),
    Column("FacebookLikes", "VARCHAR", type_2=True),
    Column("FacebookShares", "VARCHAR", type_2=True),
    Column("FacebookComments", "VARCHAR", type_2=True),
    Column("TwitterShares", "VARCHAR", type_2=True),
    Column("GooglePlusShares", "VARCHAR", type_2=True),
    Column("PinterestShares", "VARCHAR", type_2=True),
    Column("NotFound", "VARCHAR", type_2=True),
    Column("PublisherName", "VARCHAR", type_2=True),
    Column("CampaignName", "VARCHAR", type_2=True),
    Column("NetworkPublisherId", "INT", type_2=True),
    Column("PartnerId", "INT", type_2=True),
    Column("LogoURL", "VARCHAR", type_2=True),
    Column("StartDate", "DATE", type_2=True),
    Column("EndDate", "DATE", type_2=True),
    Column("FlagshipProperty", "VARCHAR", type_2=True),
    Column("AverageViewDuration", "VARCHAR", type_2=True),
    Column("AverageViewPercentage", "VARCHAR", type_2=True),
    Column("SubscribersGained", "VARCHAR", type_2=True),
    Column("AvgViewsPerPost", "VARCHAR", type_2=True),
    Column("SubscribersLost", "VARCHAR", type_2=True),
    Column("AnnotationClickThroughRate", "VARCHAR", type_2=True),
    Column("AnnotationCloseRate", "VARCHAR", type_2=True),
    Column("MonetizedPlaybacks", "VARCHAR", type_2=True),
    Column("GrossRevenue", "VARCHAR", type_2=True),
    Column("ImpressionBasedCpm", "VARCHAR", type_2=True),
    Column("Duration", "VARCHAR", type_2=True),
    Column("ActualImpressions", "VARCHAR", type_2=True),
    Column("EngagementRate", "VARCHAR", type_2=True),
    Column("PaidImpressions", "VARCHAR", type_2=True),
    Column("PaidReach", "VARCHAR", type_2=True),
    Column("PaidEngagement", "VARCHAR", type_2=True),
    Column("PaidEngagementRate", "VARCHAR", type_2=True),
    Column("PaidClicks", "VARCHAR", type_2=True),
    Column("PaidViews", "VARCHAR", type_2=True),
    Column("PaidLikes", "VARCHAR", type_2=True),
    Column("PaidComments", "VARCHAR", type_2=True),
    Column("PaidTotalShares", "VARCHAR", type_2=True),
    Column("PaidSaves", "VARCHAR", type_2=True),
    Column("CombinedImpressions", "VARCHAR", type_2=True),
    Column("CombinedReach", "VARCHAR", type_2=True),
    Column("CombinedEngagement", "VARCHAR", type_2=True),
    Column("CombinedClicks", "VARCHAR", type_2=True),
    Column("CombinedViews", "VARCHAR", type_2=True),
    Column("CombinedLikes", "VARCHAR", type_2=True),
    Column("CombinedComments", "VARCHAR", type_2=True),
    Column("CombinedTotalShares", "VARCHAR", type_2=True),
    Column("CombinedSaves", "VARCHAR", type_2=True),
    Column("CombinedEngagementRate", "VARCHAR", type_2=True),
    Column("NetworkImpressions", "VARCHAR", type_2=True),
    Column("EstimatedImpressions", "NUMBER(19, 4)", type_2=True),
    Column("FollowersAtTimeOfPost", "INT", type_2=True),
    Column("FBIGId", "INT", type_2=True),
    Column("Thumbnail", "VARCHAR", type_2=True),
    Column("SavedPicture", "VARCHAR", type_2=True),
    Column("Replies", "VARCHAR", type_2=True),
    Column("Exits", "VARCHAR", type_2=True),
    Column("TapsForward", "VARCHAR", type_2=True),
    Column("TapsBack", "VARCHAR", type_2=True),
    Column("SwipeAways", "VARCHAR", type_2=True),
    Column("PostTitle", "VARCHAR", type_2=True),
    Column("Uniques", "VARCHAR", type_2=True),
    Column("Note", "VARCHAR", type_2=True),
    Column("FTCCompliant", "VARCHAR", type_2=True),
    Column("SwipeUps", "VARCHAR", type_2=True),
    Column("StickerTaps", "VARCHAR", type_2=True),
    Column("Sentiment", "VARCHAR", type_2=True),
    Column("ManualReachFlag", "INT", type_2=True),
    Column("ManualImpressionsFlag", "INT", type_2=True),
    Column("SocialId", "INT", type_2=True),
    Column("Dislikes", "VARCHAR", type_2=True),
    Column("Saved", "VARCHAR", type_2=True),
    Column("Linked", "INT", type_2=True),
    Column("Unlinked", "INT", type_2=True),
    Column("LinkBroken", "INT", type_2=True),
    Column("Verified", "INT", type_2=True),
    Column("TokenInvalidDate", "VARCHAR", type_2=True),
    Column("LinkedAt", "VARCHAR", type_2=True),
    Column("Business", "INT", type_2=True),
    Column("SubmitByDate", "VARCHAR", type_2=True),
    Column("NetworkEngagements", "VARCHAR", type_2=True),
    Column("InstagramShares", "VARCHAR", type_2=True),
    Column("ProfileVisits", "VARCHAR", type_2=True),
    Column("Follows", "VARCHAR", type_2=True),
    Column("CampaignStatus", "VARCHAR", type_2=True),
    Column("BoardId", "VARCHAR", type_2=True),
    Column("BoardUrl", "VARCHAR", type_2=True),
    Column("BoardsCount", "VARCHAR", type_2=True),
    Column("Saves", "VARCHAR", type_2=True),
    Column("isVerifiedBySocialNetwork", "VARCHAR", type_2=True),
    Column("DateSubmitted", "VARCHAR", type_2=True),
    Column("DateApproved", "VARCHAR", type_2=True),
    Column("DateSuspended", "VARCHAR", type_2=True),
    Column("PostLink", "VARCHAR", type_2=True),
    Column("Post", "VARCHAR", type_2=True),
    Column("LastUpdated", "TIMESTAMP_LTZ", type_1=True, delta_column=True),
    Column("UserNameTitle", "VARCHAR", type_2=True),
    Column("Engagement", "INT", type_2=True),
    Column("Clicks", "VARCHAR", type_2=True),
    Column("PostClicks", "VARCHAR", type_2=True),
    Column("TotalReach", "NUMBER(19, 4)", type_2=True),
    Column("LinkClicks", "VARCHAR", type_2=True),
    Column("PostConversionMetrics", "VARCHAR", type_2=True),
    Column("Impressions", "NUMBER(19, 4)", type_2=True),
    Column("SocialMediaValue", "NUMBER(19, 4)", type_2=True),
    Column("EstimatedReachFlag", "INT", type_2=True),
    Column("EstimatedImpressionsFlag", "INT", type_2=True),
    Column("BaseMetric", "VARCHAR", type_2=True),
    Column("BaseMetricValue", "NUMBER(19, 4)", type_2=True),
    Column("CloseUps", "VARCHAR", type_2=True),
    Column("PeakStreamConcurrentViews", "VARCHAR", type_2=True),
    Column("AvgStreamConcurrentViews", "VARCHAR", type_2=True),
    Column("SubscribersAtTimeOfPost", "VARCHAR", type_2=True),
    Column("EstimatedUniqueStreamViews", "VARCHAR", type_2=True),
    Column("Votes", "VARCHAR", type_2=True),
    Column("RequirementLinked", "INT", type_2=True),
]

default_args = {
    "start_date": pendulum.datetime(2020, 11, 13, 7, tz="America/Los_Angeles"),
    "retries": 1,
    'owner': owners.media_analytics,
    "email": airflow_media_support,
    "on_failure_callback": slack_failure_media,
}

dag = DAG(
    dag_id=f"media_inbound_{schema}_{table}_hourly",
    default_args=default_args,
    schedule="50 * * * *",
    catchup=False,
    max_active_tasks=20,
    max_active_runs=1,
)

sql_cmd = (
    "select distinct campaignid from LAKE.CREATORIQ.CAMPAIGN_LIST "
    "where DATEDIFF(day,ENDDATE,CURRENT_TIMESTAMP()) <= 8"
)


class CreatorIQPullCampaignActivityToS3Operator(CreatorIQToS3Operator):
    """
    This operator expends CreatorIQToS3Operator to override get_rows method
    This operator pulls campaign activity for all the active campaigns
    """

    def __init__(
        self,
        sql_cmd: str,
        snowflake_conn_id: str = conn_ids.Snowflake.default,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.snowflake_conn_id = snowflake_conn_id
        self.sql_cmd = sql_cmd

    def get_rows(self):
        ids = self.get_sql_data()
        for id in ids:
            response = self.hook.make_request(
                url=f'https://api.creatoriq.com/api/public/campaign/{id[0]}/activity'
            )
            data = response.json()['CampaignActivity']['items']
            yield from data
            print(f"pulled for id:{id[0]} fetched {len(data)} records")
        print("completed fetching data")


with dag:
    to_snowflake = SnowflakeScdOperator(
        task_id="load_to_snowflake",
        database="LAKE",
        schema=schema,
        table=table,
        column_list=column_list,
        files_path=f"{stages.tsos_da_int_inbound}/{S3_PREFIX}",
        copy_config=CopyConfigCsv(field_delimiter='\t', header_rows=0),
    )
    op = CreatorIQPullCampaignActivityToS3Operator(
        task_id="get_ciq_data",
        sql_cmd=sql_cmd,
        bucket=s3_buckets.tsos_da_int_inbound,
        key=f"{S3_PREFIX}/{schema}_{table}_{date_param}.tsv.gz",
        column_list=[x.source_name for x in column_list],
        s3_conn_id=conn_ids.S3.tsos_da_int_prod,
    )
    op >> to_snowflake
