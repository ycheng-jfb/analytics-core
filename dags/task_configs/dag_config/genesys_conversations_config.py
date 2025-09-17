from dataclasses import dataclass

from include.utils.snowflake import Column


@dataclass
class Config:
    database: str
    schema: str
    table: str
    column_list: list


analytics_config = Config(
    database="lake",
    schema="genesys",
    table="genesys_speech_analytics",
    column_list=[
        Column(
            "conversation_id", "VARCHAR", source_name="conversation_id", uniqueness=True
        ),
        Column("conversation_self_uri", "VARCHAR", source_name="conversation_selfUri"),
        Column("sentiment_score", "NUMBER(18,17)", source_name="sentimentScore"),
        Column("sentiment_trend", "NUMBER(18,17)", source_name="sentimentTrend"),
        Column("sentiment_trend_class", "VARCHAR", source_name="sentimentTrendClass"),
        Column(
            "agent_duration_percentage",
            "NUMBER(5,2)",
            source_name="participantMetrics_agentDurationPercentage",
        ),
        Column(
            "customer_duration_percentage",
            "NUMBER(5,2)",
            source_name="participantMetrics_customerDurationPercentage",
        ),
        Column(
            "silence_duration_percentage",
            "NUMBER(5,2)",
            source_name="participantMetrics_silenceDurationPercentage",
        ),
        Column(
            "ivr_duration_percentage",
            "NUMBER(5,2)",
            source_name="participantMetrics_ivrDurationPercentage",
        ),
        Column(
            "acd_duration_percentage",
            "NUMBER(5,2)",
            source_name="participantMetrics_acdDurationPercentage",
        ),
        Column(
            "overtalk_duration_percentage",
            "NUMBER(5,2)",
            source_name="participantMetrics_overtalkDurationPercentage",
        ),
        Column(
            "other_duration_percentage",
            "NUMBER(5,2)",
            source_name="participantMetrics_otherDurationPercentage",
        ),
        Column(
            "overtalk_count", "NUMBER", source_name="participantMetrics_overtalkCount"
        ),
        Column(
            "updated_at",
            "TIMESTAMP_LTZ(3)",
            source_name="updated_at",
            delta_column=True,
        ),
    ],
)
