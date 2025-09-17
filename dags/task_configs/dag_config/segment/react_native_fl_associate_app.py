from snowflake.snowpark.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    ArrayType,
    BooleanType,
    MapType,
    LongType,
    DoubleType,
    TimestampType,
)

react_native_fl_associate_app_schema = StructType(
    [
        StructField("messageId", StringType(), True),
        StructField("type", StringType(), True),
        StructField("event", StringType(), True),
        StructField("receivedAt", TimestampType(), True),
        StructField("originalTimestamp", TimestampType(), True),
        StructField("channel", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField(
            "context",
            StructType(
                [
                    StructField("instanceId", StringType(), True),
                    StructField(
                        "network",
                        StructType(
                            [
                                StructField("cellular", BooleanType(), True),
                                StructField("wifi", BooleanType(), True),
                                StructField("carrier", StringType(), True),
                            ]
                        ),
                    ),
                    StructField("ip", StringType(), True),
                    StructField("timezone", StringType(), True),
                    StructField(
                        "os",
                        StructType(
                            [
                                StructField("name", StringType(), True),
                                StructField("version", StringType(), True),
                            ]
                        ),
                    ),
                    StructField("locale", StringType(), True),
                    StructField(
                        "screen",
                        StructType(
                            [
                                StructField("height", StringType(), True),
                                StructField("width", StringType(), True),
                            ]
                        ),
                    ),
                    StructField(
                        "library",
                        StructType(
                            [
                                StructField("name", StringType(), True),
                                StructField("version", StringType(), True),
                            ]
                        ),
                    ),
                    StructField(
                        "app",
                        StructType(
                            [
                                StructField("namespace", StringType(), True),
                                StructField("name", StringType(), True),
                                StructField("build", StringType(), True),
                                StructField("version", StringType(), True),
                            ]
                        ),
                    ),
                    StructField(
                        "device",
                        StructType(
                            [
                                StructField("name", StringType(), True),
                                StructField("model", StringType(), True),
                                StructField("id", StringType(), True),
                                StructField("manufacturer", StringType(), True),
                                StructField("type", StringType(), True),
                            ]
                        ),
                    ),
                ]
            ),
        ),
        StructField("projectId", StringType(), True),
        StructField("version", StringType(), True),
        StructField("sentAt", TimestampType(), True),
        StructField("writeKey", StringType(), True),
        StructField("anonymousId", StringType(), True),
        StructField(
            "properties",
            StructType(
                [
                    StructField("order_id", StringType(), True),
                    StructField("store_id", StringType(), True),
                ]
            ),
        ),
    ]
)
