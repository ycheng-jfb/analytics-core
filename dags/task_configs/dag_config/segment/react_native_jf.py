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
)

react_native_jf_schema = StructType(
    [
        StructField("anonymousId", StringType(), True),
        StructField("channel", StringType(), True),
        StructField(
            "context",
            StructType(
                [
                    StructField(
                        "app",
                        StructType(
                            [
                                StructField("build", StringType(), True),
                                StructField("name", StringType(), True),
                                StructField("namespace", StringType(), True),
                                StructField("version", StringType(), True),
                            ]
                        ),
                    ),
                    StructField(
                        "device",
                        StructType(
                            [
                                StructField("id", StringType(), True),
                                StructField("manufacturer", StringType(), True),
                                StructField("model", StringType(), True),
                                StructField("name", StringType(), True),
                                StructField("type", StringType(), True),
                            ]
                        ),
                    ),
                    StructField("ip", StringType(), True),
                    StructField(
                        "library",
                        StructType(
                            [
                                StructField("name", StringType(), True),
                                StructField("version", StringType(), True),
                            ]
                        ),
                    ),
                    StructField("locale", StringType(), True),
                    StructField(
                        "network",
                        StructType(
                            [
                                StructField("bluetooth", BooleanType(), True),
                                StructField("carrier", StringType(), True),
                                StructField("cellular", BooleanType(), True),
                                StructField("wifi", BooleanType(), True),
                            ]
                        ),
                    ),
                    StructField(
                        "os",
                        StructType(
                            [
                                StructField("name", StringType(), True),
                                StructField("version", StringType(), True),
                            ]
                        ),
                    ),
                    StructField(
                        "protocols",
                        StructType(
                            [
                                StructField("sourceId", StringType(), True),
                                StructField(
                                    "violations",
                                    ArrayType(
                                        StructType(
                                            [
                                                StructField("description", StringType(), True),
                                                StructField("field", StringType(), True),
                                                StructField("type", StringType(), True),
                                            ]
                                        )
                                    ),
                                ),
                            ]
                        ),
                    ),
                    StructField(
                        "screen",
                        StructType(
                            [
                                StructField("density", DoubleType(), True),
                                StructField("height", LongType(), True),
                                StructField("width", LongType(), True),
                            ]
                        ),
                    ),
                    StructField("timezone", StringType(), True),
                    StructField(
                        "traits",
                        StructType(
                            [
                                StructField("anonymousId", StringType(), True),
                                StructField("birthday", StringType(), True),
                                StructField("email", StringType(), True),
                                StructField("first_name", StringType(), True),
                                StructField("last_name", StringType(), True),
                                StructField("membership_status", StringType(), True),
                                StructField("userId", StringType(), True),
                                StructField("zip_code", StringType(), True),
                            ]
                        ),
                    ),
                    StructField("userAgent", StringType(), True),
                ]
            ),
        ),
        StructField("event", StringType(), True),
        StructField("messageId", StringType(), True),
        StructField("name", StringType(), True),
        StructField("originalTimestamp", StringType(), True),
        StructField("projectId", StringType(), True),
        StructField(
            "properties",
            StructType(
                [
                    StructField("automated_test", BooleanType(), True),
                    StructField("build", StringType(), True),
                    StructField("bundle_alias", StringType(), True),
                    StructField("bundle_name", StringType(), True),
                    StructField("bundle_product_id", LongType(), True),
                    StructField("bundle_quantity", LongType(), True),
                    StructField("bundle_retail_price", StringType(), True),
                    StructField("bundle_sale_price", StringType(), True),
                    StructField("bundle_vip_price", DoubleType(), True),
                    StructField("category", StringType(), True),
                    StructField("customer_bucket_group", LongType(), True),
                    StructField("customer_gender", StringType(), True),
                    StructField("feature", StringType(), True),
                    StructField("fpl_id", LongType(), True),
                    StructField("from_background", BooleanType(), True),
                    StructField("grid_image_label", StringType(), True),
                    StructField("grid_label", StringType(), True),
                    StructField("inventory_availability", LongType(), True),
                    StructField("is_bundle", BooleanType(), True),
                    StructField("list_id", StringType(), True),
                    StructField("loggedin_status", BooleanType(), True),
                    StructField("position", LongType(), True),
                    StructField("previous_build", StringType(), True),
                    StructField("previous_version", StringType(), True),
                    StructField("price", DoubleType(), True),
                    StructField("product_id", StringType(), True),
                    StructField("product_name", StringType(), True),
                    StructField("product_quantity", LongType(), True),
                    StructField("product_variant", StringType(), True),
                    StructField(
                        "products",
                        ArrayType(
                            StructType(
                                [
                                    StructField("bundle_alias", StringType(), True),
                                    StructField("bundle_name", StringType(), True),
                                    StructField("bundle_product_id", LongType(), True),
                                    StructField("bundle_quantity", LongType(), True),
                                    StructField("bundle_retail_price", DoubleType(), True),
                                    StructField("bundle_sale_price", DoubleType(), True),
                                    StructField("bundle_vip_price", DoubleType(), True),
                                    StructField("category", StringType(), True),
                                    StructField("grid_image_label", StringType(), True),
                                    StructField("grid_label", StringType(), True),
                                    StructField("inventory_availability", LongType(), True),
                                    StructField("is_bundle", BooleanType(), True),
                                    StructField("position", LongType(), True),
                                    StructField("price", DoubleType(), True),
                                    StructField("product_id", StringType(), True),
                                    StructField("product_name", StringType(), True),
                                    StructField("product_quantity", LongType(), True),
                                    StructField("product_variant", StringType(), True),
                                    StructField("qty", LongType(), True),
                                    StructField("retail_price", DoubleType(), True),
                                    StructField("sale_price", DoubleType(), True),
                                    StructField("size", StringType(), True),
                                    StructField("sized_product_id", LongType(), True),
                                    StructField("sized_sku", StringType(), True),
                                    StructField("sku", StringType(), True),
                                    StructField("vip_price", DoubleType(), True),
                                ]
                            )
                        ),
                    ),
                    StructField("qty", LongType(), True),
                    StructField("query", StringType(), True),
                    StructField("referring_application", StringType(), True),
                    StructField("referring_page_module", StringType(), True),
                    StructField("retail_price", DoubleType(), True),
                    StructField("sale_price", DoubleType(), True),
                    StructField("session_id", StringType(), True),
                    StructField("size", StringType(), True),
                    StructField("sized_product_id", LongType(), True),
                    StructField("sized_sku", StringType(), True),
                    StructField("sku", StringType(), True),
                    StructField("store_group_id", LongType(), True),
                    StructField("url", StringType(), True),
                    StructField("version", StringType(), True),
                    StructField("vip_price", DoubleType(), True),
                ]
            ),
        ),
        StructField("receivedAt", StringType(), True),
        StructField("sentAt", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField(
            "traits",
            StructType(
                [
                    StructField("anonymousId", StringType(), True),
                    StructField("birthday", StringType(), True),
                    StructField("email", StringType(), True),
                    StructField("first_name", StringType(), True),
                    StructField("last_name", StringType(), True),
                    StructField("membership_status", StringType(), True),
                    StructField("userId", StringType(), True),
                    StructField("zip_code", StringType(), True),
                ]
            ),
        ),
        StructField("type", StringType(), True),
        StructField("userId", StringType(), True),
        StructField("version", LongType(), True),
        StructField("writeKey", StringType(), True),
    ]
)
