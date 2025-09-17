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

javascript_jf_schema = StructType(
    [
        StructField(
            "_metadata",
            StructType(
                [
                    StructField("bundled", ArrayType(StringType()), True),
                    StructField("bundledIds", ArrayType(StringType()), True),
                    StructField("unbundled", ArrayType(StringType()), True),
                ]
            ),
            True,
        ),
        StructField("anonymousId", StringType(), True),
        StructField("category", StringType(), True),
        StructField("channel", StringType(), True),
        StructField(
            "context",
            StructType(
                [
                    StructField(
                        "campaign",
                        StructType(
                            [
                                StructField("content", StringType(), True),
                                StructField("id", StringType(), True),
                                StructField("medium", StringType(), True),
                                StructField("name", StringType(), True),
                                StructField("nooverride", StringType(), True),
                                StructField("segment", StringType(), True),
                                StructField("souce", StringType(), True),
                                StructField("source", StringType(), True),
                                StructField("term", StringType(), True),
                                StructField("vizid", StringType(), True),
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
                        True,
                    ),
                    StructField("locale", StringType(), True),
                    StructField(
                        "page",
                        StructType(
                            [
                                StructField("path", StringType(), True),
                                StructField("referrer", StringType(), True),
                                StructField("search", StringType(), True),
                                StructField("title", StringType(), True),
                                StructField("url", StringType(), True),
                            ]
                        ),
                        True,
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
                    StructField("userAgent", StringType(), True),
                ]
            ),
        ),
        StructField("event", StringType(), True),
        StructField(
            "integrations",
            StructType(
                [
                    StructField("All", BooleanType(), True),
                    StructField("Google Analytics", BooleanType(), True),
                    StructField("Google Tag Manager", BooleanType(), True),
                    StructField("Segment.io", BooleanType(), True),
                ]
            ),
        ),
        StructField("messageId", StringType(), True),
        StructField("name", StringType(), True),
        StructField("originalTimestamp", StringType(), True),
        StructField("projectId", StringType(), True),
        StructField(
            "properties",
            StructType(
                [
                    StructField("automated_test", BooleanType(), True),
                    StructField("avg_recommended", DoubleType(), True),
                    StructField("bundle_alias", StringType(), True),
                    StructField("bundle_index", LongType(), True),
                    StructField("bundle_name", StringType(), True),
                    StructField("bundle_product_id", StringType(), True),
                    StructField("bundle_product_id_hit", StringType(), True),
                    StructField("bundle_quantity", LongType(), True),
                    StructField("bundle_retail_price", DoubleType(), True),
                    StructField("bundle_sale_price", DoubleType(), True),
                    StructField("bundle_url", StringType(), True),
                    StructField("bundle_vip_price", DoubleType(), True),
                    StructField("category", StringType(), True),
                    StructField("customer_bucket_group", StringType(), True),
                    StructField("customer_gender", StringType(), True),
                    StructField("customer_id", LongType(), True),
                    StructField("dmg_code", StringType(), True),
                    StructField("feature", StringType(), True),
                    StructField("fpl_id", StringType(), True),
                    StructField("gateway", BooleanType(), True),
                    StructField("grid_label", StringType(), True),
                    StructField("has_looks", BooleanType(), True),
                    StructField("has_reviews", BooleanType(), True),
                    StructField("has_ugc", BooleanType(), True),
                    StructField("has_video", BooleanType(), True),
                    StructField("image_url", StringType(), True),
                    StructField("inventory_availability", LongType(), True),
                    StructField("is_bundle", BooleanType(), True),
                    StructField("is_default_model", BooleanType(), True),
                    StructField("is_plus_size", BooleanType(), True),
                    StructField("label", StringType(), True),
                    StructField("list", StringType(), True),
                    StructField("list_id", StringType(), True),
                    StructField("location", StringType(), True),
                    StructField("loggedin_status", BooleanType(), True),
                    StructField("membership_level_group_id", LongType(), True),
                    StructField("name", StringType(), True),
                    StructField("page_name", StringType(), True),
                    StructField("path", StringType(), True),
                    StructField("position", LongType(), True),
                    StructField("price", DoubleType(), True),
                    StructField("product_id", StringType(), True),
                    StructField("product_id_hit", StringType(), True),
                    StructField(
                        "products",
                        ArrayType(
                            StructType(
                                [
                                    StructField("bundle_alias", StringType(), True),
                                    StructField("bundle_name", StringType(), True),
                                    StructField("bundle_product_id", StringType(), True),
                                    StructField("bundle_quantity", LongType(), True),
                                    StructField("bundle_retail_price", DoubleType(), True),
                                    StructField("bundle_sale_price", DoubleType(), True),
                                    StructField("bundle_url", StringType(), True),
                                    StructField("bundle_vip_price", DoubleType(), True),
                                    StructField("category", StringType(), True),
                                    StructField("category_label", StringType(), True),
                                    StructField("fpl_id", StringType(), True),
                                    StructField("grid_label", StringType(), True),
                                    StructField("has_reviews", BooleanType(), True),
                                    StructField("image_url", StringType(), True),
                                    StructField("inventory_availability", LongType(), True),
                                    StructField("is_bundle", BooleanType(), True),
                                    StructField("label", StringType(), True),
                                    StructField("list_id", StringType(), True),
                                    StructField("name", StringType(), True),
                                    StructField("position", StringType(), True),
                                    StructField("price", DoubleType(), True),
                                    StructField("product_id", StringType(), True),
                                    StructField("retail_price", DoubleType(), True),
                                    StructField("review_num", LongType(), True),
                                    StructField("review_rating", DoubleType(), True),
                                    StructField("sale_price", DoubleType(), True),
                                    StructField("size", StringType(), True),
                                    StructField("sized_product_id", StringType(), True),
                                    StructField("sized_sku", StringType(), True),
                                    StructField("sku", StringType(), True),
                                    StructField("url", StringType(), True),
                                    StructField("variant", StringType(), True),
                                    StructField("vip_price", DoubleType(), True),
                                ]
                            )
                        ),
                    ),
                    StructField("quantity", LongType(), True),
                    StructField("query", StringType(), True),
                    StructField("referrer", StringType(), True),
                    StructField("referring_page_module", StringType(), True),
                    StructField("retail_price", DoubleType(), True),
                    StructField("review_num", LongType(), True),
                    StructField("review_rating", DoubleType(), True),
                    StructField("sale_price", DoubleType(), True),
                    StructField("search", StringType(), True),
                    StructField("session_id", StringType(), True),
                    StructField("size", StringType(), True),
                    StructField("sized_product_id", StringType(), True),
                    StructField("sized_sku", StringType(), True),
                    StructField("sku", StringType(), True),
                    StructField("storeGroupId", LongType(), True),
                    StructField("store_group_id", StringType(), True),
                    StructField("title", StringType(), True),
                    StructField("url", StringType(), True),
                    StructField("user_status_initial", StringType(), True),
                    StructField("variant", StringType(), True),
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
                    StructField("activated_date", StringType(), True),
                    StructField("birthday", StringType(), True),
                    StructField("email", StringType(), True),
                    StructField("error", StringType(), True),
                    StructField("gender", StringType(), True),
                    StructField("loyalty_membership_reward_tier_id", LongType(), True),
                    StructField("loyalty_membership_tier_points", LongType(), True),
                    StructField("membership_id", LongType(), True),
                    StructField("membership_level_group_id", LongType(), True),
                    StructField("membership_level_label", StringType(), True),
                    StructField("membership_reward_balance", LongType(), True),
                    StructField("membership_status", StringType(), True),
                    StructField("membership_statuscode", LongType(), True),
                    StructField("total_num_credits", DoubleType(), True),
                    StructField("user_id", StringType(), True),
                    StructField("user_registered_at", StringType(), True),
                    StructField("zip_code", StringType(), True),
                ]
            ),
        ),
        StructField("type", StringType(), True),
        StructField("userId", StringType(), True),
        StructField("version", LongType(), True),
    ]
)
