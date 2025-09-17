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

java_sd_schema = StructType(
    [
        StructField("anonymousId", StringType(), True),
        StructField("channel", StringType(), True),
        StructField(
            "context",
            StructType(
                [
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
                    StructField(
                        "protocols",
                        StructType(
                            [
                                StructField("omitted", ArrayType(StringType()), True),
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
                ]
            ),
        ),
        StructField("event", StringType(), True),
        StructField("messageId", StringType(), True),
        StructField("originalTimestamp", StringType(), True),
        StructField("projectId", StringType(), True),
        StructField(
            "properties",
            StructType(
                [
                    StructField("membership_brand_id", DoubleType(), True),
                    StructField("first_name", StringType(), True),
                    StructField("last_name", StringType(), True),
                    StructField("phone_number", StringType(), True),
                    StructField("event_label", StringType(), True),
                    StructField("automated_test", BooleanType(), True),
                    StructField("bundle_alias", StringType(), True),
                    StructField("bundle_name", StringType(), True),
                    StructField("bundle_product_id", StringType(), True),
                    StructField("bundle_quantity", LongType(), True),
                    StructField("bundle_retail_price", DoubleType(), True),
                    StructField("bundle_sale_price", DoubleType(), True),
                    StructField("bundle_url", StringType(), True),
                    StructField("bundle_vip_price", DoubleType(), True),
                    StructField("cart_id", StringType(), True),
                    StructField("category", StringType(), True),
                    StructField("correlation_id", StringType(), True),
                    StructField("credit", DoubleType(), True),
                    StructField("currency", StringType(), True),
                    StructField("customer_bucket_group", LongType(), True),
                    StructField("customer_gender", StringType(), True),
                    StructField("customer_id", StringType(), True),
                    StructField("d1", BooleanType(), True),
                    StructField("discount", DoubleType(), True),
                    StructField("dmg_code", StringType(), True),
                    StructField("image_url", StringType(), True),
                    StructField("inventory_availability", LongType(), True),
                    StructField("inventory_count", LongType(), True),
                    StructField("is_activating", BooleanType(), True),
                    StructField("is_bundle", BooleanType(), True),
                    StructField("label", StringType(), True),
                    StructField("list_id", StringType(), True),
                    StructField("loggedin_status", BooleanType(), True),
                    StructField("m1", BooleanType(), True),
                    StructField("membership_status", StringType(), True),
                    StructField("name", StringType(), True),
                    StructField("nonInteraction", StringType(), True),
                    StructField("order_id", StringType(), True),
                    StructField("page_hostname", StringType(), True),
                    StructField("price", DoubleType(), True),
                    StructField("product_id", StringType(), True),
                    StructField(
                        "products",
                        ArrayType(
                            StructType(
                                [
                                    StructField("bundle_alias", StringType(), True),
                                    StructField("bundle_name", StringType(), True),
                                    StructField("bundle_product_id", StringType(), True),
                                    StructField("bundle_quantity", LongType(), True),
                                    StructField("bundle_retail_price", LongType(), True),
                                    StructField("bundle_sale_price", LongType(), True),
                                    StructField("bundle_url", StringType(), True),
                                    StructField("bundle_vip_price", LongType(), True),
                                    StructField("category", StringType(), True),
                                    StructField("image_url", StringType(), True),
                                    StructField("inventory_availability", LongType(), True),
                                    StructField("inventory_count", LongType(), True),
                                    StructField("is_bundle", BooleanType(), True),
                                    StructField("list_id", StringType(), True),
                                    StructField("name", StringType(), True),
                                    StructField("price", DoubleType(), True),
                                    StructField("product_id", StringType(), True),
                                    StructField("quantity", LongType(), True),
                                    StructField("retail_price", DoubleType(), True),
                                    StructField("sale_price", DoubleType(), True),
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
                    StructField("retail_price", DoubleType(), True),
                    StructField("revenue", DoubleType(), True),
                    StructField("sale_price", DoubleType(), True),
                    StructField("session_id", StringType(), True),
                    StructField("sha_email", StringType(), True),
                    StructField("shipping", DoubleType(), True),
                    StructField("signin_location", StringType(), True),
                    StructField("signin_method", StringType(), True),
                    StructField("size", StringType(), True),
                    StructField("sized_product_id", StringType(), True),
                    StructField("sized_sku", StringType(), True),
                    StructField("sku", StringType(), True),
                    StructField("source", StringType(), True),
                    StructField("store_group_id", LongType(), True),
                    StructField("store_id", LongType(), True),
                    StructField("subtotal", DoubleType(), True),
                    StructField("tax", DoubleType(), True),
                    StructField("total", DoubleType(), True),
                    StructField("url", StringType(), True),
                    StructField("variant", StringType(), True),
                    StructField("vip_price", DoubleType(), True),
                    StructField("visitor_group", LongType(), True),
                    StructField("visitor_id", StringType(), True),
                ]
            ),
        ),
        StructField("receivedAt", StringType(), True),
        StructField("sentAt", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("type", StringType(), True),
        StructField("userId", StringType(), True),
        StructField("version", LongType(), True),
        StructField("writeKey", StringType(), True),
        StructField("traits", StringType(), True),
    ]
)
