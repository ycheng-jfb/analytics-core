from include.airflow.operators.snowflake_mart_base import Column, KeyLookupJoin
from include.airflow.operators.snowflake_mart_fact import SnowflakeMartFactOperator
from include.utils.snowflake import ForeignKey


def get_mart_operator():
    return SnowflakeMartFactOperator(
        table="fact_refund",
        use_surrogate_key=False,
        initial_load_value="1900-01-01",
        transform_proc_list=["stg.transform_fact_refund.sql"],
        column_list=[
            Column("refund_id", "NUMBER(38,0)", uniqueness=True),
            Column("meta_original_refund_id", "NUMBER(38,0)"),
            Column("refund_payment_method_key", "NUMBER(38,0)"),
            Column(
                "refund_reason_key",
                "NUMBER(38,0)",
                lookup_table="dim_refund_reason",
                lookup_table_key="refund_reason_key",
                key_lookup_list=[
                    KeyLookupJoin(
                        stg_column="refund_reason", stg_column_type="VARCHAR(255)"
                    )
                ],
            ),
            Column(
                "refund_comment_key",
                "NUMBER(38,0)",
                lookup_table="dim_refund_comment",
                lookup_table_key="refund_comment_key",
                key_lookup_list=[
                    KeyLookupJoin(
                        stg_column="refund_comment", stg_column_type="VARCHAR(255)"
                    )
                ],
            ),
            Column(
                "refund_status_key",
                "NUMBER(38,0)",
                lookup_table="dim_refund_status",
                lookup_table_key="refund_status_key",
                key_lookup_list=[
                    KeyLookupJoin(
                        stg_column="refund_status_code", stg_column_type="INT"
                    )
                ],
            ),
            Column(
                "raw_refund_status_key",
                "NUMBER(38,0)",
                lookup_table="dim_refund_status",
                lookup_table_key="refund_status_key",
                key_lookup_list=[
                    KeyLookupJoin(
                        stg_column="raw_refund_status_code",
                        stg_column_type="INT",
                        lookup_column="refund_status_code",
                    )
                ],
            ),
            Column("order_id", "NUMBER(38,0)", foreign_key=ForeignKey("fact_order")),
            Column(
                "customer_id", "NUMBER(38,0)", foreign_key=ForeignKey("dim_customer")
            ),
            Column("activation_key", "NUMBER(38,0)"),
            Column("first_activation_key", "NUMBER(38,0)"),
            Column("store_id", "NUMBER(38,0)", foreign_key=ForeignKey("dim_store")),
            Column("return_id", "NUMBER(38,0)"),
            Column("refund_request_local_datetime", "TIMESTAMP_TZ(3)"),
            Column("refund_completion_local_datetime", "TIMESTAMP_TZ(3)"),
            Column("rma_id", "NUMBER(38,0)"),
            Column("refund_administrator_id", "NUMBER(38,0)"),
            Column("refund_approver_id", "NUMBER(38,0)"),
            Column("refund_payment_transaction_id", "NUMBER(38,0)"),
            Column("refund_product_local_amount", "NUMBER(19,4)"),
            Column("refund_freight_local_amount", "NUMBER(19,4)"),
            Column("refund_tax_local_amount", "NUMBER(19,4)"),
            Column("refund_total_local_amount", "NUMBER(19,4)"),
            Column("refund_completion_date_eur_conversion_rate", "NUMBER(18,6)"),
            Column("refund_completion_date_usd_conversion_rate", "NUMBER(18,6)"),
            Column("effective_vat_rate", "NUMBER(18,6)"),
            Column("is_chargeback", "BOOLEAN"),
            Column("is_test_customer", "BOOLEAN"),
            Column("is_deleted", "BOOLEAN"),
            Column("source_order_id", "NUMBER(38,0)"),
        ],
        watermark_tables=[
            "lake_consolidated.ultra_merchant.refund",
            "lake_consolidated.ultra_merchant.refund_reason",
            "edw_prod.stg.fact_chargeback",
            "lake_consolidated.ultra_merchant.order_detail",
            "lake_consolidated.ultra_merchant.order_classification",
            "lake_consolidated.ultra_merchant.address",
            "edw_prod.stg.dim_customer",
            "edw_prod.stg.dim_refund_payment_method",
            "edw_prod.stg.fact_activation",
            "edw_prod.stg.fact_order",
            "lake_consolidated.ultra_merchant.rma_refund",
            "lake_consolidated.ultra_merchant.rma",
            "lake_consolidated.ultra_merchant.return",
            "lake_consolidated.ultra_merchant.refund_membership_token",
            "lake_consolidated.ultra_merchant.membership_token",
            "lake_consolidated.ultra_merchant.membership_token_reason",
            "lake_consolidated.ultra_merchant.refund_store_credit",
            "lake_consolidated.ultra_merchant.store_credit",
            "lake_consolidated.ultra_merchant.store_credit_reason",
        ],
        key_lookup_join_datetime_column="refund_completion_local_datetime",
    )
