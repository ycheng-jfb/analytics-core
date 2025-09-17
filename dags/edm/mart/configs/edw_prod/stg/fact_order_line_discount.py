from include.airflow.operators.snowflake_mart_base import Column
from include.airflow.operators.snowflake_mart_fact import SnowflakeMartFactOperator
from include.utils.snowflake import ForeignKey


def get_mart_operator():
    return SnowflakeMartFactOperator(
        table="fact_order_line_discount",
        initial_load_value="1900-01-01",
        transform_proc_list=["stg.transform_fact_order_line_discount.sql"],
        use_surrogate_key=False,
        column_list=[
            Column("order_line_discount_id", "NUMBER(38,0)", uniqueness=True),
            Column("meta_original_order_line_discount_id", "NUMBER(38,0)"),
            Column(
                "order_line_id",
                "NUMBER(38,0)",
                foreign_key=ForeignKey("fact_order_line"),
                uniqueness=True,
            ),
            Column("order_id", "NUMBER(38,0)", foreign_key=ForeignKey("fact_order")),
            Column("bundle_order_line_discount_id", "NUMBER(38,0)", uniqueness=True),
            Column(
                "promo_history_key",
                "NUMBER(38,0)",
                foreign_key=ForeignKey("dim_promo_history"),
            ),
            Column("promo_id", "NUMBER(38,0)", uniqueness=True),
            Column(
                "discount_id", "NUMBER(38,0)", foreign_key=ForeignKey("dim_discount")
            ),
            Column("order_line_discount_local_amount", "NUMBER(19,4)"),
            Column("is_indirect_discount", "BOOLEAN"),
            Column("is_deleted", "BOOLEAN"),
        ],
        watermark_tables=[
            "lake_consolidated.ultra_merchant.order_line_discount",
            "lake_consolidated.ultra_merchant.order_line",
            "edw_prod.stg.dim_promo_history",
            "edw_prod.stg.fact_order_line",
        ],
    )
