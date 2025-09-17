from include.airflow.operators.snowflake_mart_base import Column
from include.airflow.operators.snowflake_mart_fact import SnowflakeMartFactOperator
from include.utils.snowflake import ForeignKey


def get_mart_operator():
    return SnowflakeMartFactOperator(
        table="fact_order_line_embroidery",
        use_surrogate_key=False,
        initial_load_value="1900-01-01",
        transform_proc_list=["stg.transform_fact_order_line_embroidery.sql"],
        column_list=[
            Column("order_line_id", "INT", uniqueness=True),
            Column("embroidery_order_line_id", "INT", uniqueness=True),
            Column("meta_original_order_line_id", "INT"),
            Column("meta_original_embroidery_order_line_id", "INT"),
            Column("order_id", "INT", foreign_key=ForeignKey("fact_order")),
            Column("product_id", "INT", foreign_key=ForeignKey("dim_product")),
            Column("order_line_status_key", "INT"),
            Column("order_status_key", "INT"),
            Column("item_quantity", "INT"),
            Column("payment_transaction_local_amount", "NUMBER(19,4)"),
            Column("subtotal_excl_tariff_local_amount", "NUMBER(19,4)"),
            Column("tax_local_amount", "NUMBER(19,4)"),
            Column("tax_cash_local_amount", "NUMBER(19,4)"),
            Column("tax_credit_local_amount", "NUMBER(19,4)"),
            Column("product_discount_local_amount", "NUMBER(19,4)"),
            Column("cash_credit_local_amount", "NUMBER(19,4)"),
            Column("cash_membership_credit_local_amount", "NUMBER(19,4)"),
            Column("cash_refund_credit_local_amount", "NUMBER(19,4)"),
            Column("cash_giftco_credit_local_amount", "NUMBER(19,4)"),
            Column("cash_giftcard_credit_local_amount", "NUMBER(19,4)"),
            Column("non_cash_credit_local_amount", "NUMBER(19,4)"),
            Column("estimated_landed_cost_local_amount", "NUMBER(19,4)"),
            Column("reporting_landed_cost_local_amount", "NUMBER(19,4)"),
            Column("actual_landed_cost_local_amount", "NUMBER(19,4)"),
            Column("oracle_cost_local_amount", "NUMBER(19,4)"),
            Column("tariff_revenue_local_amount", "NUMBER(19,4)"),
            Column("price_offered_local_amount", "NUMBER(19,4)"),
            Column("air_vip_price", "NUMBER(19,4)"),
            Column("retail_unit_price", "NUMBER(19,4)"),
            Column("product_price_history_key", "INT"),
            Column("sub_group_key", "VARCHAR"),
            Column("is_test_customer", "BOOLEAN"),
            Column("is_deleted", "BOOLEAN"),
            Column("bounceback_endowment_local_amount", "NUMBER(19,4)"),
            Column("vip_endowment_local_amount", "NUMBER(19,4)"),
        ],
        watermark_tables=[
            "edw_prod.stg.fact_order_line",
            "edw_prod.stg.fact_order_line_product_cost",
            'lake_consolidated.ultra_merchant."ORDER"',
        ],
    )
