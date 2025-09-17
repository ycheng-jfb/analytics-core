from include.airflow.operators.snowflake_mart_base import Column, KeyLookupJoin
from include.airflow.operators.snowflake_mart_fact import SnowflakeMartFactOperator


def get_mart_operator():
    return SnowflakeMartFactOperator(
        table="fact_product_ref_cost_history",
        initial_load_value="2020-05-01",
        transform_proc_list=["stg.transform_fact_product_ref_cost_history.sql"],
        column_list=[
            Column(
                "geography_key",
                "INT",
                uniqueness=True,
                lookup_table="dim_geography",
                lookup_table_key="geography_key",
                key_lookup_list=[
                    KeyLookupJoin(stg_column="sub_region", stg_column_type="VARCHAR(7)")
                ],
            ),
            Column(
                "currency_key",
                "INT",
                lookup_table="dim_currency",
                lookup_table_key="currency_key",
                key_lookup_list=[
                    KeyLookupJoin(
                        stg_column="currency_code",
                        stg_column_type="VARCHAR(7)",
                        lookup_column="iso_currency_code",
                    ),
                    KeyLookupJoin(
                        stg_column="exchange_rate_type",
                        stg_column_type="VARCHAR(25)",
                        lookup_column="currency_exchange_rate_type",
                    ),
                ],
            ),
            Column("sku", "VARCHAR(30)", uniqueness=True),
            Column("cost_start_datetime", "TIMESTAMP_LTZ(3)", uniqueness=True),
            Column("cost_end_datetime", "TIMESTAMP_LTZ(3)"),
            Column("cost_type", "VARCHAR(50)"),
            Column("is_current_cost", "BOOLEAN"),
            Column("total_cost_amount", "NUMBER(38,2)"),
            Column("product_cost_amount", "NUMBER(38,2)"),
            Column("cmt_cost_amount", "NUMBER(38,2)"),
            Column("freight_cost_amount", "NUMBER(38,2)"),
            Column("duty_cost_amount", "NUMBER(38,2)"),
            Column("commission_cost_amount", "NUMBER(38,2)"),
        ],
        watermark_tables=[
            "lake.evolve01_ssrs_reports.jfportal_poskus",
            "lake.great_plains.iv_fifo",
        ],
        key_lookup_join_datetime_column="cost_start_datetime",
    )
