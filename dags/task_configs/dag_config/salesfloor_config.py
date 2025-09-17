from dataclasses import dataclass

from include.config import stages
from include.utils.snowflake import Column


@dataclass
class Config:
    database: str
    schema: str
    table: str
    version: str
    column_list: list

    @property
    def s3_prefix(self):
        return f"lake/{self.database}.{self.schema}.{self.table}/{self.version}"

    @property
    def custom_select(self):
        return f"""
                SELECT
                    f.$1, f.$2, f.$3, f.$4, f.$5, f.$6, f.$7, f.$8, f.$9, f.$10, f.$11,
                    f.$12, f.$13, f.$14, f.$15, f.$16, f.$17, f.$18, f.$19, f.$20, f.$21,
                    DATE(f.$12) as date_truncated,
                    SPLIT_PART(SPLIT_PART(metadata$filename, '_', -1), '.', 0) AS updated_at
                FROM '{stages.tsos_da_int_inbound}/{self.s3_prefix}/' as f
        """


transaction_details_cfg = Config(
    database="lake",
    schema="salesfloor",
    table="transaction_details",
    version="v1",
    column_list=[
        Column("user", "VARCHAR", source_name="User"),
        Column("current_status", "VARCHAR", source_name="Current Status"),
        Column("email", "VARCHAR", source_name="Email"),
        Column("retailer_id", "NUMBER", source_name="Retailer ID", uniqueness=True),
        Column("user_login", "VARCHAR", source_name="User Login"),
        Column("permission", "NUMBER", source_name="Permission"),
        Column("type", "VARCHAR", source_name="Type"),
        Column("store_id", "NUMBER", source_name="Store ID", uniqueness=True),
        Column(
            "retailer_store_id",
            "NUMBER",
            source_name="Retailer Store ID",
            uniqueness=True,
        ),
        Column("location", "VARCHAR", source_name="Location"),
        Column(
            "transaction_id", "NUMBER", source_name="Transaction ID", uniqueness=True
        ),
        Column("date", "VARCHAR", source_name="Date"),
        Column("product_id", "STRING", source_name="Product ID", uniqueness=True),
        Column("product_name", "VARCHAR", source_name="Product Name"),
        Column("quantity", "NUMBER", source_name="Quantity"),
        Column("unit_price", "FLOAT", source_name="Unit Price"),
        Column("attribution", "VARCHAR", source_name="Attribution"),
        Column("contact_record_id", "VARCHAR", source_name="Contact Record ID"),
        Column(
            "date_store_local_time", "VARCHAR", source_name="Date (Store's local time)"
        ),
        Column("transaction_type", "VARCHAR", source_name="Transaction Type"),
        Column("variant_id", "STRING", source_name="Variant ID", uniqueness=True),
        Column("date_truncated", "DATE", uniqueness=True),
        Column("updated_at", "DATE", delta_column=True),
    ],
)
