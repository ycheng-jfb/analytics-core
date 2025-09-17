from abc import abstractmethod
from dataclasses import dataclass
from typing import Callable

from include.utils.snowflake import Column


@dataclass
class SamConfig:
    get_query_func: Callable
    sort_param = [{"_id": "asc"}]
    sam_column_list = [
        Column("image_id", "NUMBER(38,0) AUTOINCREMENT"),
        Column("sam_id", "VARCHAR", source_name="_id"),
        Column("aem_uuid", "VARCHAR", source_name="_source_metadata_asset_uuid"),
        Column(
            "membership_brand_id", "VARCHAR", source_name="_source_membership_brand_id"
        ),
        Column(
            "master_store_group_id",
            "VARCHAR",
            source_name="_source_master_store_group_id",
        ),
        Column("product_name", "VARCHAR", source_name="_source_product_name"),
        Column("is_ecat", "BOOLEAN", source_name="_source_is_ecat"),
        Column("is_plus", "BOOLEAN", source_name="_source_is_plus"),
        Column("sort", "VARCHAR", source_name="_source_sort"),
        Column(
            "datetime_added_sam",
            "TIMESTAMP_LTZ(3)",
            source_name="_source_datetime_added",
        ),
        Column(
            "datetime_modified_sam",
            "TIMESTAMP_LTZ(3)",
            source_name="_source_datetime_modified",
        ),
        Column("json_blob", "VARIANT", source_name="json_blob"),
        Column("_effective_from_date", "TIMESTAMP_LTZ(3)", source_name=""),
        Column("_effective_to_date", "TIMESTAMP_LTZ(3)", source_name=""),
        Column("_is_current", "BOOLEAN", source_name=""),
        Column("_is_deleted", "BOOLEAN", source_name=""),
        Column("is_testable", "BOOLEAN", source_name=""),
    ]


def get_hourly_query(watermark_operator):
    return {
        "bool": {
            "must": [
                {
                    "range": {
                        "datetime_modified": {"gt": watermark_operator.low_watermark}
                    }
                },
                {"term": {"_type": "_doc"}},
            ]
        }
    }


def get_daily_query(watermark_operator):
    return {"match_all": {}}


hourlyConfig = SamConfig(get_hourly_query)

dailyConfig = SamConfig(get_daily_query)
