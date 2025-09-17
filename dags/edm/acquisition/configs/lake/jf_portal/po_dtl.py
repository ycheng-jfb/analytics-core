from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database="JF_Portal",
    schema="dbo",
    table="PoDtl",
    schema_version_prefix="v4",
    is_full_upsert=True,
    enable_archive=True,
    column_list=[
        Column("po_dtl_id", "INT", uniqueness=True, source_name="PoDtlId"),
        Column("po_id", "INT", source_name="PoId"),
        Column("style", "VARCHAR(50)", source_name="Style"),
        Column("style_sku_seg", "VARCHAR(10)", source_name="StyleSkuSeg"),
        Column("description", "VARCHAR(500)", source_name="Description"),
        Column("style_type", "VARCHAR(50)", source_name="StyleType"),
        Column("style_type_sku_seg", "VARCHAR(2)", source_name="StyleTypeSkuSeg"),
        Column("color_id", "INT", source_name="ColorId"),
        Column("color", "VARCHAR(50)", source_name="Color"),
        Column("color_sku_seg", "VARCHAR(5)", source_name="ColorSkuSeg"),
        Column("style_scale_dtl_id", "INT", source_name="StyleScaleDtlId"),
        Column("size_name", "VARCHAR(15)", source_name="SizeName"),
        Column("size_sku_seg", "VARCHAR(5)", source_name="SizeSkuSeg"),
        Column("case_pack_size", "INT", source_name="CasePackSize"),
        Column("num_case_pack", "INT", source_name="NumCasePack"),
        Column("qty", "INT", source_name="Qty"),
        Column("cost", "NUMBER(19, 4)", source_name="Cost"),
        Column("misc_cost", "NUMBER(19, 4)", source_name="MiscCost"),
        Column("duty", "NUMBER(19, 4)", source_name="Duty"),
        Column("freight", "NUMBER(19, 4)", source_name="Freight"),
        Column("sku", "VARCHAR(31)", source_name="Sku"),
        Column("vendor_sku", "VARCHAR(31)", source_name="VendorSku"),
        Column("user_create", "VARCHAR(100)", source_name="UserCreate"),
        Column("user_update", "VARCHAR(100)", source_name="UserUpdate"),
        Column(
            "date_create", "TIMESTAMP_NTZ(3)", delta_column=1, source_name="DateCreate"
        ),
        Column(
            "date_update", "TIMESTAMP_NTZ(3)", delta_column=0, source_name="DateUpdate"
        ),
        Column("ord", "INT", source_name="ORD"),
        Column("gp_status", "INT", source_name="GpStatus"),
        Column("style_name", "VARCHAR(100)", source_name="StyleName"),
        Column("gp_description", "VARCHAR(500)", source_name="GpDescription"),
        Column("brand", "VARCHAR(50)", source_name="Brand"),
        Column("msrp", "NUMBER(19, 4)", source_name="MSRP"),
        Column("vip_price", "NUMBER(19, 4)", source_name="VIPPrice"),
        Column("availability", "BOOLEAN", source_name="Availability"),
        Column("subclass", "VARCHAR(50)", source_name="Subclass"),
        Column("msrpusd", "NUMBER(19, 4)", source_name="MSRPUSD"),
        Column("msrpeur", "NUMBER(19, 4)", source_name="MSRPEUR"),
        Column("msrpgbp", "NUMBER(19, 4)", source_name="MSRPGBP"),
        Column("oracle_po_line_id", "INT", source_name="OraclePOLineID"),
        Column("oracle_line_number", "INT", source_name="OracleLineNumber"),
        Column("is_cancelled", "BOOLEAN", source_name="IsCancelled"),
        Column("hide", "BOOLEAN"),
        Column("plm_dev_style_color_id", "INT", source_name="PlmDevStyleColorId"),
        Column(
            "centric_composition", "VARCHAR(1000)", source_name="CentricComposition"
        ),
        Column(
            "centric_fabric_category",
            "VARCHAR(1000)",
            source_name="CentricFabricCategory",
        ),
    ],
)
