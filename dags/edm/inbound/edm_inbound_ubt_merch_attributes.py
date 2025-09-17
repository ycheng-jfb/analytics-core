from dataclasses import dataclass

import pendulum
from airflow.models import DAG
from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.dag_helpers import chain_tasks
from include.airflow.operators.excel_smb import ExcelSMBToS3Operator, SheetConfig
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.config import conn_ids, email_lists, owners, s3_buckets
from include.utils.snowflake import Column

item_sheet_column_list = [
    Column("product_segment", "STRING"),
    Column("style_sku", "STRING"),
    Column("sku", "STRING"),
    Column("gender", "STRING"),
    Column("category", "STRING"),
    Column("class", "STRING"),
    Column("subclass", "STRING"),
    Column("original_showroom_month", "STRING"),
    Column("current_showroom", "DATE"),
    Column("marketing_story", "STRING"),
    Column("go_live_date", "DATE"),
    Column("item_rank", "STRING"),
    Column("design_style_number", "STRING"),
    Column("current_name", "STRING"),
    Column("color", "STRING"),
    Column("color_family", "STRING"),
    Column("buy_timing", "STRING"),
    Column("item_status", "STRING"),
    Column("style_status", "STRING"),
    Column("sku_status", "STRING"),
    Column("inseams_construction", "STRING"),
    Column("end_use", "STRING"),
    Column("eco_system", "STRING"),
    Column("fit_block", "STRING"),
    Column("color_application", "STRING"),
    Column("eco_style", "STRING"),
    Column("fabric", "STRING"),
    Column("fit_style", "STRING"),
    Column("lined_unlined", "STRING"),
    Column("style_parent", "STRING"),
    Column("size_scale", "STRING"),
    Column("size_range", "STRING"),
    Column("factory", "STRING"),
    Column("editorial_features", "STRING"),
    Column("us_msrp_dollar", "NUMBER(38,2)"),
    Column("us_vip_dollar", "NUMBER(38,2)"),
    Column("us_ttl_buy_units", "NUMBER(38,2)"),
    Column("us_ttl_blended_ldp_dollar", "NUMBER(38,2)"),
    Column("us_blended_vip_imu_percent", "NUMBER(38,2)"),
    Column("us_blended_ldp_dollar", "NUMBER(38,2)"),
    Column("us_ttl_msrp_dollar", "NUMBER(38,2)"),
    Column("us_ttl_vip_dollar", "NUMBER(38,2)"),
    Column("mx_msrp_dollar", "NUMBER(38,2)"),
    Column("mx_vip_dollar", "NUMBER(38,2)"),
    Column("mx_ttl_buy_units", "NUMBER(38,2)"),
    Column("mx_ttl_blended_ldp_dollar", "NUMBER(38,2)"),
    Column("mx_blended_vip_imu_percent", "NUMBER(38,2)"),
    Column("mx_blended_ldp_dollar", "NUMBER(38,2)"),
    Column("mx_ttl_msrp_dollar", "NUMBER(38,2)"),
    Column("mx_ttl_vip_dollar", "NUMBER(38,2)"),
    Column("eu_msrp_dollar", "NUMBER(38,2)"),
    Column("eu_vip_dollar", "NUMBER(38,2)"),
    Column("eu_ttl_buy_units", "NUMBER(38,2)"),
    Column("eu_ttl_blended_ldp_dollar", "NUMBER(38,2)"),
    Column("eu_blended_vip_imu_percent", "NUMBER(38,2)"),
    Column("eu_blended_ldp_dollar", "NUMBER(38,2)"),
    Column("eu_ttl_msrp_dollar", "NUMBER(38,2)"),
    Column("eu_ttl_vip_dollar", "NUMBER(38,2)"),
    Column("uk_msrp_dollar", "NUMBER(38,2)"),
    Column("uk_vip_dollar", "NUMBER(38,2)"),
    Column("uk_ttl_buy_units", "NUMBER(38,2)"),
    Column("uk_ttl_blended_ldp_dollar", "NUMBER(38,2)"),
    Column("uk_blended_vip_imu_percent", "NUMBER(38,2)"),
    Column("uk_blended_ldp_dollar", "NUMBER(38,2)"),
    Column("uk_ttl_msrp_dollar", "NUMBER(38,2)"),
    Column("uk_ttl_vip_dollar", "NUMBER(38,2)"),
    Column("rtl_msrp_dollar", "NUMBER(38,2)"),
    Column("rtl_vip_dollar", "NUMBER(38,2)"),
    Column("rtl_ttl_buy_units", "NUMBER(38,2)"),
    Column("rtl_ttl_blended_ldp_dollar", "NUMBER(38,2)"),
    Column("rtl_blended_vip_imu_percent", "NUMBER(38,2)"),
    Column("rtl_blended_ldp_dollar", "NUMBER(38,2)"),
    Column("rtl_ttl_msrp_dollar", "NUMBER(38,2)"),
    Column("rtl_ttl_vip_dollar", "NUMBER(38,2)"),
    Column("channel", "STRING"),
    Column("global_total_units", "NUMBER(38,2)"),
    Column("na_total_units", "NUMBER(38,2)"),
    Column("na_blended_ldp", "NUMBER(38,2)"),
    Column("eu_plus_uk_total_units", "NUMBER(38,2)"),
    Column("eu_plus_uk_blended_ldp", "NUMBER(38,2)"),
]

outfit_sheet_column_list = [
    Column("gender", "STRING"),
    Column("outfit_number", "STRING"),
    Column("showroom_month", "STRING"),
    Column("go_live_date", "STRING"),
    Column("number_of_outfit_components", "NUMBER(38,0)"),
    Column("us_outfit_vip_dollar", "NUMBER(38,2)"),
    Column("eu_uk_vipdollar", "NUMBER(38,2)"),
    Column("initial_launch", "STRING"),
    Column("additional_information", "STRING"),
    Column("outfit_complexion", "STRING"),
    Column("sku_number_1", "STRING"),
    Column("description_1_current_name", "STRING"),
    Column("style_color_1", "STRING"),
    Column("sku_number_2", "STRING"),
    Column("description_2_current_name", "STRING"),
    Column("style_color_2", "STRING"),
    Column("sku_number_3_reg_inseam", "STRING"),
    Column("sku_number_3_short_inseam", "STRING"),
    Column("sku_number_3_tall_inseam", "STRING"),
    Column("inseam", "STRING"),
    Column("description_3_current_name", "STRING"),
    Column("style_color_3", "STRING"),
    Column("sku_number_4", "STRING"),
    Column("description_4_current_name", "STRING"),
    Column("style_color_4", "STRING"),
    Column("sku_number_5", "STRING"),
    Column("description_5_current_name", "STRING"),
    Column("style_color_5", "STRING"),
    Column("sku_number_6", "STRING"),
    Column("description_6_current_name", "STRING"),
    Column("style_color_6", "STRING"),
    Column("sku_number_7", "STRING"),
    Column("description_7_current_name", "STRING"),
    Column("style_color_7", "STRING"),
    Column("sku_number_8", "STRING"),
    Column("description_8_current_name", "STRING"),
    Column("style_color_8", "STRING"),
    Column("outfit_msrp_dollar", "NUMBER(38,2)"),
    Column("comp_number_1_msrp", "NUMBER(38,2)"),
    Column("comp_number_2_msrp", "NUMBER(38,2)"),
    Column("comp_number_3_msrp", "NUMBER(38,2)"),
    Column("comp_number_4_msrp", "NUMBER(38,2)"),
    Column("comp_number_5_msrp", "NUMBER(38,2)"),
    Column("comp_number_6_msrp", "NUMBER(38,2)"),
    Column("comp_number_7_msrp", "NUMBER(38,2)"),
    Column("comp_number_8_msrp", "NUMBER(38,2)"),
    Column("us_imu", "NUMBER(38,2)"),
    Column("us_outfit_cost_dollar", "NUMBER(38,2)"),
    Column("us_top_cost_dollar", "NUMBER(38,2)"),
    Column("us_bra_jacket_cost_dollar", "NUMBER(38,2)"),
    Column("us_bottom_cost_dollar", "NUMBER(38,2)"),
    Column("us_cost_comp_number_4", "NUMBER(38,2)"),
    Column("us_cost_comp_number_5", "NUMBER(38,2)"),
    Column("us_cost_comp_number_6", "NUMBER(38,2)"),
    Column("us_cost_comp_number_7", "NUMBER(38,2)"),
    Column("us_cost_comp_number_8", "NUMBER(38,2)"),
    Column("mx_imu", "NUMBER(38,2)"),
    Column("mx_outfit_cost_dollar", "NUMBER(38,2)"),
    Column("mx_top_cost_dollar", "NUMBER(38,2)"),
    Column("mx_bra_jacket_cost_dollar", "NUMBER(38,2)"),
    Column("mx_bottom_cost_dollar", "NUMBER(38,2)"),
    Column("mx_cost_comp_number_4", "NUMBER(38,2)"),
    Column("mx_cost_comp_number_5", "NUMBER(38,2)"),
    Column("mx_cost_comp_number_6", "NUMBER(38,2)"),
    Column("mx_cost_comp_number_7", "NUMBER(38,2)"),
    Column("mx_cost_comp_number_8", "NUMBER(38,2)"),
    Column("eu_imu", "NUMBER(38,2)"),
    Column("eu_outfit_cost_dollar", "NUMBER(38,2)"),
    Column("eu_top_cost_dollar", "NUMBER(38,2)"),
    Column("eu_bra_jacket_cost_dollar", "NUMBER(38,2)"),
    Column("eu_bottom_cost_dollar", "NUMBER(38,2)"),
    Column("eu_cost_comp_number_4", "NUMBER(38,2)"),
    Column("eu_cost_comp_number_5", "NUMBER(38,2)"),
    Column("eu_cost_comp_number_6", "NUMBER(38,2)"),
    Column("eu_cost_comp_number_7", "NUMBER(38,2)"),
    Column("eu_cost_comp_number_8", "NUMBER(38,2)"),
    Column("uk_imu", "NUMBER(38,2)"),
    Column("uk_outfit_cost_dollar", "NUMBER(38,2)"),
    Column("uk_top_cost_dollar", "NUMBER(38,2)"),
    Column("uk_bra_jacket_cost_dollar", "NUMBER(38,2)"),
    Column("uk_bottom_cost_dollar", "NUMBER(38,2)"),
    Column("uk_cost_comp_number_4", "NUMBER(38,2)"),
    Column("uk_cost_comp_number_5", "NUMBER(38,2)"),
    Column("uk_cost_comp_number_6", "NUMBER(38,2)"),
    Column("uk_cost_comp_number_7", "NUMBER(38,2)"),
    Column("uk_cost_comp_number_8", "NUMBER(38,2)"),
    Column("rtl_imu", "NUMBER(38,2)"),
    Column("rtl_outfit_cost_dollar", "NUMBER(38,2)"),
    Column("rtl_top_cost_dollar", "NUMBER(38,2)"),
    Column("rtl_bra_jacket_cost_dollar", "NUMBER(38,2)"),
    Column("rtl_bottom_cost_dollar", "NUMBER(38,2)"),
    Column("rtl_cost_comp_number_4", "NUMBER(38,2)"),
    Column("rtl_cost_comp_number_5", "NUMBER(38,2)"),
    Column("rtl_cost_comp_number_6", "NUMBER(38,2)"),
    Column("rtl_cost_comp_number_7", "NUMBER(38,2)"),
    Column("rtl_cost_comp_number_8", "NUMBER(38,2)"),
]

women_item_sheet_column_list = [
    Column("product_segment", "STRING"),
    Column("style_sku", "STRING"),
    Column("sku", "STRING"),
    Column("gender", "STRING"),
    Column("category", "STRING"),
    Column("class", "STRING"),
    Column("subclass", "STRING"),
    Column("original_showroom_month", "STRING"),
    Column("current_showroom", "DATE"),
    Column("marketing_story", "STRING"),
    Column("go_live_date", "DATE"),
    Column("item_rank", "STRING"),
    Column("design_style_number", "STRING"),
    Column("current_name", "STRING"),
    Column("color", "STRING"),
    Column("color_family", "STRING"),
    Column("buy_timing", "STRING"),
    Column("item_status", "STRING"),
    Column("style_status", "STRING"),
    Column("sku_status", "STRING"),
    Column("inseams_construction", "STRING"),
    Column("end_use", "STRING"),
    Column("eco_system", "STRING"),
    Column("fit_block", "STRING"),
    Column("color_application", "STRING"),
    Column("eco_style", "STRING"),
    Column("fabric", "STRING"),
    Column("fit_style", "STRING"),
    Column("lined_unlined", "STRING"),
    Column("style_parent", "STRING"),
    Column("size_scale", "STRING"),
    Column("size_range", "STRING"),
    Column("factory", "STRING"),
    Column("editorial_features", "STRING"),
    Column("us_msrp_dollar", "NUMBER(38,2)"),
    Column("us_vip_dollar", "NUMBER(38,2)"),
    Column("us_ttl_buy_units", "NUMBER(38,2)"),
    Column("us_ttl_blended_ldp_dollar", "NUMBER(38,2)"),
    Column("us_blended_vip_imu_percent", "NUMBER(38,2)"),
    Column("us_blended_ldp_dollar", "NUMBER(38,2)"),
    Column("us_ttl_msrp_dollar", "NUMBER(38,2)"),
    Column("us_ttl_vip_dollar", "NUMBER(38,2)"),
    Column("mx_msrp_dollar", "NUMBER(38,2)"),
    Column("mx_vip_dollar", "NUMBER(38,2)"),
    Column("mx_ttl_buy_units", "NUMBER(38,2)"),
    Column("mx_ttl_blended_ldp_dollar", "NUMBER(38,2)"),
    Column("mx_blended_vip_imu_percent", "NUMBER(38,2)"),
    Column("mx_blended_ldp_dollar", "NUMBER(38,2)"),
    Column("mx_ttl_msrp_dollar", "NUMBER(38,2)"),
    Column("mx_ttl_vip_dollar", "NUMBER(38,2)"),
    Column("eu_msrp_dollar", "NUMBER(38,2)"),
    Column("eu_vip_dollar", "NUMBER(38,2)"),
    Column("eu_ttl_buy_units", "NUMBER(38,2)"),
    Column("eu_ttl_blended_ldp_dollar", "NUMBER(38,2)"),
    Column("eu_blended_vip_imu_percent", "NUMBER(38,2)"),
    Column("eu_blended_ldp_dollar", "NUMBER(38,2)"),
    Column("eu_ttl_msrp_dollar", "NUMBER(38,2)"),
    Column("eu_ttl_vip_dollar", "NUMBER(38,2)"),
    Column("uk_msrp_dollar", "NUMBER(38,2)"),
    Column("uk_vip_dollar", "NUMBER(38,2)"),
    Column("uk_ttl_buy_units", "NUMBER(38,2)"),
    Column("uk_ttl_blended_ldp_dollar", "NUMBER(38,2)"),
    Column("uk_blended_vip_imu_percent", "NUMBER(38,2)"),
    Column("uk_blended_ldp_dollar", "NUMBER(38,2)"),
    Column("uk_ttl_msrp_dollar", "NUMBER(38,2)"),
    Column("uk_ttl_vip_dollar", "NUMBER(38,2)"),
    Column("rtl_msrp_dollar", "NUMBER(38,2)"),
    Column("rtl_vip_dollar", "NUMBER(38,2)"),
    Column("rtl_ttl_buy_units", "NUMBER(38,2)"),
    Column("rtl_ttl_blended_ldp_dollar", "NUMBER(38,2)"),
    Column("rtl_blended_vip_imu_percent", "NUMBER(38,2)"),
    Column("rtl_blended_ldp_dollar", "NUMBER(38,2)"),
    Column("rtl_ttl_msrp_dollar", "NUMBER(38,2)"),
    Column("rtl_ttl_vip_dollar", "NUMBER(38,2)"),
    Column("channel", "STRING"),
    Column("global_total_units", "NUMBER(38,2)"),
    Column("na_total_units", "NUMBER(38,2)"),
    Column("na_blended_ldp", "NUMBER(38,2)"),
    Column("eu_plus_uk_total_units", "NUMBER(38,2)"),
    Column("eu_plus_uk_blended_ldp", "NUMBER(38,2)"),
    Column("style_color", "STRING"),
    Column("tall_inseam", "STRING"),
    Column("short_inseam", "STRING"),
]

women_outfit_sheet_column_list = [
    Column("product_segment", "STRING"),
    Column("gender", "STRING"),
    Column("outfit_number", "STRING"),
    Column("showroom_month", "STRING"),
    Column("go_live_date", "STRING"),
    Column("number_of_outfit_components", "NUMBER(38,2)"),
    Column("us_outfit_vip_dollar", "NUMBER(38,2)"),
    Column("eu_uk_vipdollar", "NUMBER(38,2)"),
    Column("initial_launch", "STRING"),
    Column("additional_information", "STRING"),
    Column("outfit_complexion", "STRING"),
    Column("sku_number_1", "STRING"),
    Column("description_1_current_name", "STRING"),
    Column("style_color_1", "STRING"),
    Column("sku_number_2", "STRING"),
    Column("description_2_current_name", "STRING"),
    Column("style_color_2", "STRING"),
    Column("sku_number_3_reg_inseam", "STRING"),
    Column("sku_number_3_short_inseam", "STRING"),
    Column("sku_number_3_tall_inseam", "STRING"),
    Column("inseam", "STRING"),
    Column("description_3_current_name", "STRING"),
    Column("style_color_3", "STRING"),
    Column("sku_number_4", "STRING"),
    Column("description_4_current_name", "STRING"),
    Column("style_color_4", "STRING"),
    Column("sku_number_5", "STRING"),
    Column("description_5_current_name", "STRING"),
    Column("style_color_5", "STRING"),
    Column("sku_number_6", "STRING"),
    Column("description_6_current_name", "STRING"),
    Column("style_color_6", "STRING"),
    Column("sku_number_7", "STRING"),
    Column("description_7_current_name", "STRING"),
    Column("style_color_7", "STRING"),
    Column("sku_number_8", "STRING"),
    Column("description_8_current_name", "STRING"),
    Column("style_color_8", "STRING"),
    Column("outfit_msrp_dollar", "NUMBER(38,2)"),
    Column("comp_number_1_msrp", "NUMBER(38,2)"),
    Column("comp_number_2_msrp", "NUMBER(38,2)"),
    Column("comp_number_3_msrp", "NUMBER(38,2)"),
    Column("comp_number_4_msrp", "NUMBER(38,2)"),
    Column("comp_number_5_msrp", "NUMBER(38,2)"),
    Column("comp_number_6_msrp", "NUMBER(38,2)"),
    Column("comp_number_7_msrp", "NUMBER(38,2)"),
    Column("comp_number_8_msrp", "NUMBER(38,2)"),
    Column("us_imu", "NUMBER(38,2)"),
    Column("us_outfit_cost_dollar", "NUMBER(38,2)"),
    Column("us_top_cost_dollar", "NUMBER(38,2)"),
    Column("us_bra_jacket_cost_dollar", "NUMBER(38,2)"),
    Column("us_bottom_cost_dollar", "NUMBER(38,2)"),
    Column("us_cost_comp_number_4", "NUMBER(38,2)"),
    Column("us_cost_comp_number_5", "NUMBER(38,2)"),
    Column("us_cost_comp_number_6", "NUMBER(38,2)"),
    Column("us_cost_comp_number_7", "NUMBER(38,2)"),
    Column("us_cost_comp_number_8", "NUMBER(38,2)"),
    Column("mx_imu", "NUMBER(38,2)"),
    Column("mx_outfit_cost_dollar", "NUMBER(38,2)"),
    Column("mx_top_cost_dollar", "NUMBER(38,2)"),
    Column("mx_bra_jacket_cost_dollar", "NUMBER(38,2)"),
    Column("mx_bottom_cost_dollar", "NUMBER(38,2)"),
    Column("mx_cost_comp_number_4", "NUMBER(38,2)"),
    Column("mx_cost_comp_number_5", "NUMBER(38,2)"),
    Column("mx_cost_comp_number_6", "NUMBER(38,2)"),
    Column("mx_cost_comp_number_7", "NUMBER(38,2)"),
    Column("mx_cost_comp_number_8", "NUMBER(38,2)"),
    Column("eu_imu", "NUMBER(38,2)"),
    Column("eu_outfit_cost_dollar", "NUMBER(38,2)"),
    Column("eu_top_cost_dollar", "NUMBER(38,2)"),
    Column("eu_bra_jacket_cost_dollar", "NUMBER(38,2)"),
    Column("eu_bottom_cost_dollar", "NUMBER(38,2)"),
    Column("eu_cost_comp_number_4", "NUMBER(38,2)"),
    Column("eu_cost_comp_number_5", "NUMBER(38,2)"),
    Column("eu_cost_comp_number_6", "NUMBER(38,2)"),
    Column("eu_cost_comp_number_7", "NUMBER(38,2)"),
    Column("eu_cost_comp_number_8", "NUMBER(38,2)"),
    Column("uk_imu", "NUMBER(38,2)"),
    Column("uk_outfit_cost_dollar", "NUMBER(38,2)"),
    Column("uk_top_cost_dollar", "NUMBER(38,2)"),
    Column("uk_bra_jacket_cost_dollar", "NUMBER(38,2)"),
    Column("uk_bottom_cost_dollar", "NUMBER(38,2)"),
    Column("uk_cost_comp_number_4", "NUMBER(38,2)"),
    Column("uk_cost_comp_number_5", "NUMBER(38,2)"),
    Column("uk_cost_comp_number_6", "NUMBER(38,2)"),
    Column("uk_cost_comp_number_7", "NUMBER(38,2)"),
    Column("uk_cost_comp_number_8", "NUMBER(38,2)"),
    Column("rtl_imu", "NUMBER(38,2)"),
    Column("rtl_outfit_cost_dollar", "NUMBER(38,2)"),
    Column("rtl_top_cost_dollar", "NUMBER(38,2)"),
    Column("rtl_bra_jacket_cost_dollar", "NUMBER(38,2)"),
    Column("rtl_bottom_cost_dollar", "NUMBER(38,2)"),
    Column("rtl_cost_comp_number_4", "NUMBER(38,2)"),
    Column("rtl_cost_comp_number_5", "NUMBER(38,2)"),
    Column("rtl_cost_comp_number_6", "NUMBER(38,2)"),
    Column("rtl_cost_comp_number_7", "NUMBER(38,2)"),
    Column("rtl_cost_comp_number_8", "NUMBER(38,2)"),
]

sheets = [
    SheetConfig(
        sheet_name="Items",
        schema="excel",
        table="fl_merch_items_ubt",
        header_rows=5,
        column_list=item_sheet_column_list,
    ),
    SheetConfig(
        sheet_name="Outfits",
        schema="excel",
        table="fl_merch_outfits_ubt",
        header_rows=4,
        column_list=outfit_sheet_column_list,
    ),
]

sheets_womens = [
    SheetConfig(
        sheet_name="Items",
        schema="excel",
        table="fl_merch_items_ubt",
        header_rows=5,
        column_list=women_item_sheet_column_list,
    ),
    SheetConfig(
        sheet_name="Outfits",
        schema="excel",
        table="fl_merch_outfits_ubt",
        header_rows=4,
        column_list=women_outfit_sheet_column_list,
    ),
]

hierarchy_sheets = [
    SheetConfig(
        sheet_name="Items",
        schema="excel",
        table="fl_merch_items_ubt_hierarchy",
        header_rows=5,
        column_list=item_sheet_column_list,
    ),
    SheetConfig(
        sheet_name="Outfits",
        schema="excel",
        table="fl_merch_outfits_ubt_hierarchy",
        header_rows=4,
        column_list=outfit_sheet_column_list,
    ),
]

hierarchy_sheets_womens = [
    SheetConfig(
        sheet_name="Items",
        schema="excel",
        table="fl_merch_items_ubt_hierarchy",
        header_rows=5,
        column_list=women_item_sheet_column_list,
    ),
    SheetConfig(
        sheet_name="Outfits",
        schema="excel",
        table="fl_merch_outfits_ubt_hierarchy",
        header_rows=4,
        column_list=women_outfit_sheet_column_list,
    ),
]


default_args = {
    "start_date": pendulum.datetime(2020, 3, 1, tz="America/Los_Angeles"),
    "retries": 1,
    "owner": owners.data_integrations,
    "email": email_lists.fabletics_analytics_support
    + email_lists.fabletics_merch_analytics_support,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id="edm_inbound_ubt_merch_attributes",
    default_args=default_args,
    schedule="0 5,10,13,15,17,20 * * *",
    catchup=False,
    max_active_tasks=100,
    max_active_runs=1,
)


@dataclass
class UbtConfig:
    task_id: str
    smb_path: str
    sheet_name_suffix: str
    ingestion_type: str = "ubt"

    @property
    def to_s3(self):
        def get_sheet_config_list():
            if self.ingestion_type == "ubt":
                if self.sheet_name_suffix == "_Womens":
                    return sheets_womens
                else:
                    return sheets
            if self.ingestion_type == "ubt_hierarchy":
                if self.sheet_name_suffix == "_Womens":
                    return hierarchy_sheets_womens
                else:
                    return hierarchy_sheets

        sheet_config_list = get_sheet_config_list()

        return ExcelSMBToS3Operator(
            task_id=self.task_id,
            smb_path=self.smb_path,
            share_name="JustFab",
            bucket=s3_buckets.tsos_da_int_inbound,
            s3_conn_id=conn_ids.S3.tsos_da_int_prod,
            smb_conn_id=conn_ids.SMB.app01,
            sheet_configs=[
                SheetConfig(
                    sheet_name=f"{sheet.sheet_name}{self.sheet_name_suffix}",
                    schema=sheet.schema,
                    table=sheet.table,
                    header_rows=sheet.header_rows,
                    column_list=sheet.column_list,
                )
                for sheet in sheet_config_list
            ],
            remove_header_new_lines=True,
            default_schema_version="v2",
        )


with dag:
    women_ubt_to_s3 = UbtConfig(
        task_id="women_ubt_excel_to_s3",
        smb_path="Fabletics/Shared_Documents/Bible Info/WOMENS UBT REVAMP 2.2024.xlsx",
        sheet_name_suffix="_Womens",
    ).to_s3

    men_ubt_to_s3 = UbtConfig(
        task_id="men_ubt_excel_to_s3",
        smb_path="Fabletics/Shared_Documents/Bible Info/MENS UBT REVAMP 2.2024.xlsx",
        sheet_name_suffix="_Mens",
    ).to_s3

    scrubs_ubt_to_s3 = UbtConfig(
        task_id="scrubs_ubt_excel_to_s3",
        smb_path="Fabletics/Shared_Documents/Bible Info/SCRUBS UBT REVAMP 2.2024.xlsx",
        sheet_name_suffix="_Scrubs",
    ).to_s3

    shapewear_ubt_to_s3 = UbtConfig(
        task_id="shapewear_ubt_excel_to_s3",
        smb_path="Fabletics/Shared_Documents/Bible Info/SHAPEWEAR UBT REVAMP 2.2024.xlsx",
        sheet_name_suffix="_Shapewear",
    ).to_s3

    fl_items_ubt_to_snowflake = SnowflakeProcedureOperator(
        database="lake",
        procedure="excel.fl_merch_items_ubt.sql",
        warehouse="DA_WH_ETL_LIGHT",
    )
    fl_outfits_ubt_to_snowflake = SnowflakeProcedureOperator(
        database="lake",
        procedure="excel.fl_merch_outfits_ubt.sql",
        warehouse="DA_WH_ETL_LIGHT",
    )

    chain_tasks(
        [shapewear_ubt_to_s3, women_ubt_to_s3, men_ubt_to_s3, scrubs_ubt_to_s3],
        [fl_items_ubt_to_snowflake, fl_outfits_ubt_to_snowflake],
    )

    # UBT Hierarchy November Ingestion

    women_ubt_hierarchy_to_s3 = UbtConfig(
        task_id="women_ubt_hierarchy_excel_to_s3",
        smb_path="Fabletics/Shared_Documents/Bible Info/WOMENS UBT HIERARCHY REVAMP NOV 2024.xlsx",
        sheet_name_suffix="_Womens",
        ingestion_type="ubt_hierarchy",
    ).to_s3

    men_ubt_hierarchy_to_s3 = UbtConfig(
        task_id="men_ubt_hierarchy_excel_to_s3",
        smb_path="Fabletics/Shared_Documents/Bible Info/MENS UBT HIERARCHY REVAMP NOV 24.xlsx",
        sheet_name_suffix="_Mens",
        ingestion_type="ubt_hierarchy",
    ).to_s3

    scrubs_ubt_hierarchy_to_s3 = UbtConfig(
        task_id="scrubs_ubt_hierarchy_excel_to_s3",
        smb_path="Fabletics/Shared_Documents/Bible Info/SCRUBS UBT HIERARCHY REVAMP NOV 24.xlsx",
        sheet_name_suffix="_Scrubs",
        ingestion_type="ubt_hierarchy",
    ).to_s3

    shapewear_ubt_hierarchy_to_s3 = UbtConfig(
        task_id="shapewear_ubt_hierarchy_excel_to_s3",
        smb_path="Fabletics/Shared_Documents/Bible Info/SHAPEWEAR UBT HIERARCHY "
        "REVAMP NOV 24.xlsx",
        sheet_name_suffix="_Shapewear",
        ingestion_type="ubt_hierarchy",
    ).to_s3

    fl_items_ubt_hierarchy_to_snowflake = SnowflakeProcedureOperator(
        database="lake",
        procedure="excel.fl_merch_items_ubt_hierarchy.sql",
        warehouse="DA_WH_ETL_LIGHT",
    )
    fl_outfits_ubt_hierarchy_to_snowflake = SnowflakeProcedureOperator(
        database="lake",
        procedure="excel.fl_merch_outfits_ubt_hierarchy.sql",
        warehouse="DA_WH_ETL_LIGHT",
    )

    chain_tasks(
        [
            shapewear_ubt_hierarchy_to_s3,
            women_ubt_hierarchy_to_s3,
            men_ubt_hierarchy_to_s3,
            scrubs_ubt_hierarchy_to_s3,
        ],
        [fl_items_ubt_hierarchy_to_snowflake, fl_outfits_ubt_hierarchy_to_snowflake],
    )
