import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional, Type, Union

import pandas as pd
import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_gsc
from include.airflow.hooks.s3 import S3Hook
from include.airflow.hooks.smb import SMBHook
from include.airflow.operators.excel_smb import ExcelSMBToS3BatchOperator, SheetConfig
from include.airflow.operators.snowflake_load import (
    SnowflakeCopyTableOperator,
    SnowflakeInsertOperator,
)
from include.config import conn_ids, email_lists, owners, s3_buckets, stages
from include.utils.snowflake import Column, CopyConfigCsv

column_list = [
    Column("carrier", "VARCHAR", source_name="Carrier"),
    Column("invoice_number", "VARCHAR", source_name="Inv#"),
    Column("invoice_date", "DATE", source_name="Invoice Date"),
    Column("post_advice", "VARCHAR", source_name="Post-advice"),
    Column("container", "VARCHAR", source_name="Container"),
    Column("traffic_mode", "VARCHAR", source_name="Traffic Mode"),
    Column("bl", "VARCHAR", source_name="BL"),
    Column("size", "VARCHAR", source_name="Size"),
    Column("cbm", "NUMBER(38,4)", source_name="CBM"),
    Column("weight", "NUMBER(38,4)", source_name="Weight"),
    Column("po_number", "VARCHAR", source_name="PO Number"),
    Column("pol", "VARCHAR", source_name="POL"),
    Column("pol_etd", "DATE", source_name="POL ETD"),
    Column("pod", "VARCHAR", source_name="POD"),
    Column("fnd", "VARCHAR", source_name="FND"),
    Column("description", "VARCHAR", source_name="Description"),
    Column("unit", "NUMBER(38,4)", source_name="Unit"),
    Column("charge_per_unit", "NUMBER(38,2)", source_name="Charge/Unit"),
    Column("exchange_rate", "NUMBER(38,6)", source_name="Exchange Rate"),
    Column("amount_usd", "NUMBER(38,2)", source_name="AMOUNT (USD)"),
]


def get_pandas_dtype(column_list):
    dtype = {}
    for column in column_list:
        if column.type.upper() == "VARCHAR":
            dtype[column.source_name] = "str"
    return dtype


dtype = get_pandas_dtype(column_list)


sheets = {
    "oocl_shipments": SheetConfig(
        sheet_name="Sheet1",
        schema="excel",
        table="carrier_invoice_oocl_shipments",
        s3_replace=False,
        header_rows=1,
        column_list=column_list,
        dtype=dtype,
    ),
    "oocl": SheetConfig(
        sheet_name="Sheet1",
        schema="excel",
        table="carrier_invoice_oocl",
        s3_replace=False,
        header_rows=1,
        column_list=column_list,
        dtype=dtype,
    ),
    "carrier": SheetConfig(
        sheet_name="Sheet1",
        schema="excel",
        table="carrier_invoice_carrier",
        s3_replace=False,
        header_rows=1,
        column_list=column_list,
        dtype=dtype,
    ),
    "global_carrier": SheetConfig(
        sheet_name="Sheet1",
        schema="excel",
        table="carrier_invoice_global_carrier",
        s3_replace=False,
        header_rows=1,
        column_list=column_list + [Column("code", "VARCHAR", source_name="CODE")],
        dtype=dtype,
    ),
    "crossdock": SheetConfig(
        sheet_name="Sheet1",
        schema="excel",
        table="carrier_invoice_crossdock",
        header_rows=1,
        column_list=[
            Column("load_number", "NUMBER(38,0)", uniqueness=True),
            Column("brand", "STRING"),
            Column("type", "STRING"),
            Column("date_collection", "DATE"),
            Column("date_delivery", "DATE", delta_column=True),
            Column("lovos", "STRING"),
            Column("plates", "STRING"),
            Column("rate", "NUMBER(38,4)"),
            Column("fuel_charge", "NUMBER(38,4)"),
            Column("total", "NUMBER(38,4)"),
            Column("business_unit", "STRING"),
            Column("cargo_per", "NUMBER(20,4)"),
            Column("invoice_number", "STRING", uniqueness=True),
            Column("invoice_date", "DATE", delta_column=True),
        ],
    ),
    "screen_printing": SheetConfig(
        sheet_name="Sheet1",
        schema="excel",
        table="carrier_invoice_screen_printing",
        header_rows=1,
        column_list=[
            Column("po_number", "STRING", source_name="PO_NUMBER"),
            Column("sku", "STRING", source_name="SKU"),
            Column("description", "STRING", source_name="DESCRIPTION"),
            Column("qty", "NUMBER", source_name="QTY"),
            Column("uom", "STRING", source_name="UOM"),
            Column("unit_weight_kg", "NUMBER(38, 2)", source_name="UNIT WEIGHT KG"),
            Column("total_weight_kg", "NUMBER(38, 2)", source_name="TOTAL WEIGHT KG"),
            Column("country_of_origin", "STRING", source_name="COUNTRY OF ORIGIN"),
            Column("hts_code", "STRING", source_name="HTS CODE"),
            Column("invoice_number", "STRING", source_name="INVOICE_NUMBER"),
            Column("INVOICE_DATE", "date", source_name="INVOICE DATE"),
            Column("total_duty_cost", "NUMBER(38, 4)", source_name="TOTAL DUTY COST"),
            Column(
                "total_freight_cost", "NUMBER(38, 4)", source_name="TOTAL FREIGHT COST"
            ),
            Column("total_invoice", "NUMBER(38, 4)", source_name="TOTAL INVOICED"),
        ],
    ),
}

email_list = email_lists.data_integration_support
email_list.append("calcorta@techstyle.com")
email_list.append("mgarza@techstyle.com")
default_args = {
    "start_date": pendulum.datetime(2021, 2, 1, tz="America/Los_Angeles"),
    "retries": 2,
    "owner": owners.data_integrations,
    "email": email_list,
    "on_failure_callback": slack_failure_gsc,
}

dag = DAG(
    dag_id="global_apps_inbound_landed_cost_carrier_invoice",
    default_args=default_args,
    schedule="1 * * * *",
    catchup=False,
    max_active_tasks=100,
    max_active_runs=1,
)


class ExcelSMBToS3BatchInvoiceOperator(ExcelSMBToS3BatchOperator):
    def __init__(
        self,
        get_invoice_details: bool = False,
        **kwargs,
    ):
        self.get_invoice_details = get_invoice_details
        super().__init__(**kwargs)

    def set_invoice_details(self, smb_client, file_name):
        with tempfile.TemporaryDirectory() as td:
            local_excel_path = (Path(td) / Path(file_name).name).as_posix()
            with open(local_excel_path, "wb") as file:
                smb_client.retrieveFile(
                    service_name=self.share_name, path=file_name, file_obj=file
                )
            print("Downloaded file to get invoice details: ", Path(file_name).name)

            for sheet in self.sheet_configs:
                df = pd.read_excel(
                    local_excel_path,
                    sheet_name=sheet.sheet_name,
                    usecols="B",
                    skiprows=4,
                    header=None,
                    nrows=2,
                )
                sheet.add_meta_cols = {
                    "invoice_number": df.values[1][0],
                    "invoice_date": df.values[0][0],
                }

    def execute(self, context=None):
        smb_hook = SMBHook(smb_conn_id=self.smb_conn_id)
        s3_hook = S3Hook(self.s3_conn_id)

        with smb_hook.get_conn() as smb_client:
            files = self.get_file_list(smb_client)

            for file_name in files:
                if self.get_invoice_details:
                    self.set_invoice_details(
                        smb_client=smb_client,
                        file_name=Path(self.smb_dir, file_name).as_posix(),
                    )
                self.load_file(
                    s3_hook=s3_hook,
                    smb_client=smb_client,
                    file_name=Path(self.smb_dir, file_name).as_posix(),
                )

            if self.is_archive_file:
                for file_name in files:
                    self.archive_file(
                        smb_client=smb_client, smb_dir=self.smb_dir, file_name=file_name
                    )


@dataclass
class ExcelConfig:
    task_id: str
    smb_dir: str
    sheet_config: Any
    get_invoice_details: bool = False
    load_operator: Type[Union[SnowflakeInsertOperator, SnowflakeCopyTableOperator]] = (
        SnowflakeInsertOperator
    )
    custom_select: Optional[str] = None

    @property
    def to_s3(self):
        return ExcelSMBToS3BatchInvoiceOperator(
            task_id=f"{self.task_id}_excel_to_s3",
            smb_dir=self.smb_dir,
            share_name="BI",
            file_pattern_list=["*.xls*"],
            bucket=s3_buckets.tsos_da_int_inbound,
            s3_conn_id=conn_ids.S3.tsos_da_int_prod,
            smb_conn_id=conn_ids.SMB.nas01,
            is_archive_file=True,
            sheet_configs=[self.sheet_config],
            remove_header_new_lines=True,
            get_invoice_details=self.get_invoice_details,
            default_schema_version="v2",
        )

    @property
    def to_snowflake(self):
        return self.load_operator(
            task_id=f"{self.task_id}_s3_to_snowflake",
            database="lake",
            schema=self.sheet_config.schema,
            table=self.sheet_config.table,
            staging_database="lake_stg",
            view_database="lake_view",
            snowflake_conn_id=conn_ids.Snowflake.default,
            column_list=self.sheet_config.column_list,
            files_path=f"{stages.tsos_da_int_inbound}/lake/{self.sheet_config.schema}.{self.sheet_config.table}/v2/",
            copy_config=CopyConfigCsv(
                header_rows=1, field_delimiter="|", custom_select=self.custom_select
            ),
            custom_select=self.custom_select,
        )


smb_path = "Inbound/airflow.landed_cost/carrier_invoice"

landed_cost_config = [
    ExcelConfig(
        task_id="carrier_invoice_oocl",
        smb_dir=f"{smb_path}/oocl_duty",
        sheet_config=sheets["oocl"],
        get_invoice_details=False,
    ),
    ExcelConfig(
        task_id="carrier_invoice_oocl_shipments",
        smb_dir=f"{smb_path}/oocl_freight",
        sheet_config=sheets["oocl_shipments"],
        get_invoice_details=False,
    ),
    ExcelConfig(
        task_id="carrier_invoice_carrier",
        smb_dir=f"{smb_path}/carrier_freight",
        sheet_config=sheets["carrier"],
    ),
    ExcelConfig(
        task_id="global_carrier_invoice_carrier",
        smb_dir=f"{smb_path}/global_carrier",
        sheet_config=sheets["global_carrier"],
    ),
    ExcelConfig(
        task_id="carrier_invoice_crossdock",
        smb_dir=f"{smb_path}/crossdock",
        sheet_config=sheets["crossdock"],
    ),
    ExcelConfig(
        task_id="carrier_invoice_screen_printing",
        smb_dir=f"{smb_path}/screen_printing",
        sheet_config=sheets["screen_printing"],
    ),
]

with dag:
    for cfg in landed_cost_config:
        to_s3 = cfg.to_s3
        to_snowflake = cfg.to_snowflake

        to_s3 >> to_snowflake
