import csv
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import pendulum
from include.airflow.callbacks.slack import slack_failure_gsc
from include.airflow.operators.excel_smb import ExcelSMBToS3BatchOperator, SheetConfig
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.config import conn_ids, email_lists, owners, s3_buckets
from include.utils.snowflake import Column

from airflow import DAG

default_args = {
    "start_date": pendulum.datetime(2021, 11, 15, 7, tz="America/Los_Angeles"),
    "owner": owners.data_integrations,
    "email": email_lists.data_integration_support,
    "on_failure_callback": slack_failure_gsc,
}

dag = DAG(
    dag_id="global_apps_inbound_gms_masterfile",
    default_args=default_args,
    schedule="00 18 * * 6,0,1",
    catchup=False,
    max_active_tasks=20,
)


column_list = [
    Column("agent_id", "NUMBER(10,0)", source_name="Agent ID"),
    Column("ecom_id", "NUMBER(10,0)", source_name="Ecom ID"),
    Column("genesys_id", "VARCHAR", source_name="Genesys ID"),
    Column("agent_name", "VARCHAR", source_name="Agent Name"),
    Column("team_name", "VARCHAR", source_name="Team Name"),
    Column("team_id", "VARCHAR", source_name="Team ID"),
]

column_list_addn = [
    Column("agent_id", "NUMBER(10,0)", source_name="Agent ID"),
    Column("ecom_id", "NUMBER(10,0)", source_name="Ecom ID"),
    Column("genesys_id", "VARCHAR", source_name="Genesys ID"),
    Column("agent_name", "VARCHAR", source_name="Agent Name"),
    Column("team_name", "VARCHAR", source_name="Team Name"),
    Column("team_id", "VARCHAR", source_name="Team ID"),
    Column("attrition_date", "DATE", source_name="Attrition Date"),
]

sheets = {
    "DVO": SheetConfig(
        sheet_name="DVO",
        schema="excel",
        table="gms_master",
        s3_replace=True,
        header_rows=1,
        column_list=column_list,
    ),
    "DVO NH": SheetConfig(
        sheet_name="DVO NH",
        schema="excel",
        table="gms_master",
        s3_replace=True,
        header_rows=1,
        column_list=column_list,
    ),
    "DVO ResignedTerminated": SheetConfig(
        sheet_name="DVO ResignedTerminated",
        schema="excel",
        table="gms_master",
        s3_replace=True,
        header_rows=1,
        column_list=column_list_addn,
    ),
    "MIA": SheetConfig(
        sheet_name="MIA",
        schema="excel",
        table="gms_master",
        s3_replace=True,
        header_rows=1,
        column_list=column_list,
    ),
    "MIA NH": SheetConfig(
        sheet_name="MIA NH",
        schema="excel",
        table="gms_master",
        s3_replace=True,
        header_rows=1,
        column_list=column_list,
    ),
    "MIA ResignedTerminated": SheetConfig(
        sheet_name="MIA ResignedTerminated",
        schema="excel",
        table="gms_master",
        s3_replace=True,
        header_rows=1,
        column_list=column_list_addn,
    ),
    "BCD": SheetConfig(
        sheet_name="BCD",
        schema="excel",
        table="gms_master",
        s3_replace=True,
        header_rows=1,
        column_list=column_list,
    ),
    "BCD NH": SheetConfig(
        sheet_name="BCD NH",
        schema="excel",
        table="gms_master",
        s3_replace=True,
        header_rows=1,
        column_list=column_list,
    ),
    "BCD ResignedTerminated": SheetConfig(
        sheet_name="BCD ResignedTerminated",
        schema="excel",
        table="gms_master",
        s3_replace=True,
        header_rows=1,
        column_list=column_list_addn,
    ),
    "UK": SheetConfig(
        sheet_name="UK",
        schema="excel",
        table="gms_master",
        s3_replace=True,
        header_rows=1,
        column_list=column_list,
    ),
    "UK NH": SheetConfig(
        sheet_name="UK NH",
        schema="excel",
        table="gms_master",
        s3_replace=True,
        header_rows=1,
        column_list=column_list,
    ),
    "UK ResignedTerminated": SheetConfig(
        sheet_name="UK ResignedTerminated",
        schema="excel",
        table="gms_master",
        s3_replace=True,
        header_rows=1,
        column_list=column_list_addn,
    ),
    "EU": SheetConfig(
        sheet_name="EU",
        schema="excel",
        table="gms_master",
        s3_replace=True,
        header_rows=1,
        column_list=column_list,
    ),
    "EU NH": SheetConfig(
        sheet_name="EU NH",
        schema="excel",
        table="gms_master",
        s3_replace=True,
        header_rows=1,
        column_list=column_list,
    ),
    "EU ResignedTerminated": SheetConfig(
        sheet_name="EU ResignedTerminated",
        schema="excel",
        table="gms_master",
        s3_replace=True,
        header_rows=1,
        column_list=column_list_addn,
    ),
    "Casablanca": SheetConfig(
        sheet_name="Casablanca",
        schema="excel",
        table="gms_master",
        s3_replace=True,
        header_rows=1,
        column_list=column_list,
    ),
    "Casablanca NH": SheetConfig(
        sheet_name="Casablanca NH",
        schema="excel",
        table="gms_master",
        s3_replace=True,
        header_rows=1,
        column_list=column_list,
    ),
    "Casablanca ResignedTerminated": SheetConfig(
        sheet_name="Casablanca ResignedTerminated",
        schema="excel",
        table="gms_master",
        s3_replace=True,
        header_rows=1,
        column_list=column_list_addn,
    ),
    "Tier 2": SheetConfig(
        sheet_name="Tier 2",
        schema="excel",
        table="gms_master",
        s3_replace=True,
        header_rows=1,
        column_list=column_list,
    ),
    "Tier ResignedTerminated": SheetConfig(
        sheet_name="Tier ResignedTerminated",
        schema="excel",
        table="gms_master",
        s3_replace=True,
        header_rows=1,
        column_list=column_list_addn,
    ),
    "GMS Training": SheetConfig(
        sheet_name="GMS Training",
        schema="excel",
        table="gms_master",
        s3_replace=True,
        header_rows=1,
        column_list=column_list,
    ),
    "GMS Training ResignedTerminated": SheetConfig(
        sheet_name="GMS Training ResignedTerminated",
        schema="excel",
        table="gms_master",
        s3_replace=True,
        header_rows=1,
        column_list=column_list_addn,
    ),
    "GMS MEA": SheetConfig(
        sheet_name="GMS MEA",
        schema="excel",
        table="gms_master",
        s3_replace=True,
        header_rows=1,
        column_list=column_list,
    ),
    "GMS MEA ResignedTerminated": SheetConfig(
        sheet_name="GMS MEA ResignedTerminated",
        schema="excel",
        table="gms_master",
        s3_replace=True,
        header_rows=1,
        column_list=column_list_addn,
    ),
    "GMS EU Training Quality": SheetConfig(
        sheet_name="GMS EU Training & Quality",
        schema="excel",
        table="gms_master",
        s3_replace=True,
        header_rows=1,
        column_list=column_list,
    ),
    "GMS EU Training Quality Resign": SheetConfig(
        sheet_name="GMS EU Training&Quality Resign.",
        schema="excel",
        table="gms_master",
        s3_replace=True,
        header_rows=1,
        column_list=column_list_addn,
    ),
    "MIA GMS Training": SheetConfig(
        sheet_name="MIA GMS Training",
        schema="excel",
        table="gms_master",
        s3_replace=True,
        header_rows=1,
        column_list=column_list,
    ),
    "SMM_MSG": SheetConfig(
        sheet_name="SMM_MSG",
        schema="excel",
        table="gms_master",
        s3_replace=True,
        header_rows=1,
        column_list=column_list,
    ),
    "SMM_MSG ResignedTerminate": SheetConfig(
        sheet_name="SMM_MSG ResignedTerminate",
        schema="excel",
        table="gms_master",
        s3_replace=True,
        header_rows=1,
        column_list=column_list_addn,
    ),
    "BackOffice": SheetConfig(
        sheet_name="BackOffice",
        schema="excel",
        table="gms_master",
        s3_replace=True,
        header_rows=1,
        column_list=column_list,
    ),
    "BackOffice ResignedTerminate": SheetConfig(
        sheet_name="BackOffice ResignedTerminate",
        schema="excel",
        table="gms_master",
        s3_replace=True,
        header_rows=1,
        column_list=column_list_addn,
    ),
}

smb_path = "Inbound/airflow.gms_master"
share_name = "BI"


class ExcelSMBToS3BatchFlexiColOperator(ExcelSMBToS3BatchOperator):
    def __init__(
        self,
        usecols: str,
        **kwargs,
    ):
        self.usecols = usecols
        super().__init__(**kwargs)

    def read_excel(
        self, local_excel_path, sheet_name, usecols, header, dtype, skip_footer
    ):
        usecols = self.usecols
        return pd.read_excel(
            local_excel_path,
            sheet_name=sheet_name,
            usecols=usecols,
            header=header,
            dtype=dtype,
            skipfooter=skip_footer,
        )

    def load_file(self, smb_client, s3_hook, file_name):
        with tempfile.TemporaryDirectory() as td:
            local_excel_path = (Path(td) / Path(file_name).name).as_posix()
            with open(local_excel_path, "wb") as file:
                smb_client.retrieveFile(
                    service_name=self.share_name, path=file_name, file_obj=file
                )

            print("Downloaded file: ", Path(file_name).name)

            for sheet in self.sheet_configs:
                df = self.read_excel(
                    local_excel_path,
                    sheet_name=sheet.sheet_name,
                    usecols=range(sheet.number_of_columns),
                    header=sheet.header_rows - 1 if self.remove_header_new_lines else 0,
                    dtype=sheet.dtype,
                    skip_footer=sheet.footer_rows,
                )

                if self.remove_header_new_lines:
                    df.columns = df.columns.map(lambda x: x.replace("\n", ""))
                if sheet.add_meta_cols:
                    for k, v in sheet.add_meta_cols.items():
                        df[k] = f"{v}"

                if "Attrition Date" not in df.columns:
                    df["Attrition Date"] = ""
                df["sheet_name"] = sheet.sheet_name

                local_path = Path(
                    td, str(sheet.sheet_name).lower() + ".csv.gz"
                ).as_posix()

                df.to_csv(
                    local_path,
                    encoding="utf-8",
                    sep="|",
                    index=False,
                    quoting=csv.QUOTE_NONNUMERIC,
                    quotechar='"',
                    line_terminator="\n",
                    mode="w",
                    compression="gzip",
                )

                s3_file_name = f"{Path(file_name).stem}-{str(sheet.sheet_name)}"
                if not sheet.s3_replace:
                    utc_time = (
                        pendulum.DateTime.utcnow()
                        .isoformat()[0:-6]
                        .replace("-", "")
                        .replace(":", "")
                    )
                    s3_key = f"lake/{sheet.schema}.{sheet.table}/{self.default_schema_version}/{s3_file_name.lower()}_{utc_time}.csv.gz"  # noqa: E501

                else:
                    s3_key = f"lake/{sheet.schema}.{sheet.table}/{self.default_schema_version}/{s3_file_name.lower()}.csv.gz"  # noqa: E501

                s3_hook.load_file(
                    filename=local_path,
                    key=s3_key,
                    bucket_name=self.bucket,
                    replace=True,
                )
                print("Loaded sheet: ", sheet.sheet_name)


@dataclass
class ExcelConfig:
    task_id: str
    smb_dir: str
    sheet_config: Any
    file_pattern_list: list
    usecols: str
    is_archive_file: bool = True

    @property
    def to_s3(self):
        return ExcelSMBToS3BatchFlexiColOperator(
            task_id=f"{self.task_id}_excel_to_s3",
            smb_dir=self.smb_dir,
            share_name="BI",
            file_pattern_list=self.file_pattern_list,
            bucket=s3_buckets.tsos_da_int_inbound,
            s3_conn_id=conn_ids.S3.tsos_da_int_prod,
            smb_conn_id=conn_ids.SMB.nas01,
            is_archive_file=self.is_archive_file,
            sheet_configs=[self.sheet_config],
            usecols=self.usecols,
            remove_header_new_lines=False,
            default_schema_version="v3",
        )


smb_dir = "Inbound/airflow.gms_master"
file_pattern_list = ["*.xls*"]

gms_master_config = [
    ExcelConfig(
        task_id="dvo",
        smb_dir=smb_dir,
        sheet_config=sheets["DVO"],
        file_pattern_list=file_pattern_list,
        usecols="E:J",
        is_archive_file=False,
    ),
    ExcelConfig(
        task_id="dvo_nh",
        smb_dir=smb_dir,
        sheet_config=sheets["DVO NH"],
        file_pattern_list=file_pattern_list,
        usecols="E:J",
        is_archive_file=False,
    ),
    ExcelConfig(
        task_id="dvo_resignedterminated",
        smb_dir=smb_dir,
        sheet_config=sheets["DVO ResignedTerminated"],
        file_pattern_list=file_pattern_list,
        usecols="E:K",
        is_archive_file=False,
    ),
    ExcelConfig(
        task_id="mia",
        smb_dir=smb_dir,
        sheet_config=sheets["MIA"],
        file_pattern_list=file_pattern_list,
        usecols="E:J",
        is_archive_file=False,
    ),
    ExcelConfig(
        task_id="mia_nh",
        smb_dir=smb_dir,
        sheet_config=sheets["MIA NH"],
        file_pattern_list=file_pattern_list,
        usecols="E:J",
        is_archive_file=False,
    ),
    ExcelConfig(
        task_id="mia_resignedterminated",
        smb_dir=smb_dir,
        sheet_config=sheets["MIA ResignedTerminated"],
        file_pattern_list=file_pattern_list,
        usecols="E:K",
        is_archive_file=False,
    ),
    ExcelConfig(
        task_id="bcd",
        smb_dir=smb_dir,
        sheet_config=sheets["BCD"],
        file_pattern_list=file_pattern_list,
        usecols="E:J",
        is_archive_file=False,
    ),
    ExcelConfig(
        task_id="bcd_nh",
        smb_dir=smb_dir,
        sheet_config=sheets["BCD NH"],
        file_pattern_list=file_pattern_list,
        usecols="E:J",
        is_archive_file=False,
    ),
    ExcelConfig(
        task_id="bcd_resignedterminated",
        smb_dir=smb_dir,
        sheet_config=sheets["BCD ResignedTerminated"],
        file_pattern_list=file_pattern_list,
        usecols="E:K",
        is_archive_file=False,
    ),
    ExcelConfig(
        task_id="uk",
        smb_dir=smb_dir,
        sheet_config=sheets["UK"],
        file_pattern_list=file_pattern_list,
        usecols="E:J",
        is_archive_file=False,
    ),
    ExcelConfig(
        task_id="uk_nh",
        smb_dir=smb_dir,
        sheet_config=sheets["UK NH"],
        file_pattern_list=file_pattern_list,
        usecols="E:J",
        is_archive_file=False,
    ),
    ExcelConfig(
        task_id="uk_resignedterminated",
        smb_dir=smb_dir,
        sheet_config=sheets["UK ResignedTerminated"],
        file_pattern_list=file_pattern_list,
        usecols="E:K",
        is_archive_file=False,
    ),
    ExcelConfig(
        task_id="eu",
        smb_dir=smb_dir,
        sheet_config=sheets["EU"],
        file_pattern_list=file_pattern_list,
        usecols="E:J",
        is_archive_file=False,
    ),
    ExcelConfig(
        task_id="eu_nh",
        smb_dir=smb_dir,
        sheet_config=sheets["EU NH"],
        file_pattern_list=file_pattern_list,
        usecols="E:J",
        is_archive_file=False,
    ),
    ExcelConfig(
        task_id="eu_resignedterminated",
        smb_dir=smb_dir,
        sheet_config=sheets["EU ResignedTerminated"],
        file_pattern_list=file_pattern_list,
        usecols="E:K",
        is_archive_file=False,
    ),
    ExcelConfig(
        task_id="casablanca",
        smb_dir=smb_dir,
        sheet_config=sheets["Casablanca"],
        file_pattern_list=file_pattern_list,
        usecols="E:J",
        is_archive_file=False,
    ),
    ExcelConfig(
        task_id="casablanca_nh",
        smb_dir=smb_dir,
        sheet_config=sheets["Casablanca NH"],
        file_pattern_list=file_pattern_list,
        usecols="E:J",
        is_archive_file=False,
    ),
    ExcelConfig(
        task_id="casablanca_resignedterminated",
        smb_dir=smb_dir,
        sheet_config=sheets["Casablanca ResignedTerminated"],
        file_pattern_list=file_pattern_list,
        usecols="E:K",
        is_archive_file=False,
    ),
    ExcelConfig(
        task_id="tier2",
        smb_dir=smb_dir,
        sheet_config=sheets["Tier 2"],
        file_pattern_list=file_pattern_list,
        usecols="E:J",
        is_archive_file=False,
    ),
    ExcelConfig(
        task_id="tier_resignedterminated",
        smb_dir=smb_dir,
        sheet_config=sheets["Tier ResignedTerminated"],
        file_pattern_list=file_pattern_list,
        usecols="E:J,L",
        is_archive_file=False,
    ),
    ExcelConfig(
        task_id="gms_training",
        smb_dir=smb_dir,
        sheet_config=sheets["GMS Training"],
        file_pattern_list=file_pattern_list,
        usecols="E:J",
        is_archive_file=False,
    ),
    ExcelConfig(
        task_id="gms_training_resignedterminated",
        smb_dir=smb_dir,
        sheet_config=sheets["GMS Training ResignedTerminated"],
        file_pattern_list=file_pattern_list,
        usecols="E:K",
        is_archive_file=False,
    ),
    ExcelConfig(
        task_id="gms_mea",
        smb_dir=smb_dir,
        sheet_config=sheets["GMS MEA"],
        file_pattern_list=file_pattern_list,
        usecols="E:J",
        is_archive_file=False,
    ),
    ExcelConfig(
        task_id="gms_mea_resignedterminated",
        smb_dir=smb_dir,
        sheet_config=sheets["GMS MEA ResignedTerminated"],
        file_pattern_list=file_pattern_list,
        usecols="E:K",
        is_archive_file=False,
    ),
    ExcelConfig(
        task_id="gms_eu_training_quality",
        smb_dir=smb_dir,
        sheet_config=sheets["GMS EU Training Quality"],
        file_pattern_list=file_pattern_list,
        usecols="E:H,J,K",
        is_archive_file=False,
    ),
    ExcelConfig(
        task_id="gms_eu_training_quality_resign",
        smb_dir=smb_dir,
        sheet_config=sheets["GMS EU Training Quality Resign"],
        file_pattern_list=file_pattern_list,
        usecols="E:H,J:L",
        is_archive_file=False,
    ),
    ExcelConfig(
        task_id="mia_gms_training",
        smb_dir=smb_dir,
        sheet_config=sheets["MIA GMS Training"],
        file_pattern_list=file_pattern_list,
        usecols="E:J",
        is_archive_file=False,
    ),
    ExcelConfig(
        task_id="smm_msg",
        smb_dir=smb_dir,
        sheet_config=sheets["SMM_MSG"],
        file_pattern_list=file_pattern_list,
        usecols="E:J",
        is_archive_file=False,
    ),
    ExcelConfig(
        task_id="smm_msg_resignedterminate",
        smb_dir=smb_dir,
        sheet_config=sheets["SMM_MSG ResignedTerminate"],
        file_pattern_list=file_pattern_list,
        usecols="E:K",
        is_archive_file=False,
    ),
    ExcelConfig(
        task_id="backoffice",
        smb_dir=smb_dir,
        sheet_config=sheets["BackOffice"],
        file_pattern_list=file_pattern_list,
        usecols="E:J",
        is_archive_file=False,
    ),
]

with dag:
    to_snowflake = SnowflakeProcedureOperator(
        procedure="excel.gms_master.sql",
        database="lake",
        warehouse="DA_WH_ETL_LIGHT",
    )
    cfg = ExcelConfig(
        task_id="backoffice_resignedterminate",
        smb_dir=smb_dir,
        sheet_config=sheets["BackOffice ResignedTerminate"],
        file_pattern_list=file_pattern_list,
        usecols="E:K",
        is_archive_file=True,
    )
    cfg_s3 = cfg.to_s3
    for cfg in gms_master_config:
        to_s3 = cfg.to_s3
        to_s3 >> cfg_s3 >> to_snowflake
