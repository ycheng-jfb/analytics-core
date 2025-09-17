import csv
import fnmatch
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, List, Optional

import pandas as pd
import pendulum
from airflow.models import BaseOperator, SkipMixin
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from include.airflow.hooks.smb import SMBHook


@dataclass
class SheetConfig:
    """
    Args:
        sheet_name: Name of the sheet or index number of sheet in the excel file
        schema: Snowflake schema name
        table: Snowflake table name
        column_list: List of columns that are to be created in Snowflake
        header_rows: Number of header rows to be skipped
        footer_rows: Number of footer rows to be skipped
        add_meta_cols: additional meta cols to be added to file
    """

    sheet_name: Any
    schema: str
    table: str
    column_list: list
    header_rows: int
    footer_rows: int = 0
    dtype: Optional[dict] = None
    add_meta_cols: Optional[dict] = None
    s3_replace: bool = True
    default_schema_version: str = "v1"

    def __post_init__(self):
        if not self.add_meta_cols:
            self.add_meta_cols = {}
        self.number_of_columns = len(
            [x for x in self.column_list if x.name not in self.add_meta_cols]
        )
        if not self.s3_replace:
            utc_time = (
                pendulum.DateTime.utcnow()
                .isoformat()[0:-6]
                .replace("-", "")
                .replace(":", "")
            )
            self.s3_key = (
                f"lake/{self.schema}.{self.table}/{self.default_schema_version}/"
                f"{str(self.sheet_name).lower()}_{utc_time}.csv.gz"
            )  # noqa: E501
        else:
            self.s3_key = (
                f"lake/{self.schema}.{self.table}/{self.default_schema_version}/"
                f"{str(self.sheet_name).lower()}.csv.gz"
            )


class ExcelSMBToS3Operator(BaseOperator):
    """
    Copies an excel file data in smb shared location to S3 as csv format per spreadsheet.

    Args:
        smb_conn_id: Reference to specific smb conn id
        bucket: S3 bucket to load data to
        smb_path: Path of the excel file that is to be processed
        share_name: The name of share in the server
        sheet_configs: An object of class:`SheetConfig` which defines sheet properties
        s3_conn_id: reference to specific s3 conn id
    """

    def __init__(
        self,
        smb_conn_id: str,
        bucket: str,
        smb_path: str,
        share_name: str,
        sheet_configs: List[SheetConfig],
        s3_conn_id: str,
        remove_header_new_lines: bool = False,
        is_archive_file: bool = False,
        archive_folder: str = "archive",
        default_schema_version: str = "v1",
        *args,
        **kwargs,
    ):
        self.smb_conn_id = smb_conn_id
        self.smb_path = smb_path
        self.share_name = share_name
        self.bucket = bucket
        self.s3_conn_id = s3_conn_id
        self.sheet_configs = sheet_configs
        self.remove_header_new_lines = remove_header_new_lines
        self.is_archive_file = is_archive_file
        self.archive_folder = archive_folder
        self.default_schema_version = default_schema_version
        super().__init__(*args, **kwargs)

    @staticmethod
    def read_excel(local_excel_path, sheet_name, usecols, header, dtype, skip_footer):
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
                    s3_key = (
                        f"lake/{sheet.schema}.{sheet.table}/{self.default_schema_version}/"
                        f"{s3_file_name.lower()}_{utc_time}.csv.gz"
                    )

                else:
                    s3_key = (
                        f"lake/{sheet.schema}.{sheet.table}/{self.default_schema_version}/"
                        f"{s3_file_name.lower()}.csv.gz"
                    )

                s3_hook.load_file(
                    filename=local_path,
                    key=s3_key,
                    bucket_name=self.bucket,
                    replace=True,
                )
                print("Loaded sheet: ", sheet.sheet_name)

    def archive_file(self, smb_client, smb_dir, file_name):
        utc_time = (
            pendulum.DateTime.utcnow()
            .isoformat()[0:-6]
            .replace("-", "")
            .replace(":", "")
        )
        file_name_ts = f"{utc_time}_{file_name}"
        smb_client.rename(
            service_name=self.share_name,
            old_path=Path(smb_dir, file_name).as_posix(),
            new_path=Path(smb_dir, self.archive_folder, file_name_ts).as_posix(),
        )

    def execute(self, context):
        smb_hook = SMBHook(smb_conn_id=self.smb_conn_id)
        s3_hook = S3Hook(self.s3_conn_id)
        with smb_hook.get_conn() as smb_client:
            self.load_file(
                s3_hook=s3_hook, smb_client=smb_client, file_name=self.smb_path
            )

            if self.is_archive_file:
                smb_dir = Path(self.smb_path).parent.as_posix()
                file_name = Path(self.smb_path).name
                self.archive_file(smb_client, smb_dir, file_name)


class ExcelSMBToS3BatchOperator(ExcelSMBToS3Operator, SkipMixin):
    """
    This operator enables the transferring multiple excel files from a SMB server to Amazon S3.

    Args:
        smb_dir: The smb remote directory. This is the specified file directory for downloading the
            files from the SMB server.
        file_pattern_list: The list of file_patterns that need to be retrieved.
        skip_downstream_if_no_files: if true, downstream task will be set to skipped state if no
            files retrieved from smb directory
    """

    def __init__(
        self,
        smb_dir,
        file_pattern_list,
        skip_downstream_if_no_files=False,
        **kwargs,
    ):
        super().__init__(smb_path="", **kwargs)
        self.smb_dir = smb_dir
        self.file_pattern_list = file_pattern_list
        self.skip_downstream_if_no_files = skip_downstream_if_no_files

    def get_file_list(self, smb_client):
        file_list = [
            f.filename for f in smb_client.listPath(self.share_name, self.smb_dir)
        ]

        remote_file_list = []
        for file_pattern in self.file_pattern_list:
            remote_file_list.extend(fnmatch.filter(names=file_list, pat=file_pattern))
        return remote_file_list

    def execute(self, context=None):
        smb_hook = SMBHook(smb_conn_id=self.smb_conn_id)
        s3_hook = S3Hook(self.s3_conn_id)

        with smb_hook.get_conn() as smb_client:
            files = self.get_file_list(smb_client)

            for file_name in files:
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

        if len(files) < 1 and self.skip_downstream_if_no_files:
            print("skipping downstream immediate task as there was no files processed")
            if context:
                downstream_tasks = context["task"].get_direct_relatives(upstream=False)
                if downstream_tasks:
                    self.skip(
                        context["dag_run"],
                        context["ti"].execution_date,
                        downstream_tasks,
                    )


class ExcelSMBToS3UseColNamesOperator(ExcelSMBToS3BatchOperator):
    def read_excel(
        self, local_excel_path, sheet_name, usecols, header, dtype, skip_footer
    ):
        for sheet in self.sheet_configs:
            if sheet.sheet_name == sheet_name:
                col_names = [x.source_name for x in sheet.column_list]

                return pd.read_excel(
                    local_excel_path,
                    sheet_name=sheet_name,
                    usecols=col_names,
                    header=header,
                    dtype=dtype,
                    skipfooter=skip_footer,
                )
