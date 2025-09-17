import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import pendulum
from airflow import DAG
from include.config import owners
from include.airflow.hooks.s3 import S3Hook
from include.airflow.hooks.smb import SMBHook
from include.airflow.operators.excel_smb import ExcelSMBToS3BatchOperator, SheetConfig
from include.airflow.operators.snowflake_load import SnowflakeInsertOperator
from include.config import conn_ids, s3_buckets, stages
from include.utils.snowflake import Column, CopyConfigCsv

sheets = {
    "eu_data": SheetConfig(
        sheet_name=0,
        schema='fpa',
        table='eu_fpa_forecast',
        s3_replace=False,
        header_rows=1,
        column_list=[
            Column('store_name', 'varchar', source_name='Store Name'),
            Column('total_credit_billings', 'number(38,10)', source_name='total credit billings'),
            Column('m2_tenure_vips', 'number(38,10)', source_name='m2 tenure vips'),
            Column('m2_plus_vips', 'number(38,10)', source_name='m2 + tenure vips '),
            Column('m2_credit_billings', 'number(38,10)', source_name='m2 credit billings'),
            Column('m2_plus_credit_billings', 'number(38,10)', source_name='m2+ credit billings'),
            Column('m2_billing_rate', 'number(38,10)', source_name='m2 billing rate'),
            Column('m2_plus_billing_rate', 'number(38,10)', source_name='m2+ billing rate'),
            Column('billing_month', 'DATE'),
        ],
    ),
    "na_data": SheetConfig(
        sheet_name=0,
        schema='fpa',
        table='na_fpa_forecast',
        s3_replace=False,
        header_rows=1,
        column_list=[
            Column('store_name', 'varchar', source_name='Store Name'),
            Column('total_credit_billings', 'number(38,10)', source_name='total credit billings'),
            Column('m2_tenure_vips', 'number(38,10)', source_name='m2 tenure vips'),
            Column('m2_plus_vips', 'number(38,10)', source_name='m2 + tenure vips '),
            Column('m2_credit_billings', 'number(38,10)', source_name='m2 credit billings'),
            Column('m2_plus_credit_billings', 'number(38,10)', source_name='m2+ credit billings'),
            Column('billing_month', 'DATE'),
        ],
    ),
}

smb_path = 'Inbound/airflow.finance_ingestions'
share_name = 'BI'


class ExcelSMBToS3BatchHyperionFinanceOperator(ExcelSMBToS3BatchOperator):
    def __init__(
        self,
        get_billing_month: bool = True,
        usecols: str = "",
        billing_month_col: str = 'C',
        **kwargs,
    ):
        self.get_billing_month = get_billing_month
        self.usecols = usecols
        self.billing_month_col = billing_month_col
        super().__init__(**kwargs)

    def set_billing_month(self, smb_client, file_name):
        with tempfile.TemporaryDirectory() as td:
            local_excel_path = (Path(td) / Path(file_name).name).as_posix()
            with open(local_excel_path, 'wb') as file:
                smb_client.retrieveFile(service_name=self.share_name, path=file_name, file_obj=file)
            print('Downloaded file to get billing month details: ', Path(file_name).name)

            for sheet in self.sheet_configs:
                df = pd.read_excel(
                    local_excel_path,
                    sheet_name=sheet.sheet_name,
                    usecols=self.billing_month_col,
                    skiprows=0,
                    header=None,
                    nrows=1,
                )

                if 1 in df.columns or '1' in df.columns:
                    bm = df[1].dt.normalize()[0]
                    bm = bm.replace(day=1)
                    df['billing_month'] = bm
                else:
                    bm = df[2].dt.normalize()[0]
                    bm = bm.replace(day=1)
                    df['billing_month'] = bm
                sheet.add_meta_cols = {'billing_month': df['billing_month'][0]}

    def read_excel(self, local_excel_path, sheet_name, usecols, header, dtype, skip_footer):
        usecols = self.usecols
        return pd.read_excel(
            local_excel_path,
            sheet_name=sheet_name,
            usecols=usecols,
            header=header,
            dtype=dtype,
            skipfooter=skip_footer,
        )

    def execute(self, context=None):
        smb_hook = SMBHook(smb_conn_id=self.smb_conn_id)
        s3_hook = S3Hook(self.s3_conn_id)

        with smb_hook.get_conn() as smb_client:
            files = self.get_file_list(smb_client)

            for file_name in files:
                if self.get_billing_month:
                    self.set_billing_month(
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
    file_pattern_list: list
    usecols: str
    procedure_name: str
    billing_month_col: str
    is_archive_file: bool = True

    @property
    def to_s3(self):
        return ExcelSMBToS3BatchHyperionFinanceOperator(
            task_id=f"{self.task_id}_excel_to_s3",
            smb_dir=self.smb_dir,
            share_name=share_name,
            file_pattern_list=self.file_pattern_list,
            bucket=s3_buckets.tsos_da_int_inbound,
            s3_conn_id=conn_ids.S3.tsos_da_int_prod,
            smb_conn_id=conn_ids.SMB.nas01,
            is_archive_file=self.is_archive_file,
            sheet_configs=[self.sheet_config],
            usecols=self.usecols,
            remove_header_new_lines=False,
            billing_month_col=self.billing_month_col,
            default_schema_version='v2',
        )

    @property
    def file_path(self):
        return (
            f"{stages.tsos_da_int_inbound}/lake/{self.sheet_config.schema}."
            f"{self.sheet_config.table}/"
        )

    @property
    def to_snowflake(self):
        return SnowflakeInsertOperator(
            task_id=f"{self.task_id}_s3_to_snowflake",
            database='lake',
            schema=self.sheet_config.schema,
            table=self.sheet_config.table,
            staging_database='lake_stg',
            view_database='lake_view',
            snowflake_conn_id=conn_ids.Snowflake.default,
            column_list=self.sheet_config.column_list,
            files_path=f"{stages.tsos_da_int_inbound}/lake/{self.sheet_config.schema}.{self.sheet_config.table}/v2/",
            copy_config=CopyConfigCsv(header_rows=1, field_delimiter='|'),
        )


default_args = {
    "owner": owners.data_integrations,
}

dag = DAG(
    dag_id='edm_inbound_hyperion_credit_billings',
    default_args=default_args,
    start_date=pendulum.datetime(2021, 10, 1, tz='America/Los_Angeles'),
    catchup=False,
    schedule='0 6,8,10,12 2 * *',
)

na_data_config = ExcelConfig(
    task_id='hyperion_credit_billings_na',
    smb_dir=f"{smb_path}/lake.fpa.na_credit_billing_forecast",
    sheet_config=sheets['na_data'],
    file_pattern_list=['*.xls*'],
    procedure_name='fpa.na_fpa_forecast.sql',
    usecols='A:F',
    billing_month_col='B',
)

eu_data_config = ExcelConfig(
    task_id='hyperion_credit_billings_eu',
    smb_dir=f"{smb_path}/lake.fpa.eu_credit_billing_forecast",
    sheet_config=sheets['eu_data'],
    file_pattern_list=['*.xls*'],
    procedure_name='fpa.eu_fpa_forecast.sql',
    usecols='A,C,E:J',
    billing_month_col='C',
)

with dag:
    na_to_s3 = na_data_config.to_s3
    na_to_snowflake = na_data_config.to_snowflake

    eu_to_s3 = eu_data_config.to_s3
    eu_to_snowflake = eu_data_config.to_snowflake

    na_to_s3 >> na_to_snowflake
    eu_to_s3 >> eu_to_snowflake
