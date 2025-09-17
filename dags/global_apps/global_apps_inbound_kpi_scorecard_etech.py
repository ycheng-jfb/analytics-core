import csv
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_gsc
from include.airflow.operators.email_to_smb import EmailToSMBOperator
from include.airflow.operators.excel_smb import ExcelSMBToS3BatchOperator, SheetConfig
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.config import conn_ids, email_lists, owners, s3_buckets, stages
from include.utils.snowflake import Column

sheets = {
    "kpi_scorecard": SheetConfig(
        sheet_name='all',
        schema='excel',
        table='kpi_scorecard_etech',
        s3_replace=False,
        header_rows=1,
        column_list=[
            Column('division', 'VARCHAR', uniqueness=True),
            Column('region', 'VARCHAR', uniqueness=True),
            Column('country', 'VARCHAR', uniqueness=True),
            Column('bu', 'VARCHAR', uniqueness=True),
            Column('language', 'VARCHAR', uniqueness=True),
            Column('campaign', 'VARCHAR', uniqueness=True),
            Column('date', 'DATE', uniqueness=True),
            Column('internal_qc_score_voice', 'NUMBER(38,17)'),
            Column('internal_alert_rate_voice', 'NUMBER(38,17)'),
            Column('internal_qc_score_chat_email', 'NUMBER(38,17)'),
            Column('internal_alert_rate_chat_email', 'NUMBER(38,17)'),
        ],
    ),
}


class ExcelSMBToS3BatchKpiScorecardOperator(ExcelSMBToS3BatchOperator):
    def __init__(
        self,
        **kwargs,
    ):
        super().__init__(**kwargs)

    def load_file(self, smb_client, s3_hook, file_name):
        with tempfile.TemporaryDirectory() as td:
            local_excel_path = (Path(td) / Path(file_name).name).as_posix()
            with open(local_excel_path, 'wb') as file:
                smb_client.retrieveFile(service_name=self.share_name, path=file_name, file_obj=file)

            print('Downloaded file: ', Path(file_name).name)
            master_df = pd.DataFrame(
                columns=[
                    'sheet_name',
                    'week',
                    'internal_qc_score_voice',
                    'internal_alert_rate_voice',
                    'internal_qc_score_chat_email',
                    'internal_alert_rate_chat_email',
                ]
            )
            df_sn = pd.ExcelFile(local_excel_path)

            for sheet in df_sn.sheet_names:
                df_week = pd.read_excel(
                    local_excel_path,
                    sheet_name=sheet,
                    usecols="D",
                    skiprows=4,
                    nrows=1,
                    header=None,
                )

                df_week.columns = ["week_name"]
                df = pd.read_excel(
                    local_excel_path, sheet_name=sheet, usecols="B,D", skiprows=7, header=None
                )
                df.columns = ["metric", "value"]
                df = df.pivot(columns='metric', values='value')
                column_dict = {}
                for i in list(df.columns):
                    if 'internalmaxscore-voice' in i.lower().replace(" ", ""):
                        column_dict[i] = "internal_qc_score_voice"
                    elif "internalcoachingalertrate-voice" in i.lower().replace(" ", ""):
                        column_dict[i] = "internal_alert_rate_voice"
                    elif "internalmaxscore-digitalservice" in i.lower().replace(" ", ""):
                        column_dict[i] = "internal_qc_score_chat_email"
                    elif "internalcoachingalertrate-digitalservice" in i.lower().replace(" ", ""):
                        column_dict[i] = "internal_alert_rate_chat_email"
                    else:
                        column_dict[i] = i

                df = df.rename(column_dict, axis=1)
                df = df.reindex(
                    columns=[
                        'internal_qc_score_voice',
                        'internal_alert_rate_voice',
                        'internal_qc_score_chat_email',
                        'internal_alert_rate_chat_email',
                    ]
                )
                df['sheet_name'] = sheet
                df['week'] = df_week.at[0, 'week_name']
                df = df.replace('-', np.NAN)
                df = df.groupby(['sheet_name', 'week'], as_index=False).sum(min_count=1)
                master_df = master_df.append(df, ignore_index=True)

            local_path = Path(td, 'kpi_scorecard_etech.csv.gz').as_posix()

            master_df.to_csv(
                local_path,
                encoding='utf-8',
                sep="|",
                index=False,
                quoting=csv.QUOTE_NONNUMERIC,
                quotechar='"',
                line_terminator='\n',
                mode='w',
                compression='gzip',
            )

            s3_file_name = f"{Path(file_name).stem}"
            sheet = self.sheet_configs[0]
            if not sheet.s3_replace:
                utc_time = (
                    pendulum.DateTime.utcnow().isoformat()[0:-6].replace("-", "").replace(":", "")
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
            print('Loaded File: ', s3_key)


default_args = {
    'start_date': pendulum.datetime(2021, 2, 1, tz='America/Los_Angeles'),
    'retries': 2,
    'owner': owners.data_integrations,
    'email': email_lists.data_integration_support,
    'on_failure_callback': slack_failure_gsc,
}

dag = DAG(
    dag_id='global_apps_inbound_kpi_scorecard_etech',
    default_args=default_args,
    schedule='0 9,12,15,18 * * 1',
    catchup=False,
    max_active_tasks=100,
    max_active_runs=1,
)

smb_path = 'Inbound/airflow.etech_kpi_scorecard'
share_name = 'BI'


@dataclass
class ExcelConfig:
    task_id: str
    smb_dir: str
    sheet_config: Any
    file_pattern_list: list
    is_archive_file: bool = True

    @property
    def to_s3(self):
        return ExcelSMBToS3BatchKpiScorecardOperator(
            task_id=f"{self.task_id}_excel_to_s3",
            smb_dir=self.smb_dir,
            share_name=share_name,
            file_pattern_list=self.file_pattern_list,
            bucket=s3_buckets.tsos_da_int_inbound,
            s3_conn_id=conn_ids.S3.tsos_da_int_prod,
            smb_conn_id=conn_ids.SMB.nas01,
            is_archive_file=self.is_archive_file,
            sheet_configs=[self.sheet_config],
            remove_header_new_lines=True,
            default_schema_version='v2',
        )

    @property
    def file_path(self):
        return (
            f"{stages.tsos_da_int_inbound}/lake/{self.sheet_config.schema}."
            f"{self.sheet_config.table}/v2/"
        )

    @property
    def to_snowflake(self):
        return SnowflakeProcedureOperator(
            database='lake', procedure='excel.kpi_scorecard_etech.sql', schema='excel'
        )


kpi_scorecard_config = ExcelConfig(
    task_id='kpi_scorecard',
    smb_dir=f"{smb_path}",
    sheet_config=sheets['kpi_scorecard'],
    file_pattern_list=['*.xls*'],
)

with dag:
    email_to_smb = EmailToSMBOperator(
        task_id='email_to_smb',
        remote_path=smb_path,
        smb_conn_id=conn_ids.SMB.nas01,
        from_address='@etech',
        resource_address="svc_tfg_etech@techstyle.com",
        subjects=["Global KPI Deck"],
        share_name=share_name,
    )
    to_s3 = kpi_scorecard_config.to_s3
    to_snowflake = kpi_scorecard_config.to_snowflake

    email_to_smb >> to_s3 >> to_snowflake
