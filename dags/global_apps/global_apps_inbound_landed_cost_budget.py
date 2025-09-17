from dataclasses import dataclass
from typing import Any, Type, Union

import pandas as pd
import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_gsc
from include.airflow.operators.excel_smb import ExcelSMBToS3BatchOperator, SheetConfig
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.airflow.operators.snowflake_load import (
    SnowflakeCopyTableOperator,
    SnowflakeInsertOperator,
)
from include.config import conn_ids, email_lists, owners, s3_buckets
from include.utils.snowflake import Column


class ExcelSMBToS3BudgetOperator(ExcelSMBToS3BatchOperator):
    @staticmethod
    def read_excel(local_excel_path, sheet_name, usecols, header, dtype, skip_footer):
        all_dfs = pd.read_excel(
            local_excel_path, header=header, skiprows=6, sheet_name=sheet_name
        )
        df = pd.concat(all_dfs, ignore_index=True)
        i = 0
        for name in df.columns:
            if i > 8:
                if 9 <= i <= 20:
                    new_name = "primary_freight"
                elif 21 <= i <= 32:
                    new_name = "first_cost_rate"
                elif 33 <= i <= 44:
                    new_name = "duty_rate"
                elif 45 <= i <= 56:
                    new_name = "tariff_rate"
                elif 57 <= i <= 68:
                    new_name = "unit_volume"
                elif 70 <= i <= 81:
                    new_name = "total_budget_first_cost"
                elif 82 <= i <= 93:
                    new_name = "total_budget_primary_cost"
                elif 94 <= i <= 105:
                    new_name = "total_budget_duty_cost"
                else:
                    new_name = "total_budget_tariff_cost"
                df.rename(columns={name: f"{new_name}*{str(name)}"}, inplace=True)
            i += 1

        df.drop(df.columns[69], axis=1, inplace=True)

        df = df.melt(
            id_vars=[
                "Business Unit",
                "Gender",
                "Category",
                "Class",
                "Origin Country",
                "Destination Country",
                "FC",
                "PO Incoterms",
                "Shipping Mode",
            ],
            var_name="metric",
            value_name="metric_value",
        )

        data = df["metric"].str.split("*", expand=True)
        df["metric_name"] = data[0]
        df["budget_date"] = pd.to_datetime(data[1]).dt.date
        df.drop(columns="metric", inplace=True)

        return df


sheets = {
    "budget": SheetConfig(
        sheet_name=None,
        schema="excel",
        table="landed_cost_budget",
        s3_replace=False,
        header_rows=1,
        column_list=[
            Column("business_unit", "VARCHAR", uniqueness=True),
            Column("gender", "VARCHAR", uniqueness=True),
            Column("category", "VARCHAR", uniqueness=True),
            Column("class", "VARCHAR", uniqueness=True),
            Column("origin_country", "VARCHAR", uniqueness=True),
            Column("destination_country", "VARCHAR", uniqueness=True),
            Column("fc", "VARCHAR", uniqueness=True),
            Column("po_incoterms", "VARCHAR", uniqueness=True),
            Column("shipping_mode", "VARCHAR", uniqueness=True),
            Column("metric_value", "NUMBER(38,6)"),
            Column("metric_name", "VARCHAR"),
            Column("budget_date", "DATE", uniqueness=True),
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
    dag_id="global_apps_inbound_landed_cost_budget",
    default_args=default_args,
    schedule="0 7 1 * *",
    catchup=False,
    max_active_tasks=100,
    max_active_runs=1,
)


@dataclass
class ExcelConfig:
    task_id: str
    smb_dir: str
    sheet_config: Any
    load_operator: Type[Union[SnowflakeInsertOperator, SnowflakeCopyTableOperator]] = (
        SnowflakeInsertOperator
    )

    @property
    def to_s3(self):
        return ExcelSMBToS3BudgetOperator(
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
            default_schema_version="v2",
        )

    @property
    def to_snowflake(self):
        return SnowflakeProcedureOperator(
            procedure="excel.landed_cost_budget.sql", database="lake"
        )


smb_path = "Inbound/airflow.landed_cost"

landed_cost_config = [
    ExcelConfig(
        task_id="landed_cost_budget",
        smb_dir=f"{smb_path}/budget",
        sheet_config=sheets["budget"],
    ),
]

with dag:
    for cfg in landed_cost_config:
        to_s3 = cfg.to_s3
        to_snowflake = cfg.to_snowflake

        to_s3 >> to_snowflake
