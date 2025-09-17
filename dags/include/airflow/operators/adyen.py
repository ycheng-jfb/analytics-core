import re
from functools import reduce
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
import pendulum
import requests
from airflow.hooks.base import BaseHook
from functools import cached_property

from include.airflow.hooks.s3 import S3Hook
from include.airflow.operators.snowflake import BaseSnowflakeOperator


class AdyenToS3Operator(BaseSnowflakeOperator):
    template_fields = ["report_name", "key"]

    def __init__(
        self,
        report_name,
        output_table,
        key,
        bucket,
        s3_conn_id,
        adyen_conn_id="adyen_default",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.adyen_conn_id = adyen_conn_id
        self.report_name = report_name
        self.database, self.schema, self.table = output_table.split(".")
        self.key = key
        self.bucket = bucket
        self.s3_conn_id = s3_conn_id

    @cached_property
    def creds(self):
        return BaseHook.get_connection(self.adyen_conn_id)

    @cached_property
    def engine(self):
        return self.snowflake_hook.get_sqlalchemy_engine()

    @property
    def merchant_code_to_bu_map(self):
        return {
            "FableticsDE": "Fabletics DE",
            "FableticsDK": "Fabletics DK",
            "FableticsES": "Fabletics ES",
            "FableticsFR": "Fabletics FR",
            "FableticsNL": "Fabletics NL",
            "FableticsSE": "Fabletics SE",
            "FableticsUK": "Fabletics UK",
            "JustFabDK": "JustFab DK",
            "JustFabES": "JustFab ES",
            "JustFabEU": "JustFab EU",
            "JustFabFR": "JustFab FR",
            "JustFabNL": "JustFab NL",
            "JustFabSE": "JustFab SE",
            "JustFabUK": "JustFab UK",
            "SavagexDE": "Savage X DE",
            "SavagexES": "Savage X ES",
            "SavagexEU": "Savage X EU",
            "SavagexFR": "Savage X FR",
            "SavagexUK": "Savage X UK",
        }

    def download_report(self, output_path):
        url = f"https://ca-live.adyen.com/reports/download/Company/JustFabulous/{self.report_name}"
        try:
            response = requests.get(url, auth=(self.creds.login, self.creds.password))
            response.raise_for_status()
            with open(output_path, "wb") as f:
                f.write(response.content)
        except requests.exceptions.HTTPError as e:
            if response.reason == "Not Found":
                raise Exception(f"{self.report_name} is not ready yet")
            else:
                raise e

    @staticmethod
    def get_journal_type_from_mutations(df, journal_type, column_rename_map={}):
        df_j = df[(df["Register"] == "Payable") & (df["Journal Type"] == journal_type)]
        return df_j.drop(["Register", "Journal Type"], axis=1).rename(
            column_rename_map, axis=1
        )

    def transform_report(self, input_path):
        # Get dataframes needed for desired metrics
        df_mu = pd.read_excel(
            input_path, sheet_name="Mutations", header=4, usecols="A:C,F,H"
        )
        df_pm = pd.read_excel(
            input_path,
            sheet_name="Payment Method Breakdown",
            header=4,
            usecols="B:C,G,I",
        )

        # Chargeback
        df_cb = self.get_journal_type_from_mutations(
            df_mu,
            "Chargeback",
            column_rename_map={"Count": "cb_count", "Amount": "cb_amount"},
        )

        # Chargeback Reversed
        df_cb_rev = self.get_journal_type_from_mutations(
            df_mu,
            "ChargebackReversed",
            column_rename_map={
                "Count": "cb_reversed_count",
                "Amount": "cb_reversed_amount",
            },
        )

        # Second Chargeback
        df_second_cb = self.get_journal_type_from_mutations(
            df_mu,
            "SecondChargeback",
            column_rename_map={
                "Count": "second_cb_count",
                "Amount": "second_cb_amount",
            },
        )

        # Paypal refund count and value
        df_paypal = (
            df_pm.loc[df_pm["Payment Method"] == "paypal"]
            .drop(["Payment Method"], axis=1)
            .rename(
                {
                    "Account Code": "Merchant Code",
                    "Count.1": "paypal_refund_count",
                    "Amount.1": "paypal_refund_amount",
                },
                axis=1,
            )
        )

        # Outer join all of the dataframes
        dfs = [df_cb, df_cb_rev, df_second_cb, df_paypal]
        df_final = reduce(
            lambda x, y: pd.merge(x, y, how="outer", on="Merchant Code"), dfs
        )

        # Add BU and Date column. Return columns in desired order
        df_final["bu"] = df_final["Merchant Code"].replace(self.merchant_code_to_bu_map)
        matched = re.search(r"\w+(\d{4})_(\d{2})\.xls", self.report_name)
        report_year, report_month = int(matched.group(1)), int(matched.group(2))
        df_final["report_date"] = pendulum.datetime(report_year, report_month, 1)
        output_columns = [
            "bu",
            "report_date",
            "cb_count",
            "cb_amount",
            "cb_reversed_count",
            "cb_reversed_amount",
            "second_cb_count",
            "second_cb_amount",
            "paypal_refund_count",
            "paypal_refund_amount",
        ]
        return df_final[output_columns]

    def execute(self, context=None):
        with TemporaryDirectory() as td:
            # Download the file
            file_path = Path(td) / self.report_name
            self.download_report(file_path)

            # Transform the file and upload to S3
            df = self.transform_report(file_path)
            final_file_path = file_path.as_posix().replace(".xls", ".csv")
            df.to_csv(final_file_path, index=False)
            s3_hook = S3Hook(self.s3_conn_id)
            s3_hook.load_file(
                filename=final_file_path,
                key=self.key,
                bucket_name=self.bucket,
                replace=True,
            )
