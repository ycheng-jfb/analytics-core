from typing import Dict, Iterable

import pendulum
from functools import cached_property
import pandas as pd
from include.airflow.hooks.rokt import RoktHook
from include.airflow.operators.rows_to_s3 import BaseRowsToS3CsvOperator


class RoktToS3Operator(BaseRowsToS3CsvOperator):
    """
    Base Operator for loading Commission Junction report metrics into S3.
    """

    template_fields = [
        "request_params",
        "key",
    ]

    def __init__(
        self,
        request_params: Dict,
        rokt_conn_id: str,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.request_params = request_params
        self.rokt_conn_id = rokt_conn_id

    @cached_property
    def hook(self):
        return RoktHook(rokt_conn_id=self.rokt_conn_id)

    def build_config(
        self,
        date: str,
        end_date: str,
    ) -> dict:
        params = {
            "dateStart": date,
            "dateEnd": end_date,
            "currency": "USD",
            "timeZoneVariation": "America/Los_Angeles",
            "groupby": "creative",
        }

        return params

    @property
    def report_date_list(self):
        date_dt_list = pd.date_range(
            start=self.request_params['dateStart'], end=self.request_params['dateEnd'], freq="D"
        )
        date_list = [x.isoformat()[0:10] for x in date_dt_list]
        return date_list

    def get_rows(self) -> Iterable[Dict]:
        print(f"request params are {self.request_params}")
        for date in self.report_date_list:
            end_date = str(pendulum.from_format(date, 'YYYY-MM-DD').add(days=1).date())
            extra_cols = dict(
                dateStart=date,
                dateEnd=end_date,
                accountId=self.request_params['accountId'],
                campaignId=self.request_params['campaignId'],
            )
            response = self.hook.make_request(
                method='GET',
                endpoint=f"/reporting/accounts/{self.request_params['accountId']}/campaigns/{self.request_params['campaignId']}/breakdown",
                params=self.build_config(date, end_date),
            )
            data = response.json()

            if len(data['data']['items']) > 0:
                for record in data['data']['items']:
                    yield dict(record, **extra_cols)
            else:
                self.log.warning(data)
                self.log.warning(f'unable to pull records for {self.build_config(date, end_date)} ')
