from typing import Dict, Iterable

import pendulum
from functools import cached_property
import pandas as pd
from include.airflow.hooks.commission_junction import CommissionJunctionHook
from include.airflow.operators.rows_to_s3 import BaseRowsToS3CsvOperator


class CommissionJunctionToS3Operator(BaseRowsToS3CsvOperator):
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
        commissionjunction_conn_id: str,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.request_params = request_params
        self.commissionjunction_conn_id = commissionjunction_conn_id

    @cached_property
    def hook(self):
        return CommissionJunctionHook(
            commissionjunction_conn_id=self.commissionjunction_conn_id
        )

    def build_config(self, date: str) -> dict:
        end_date = pendulum.from_format(date, "YYYY-MM-DD").add(days=1)
        config = (
            self.request_params[:72]
            + date
            + 'T00:00:00Z",beforePostingDate:"'
            + str(end_date.date())
            + 'T00:00:00Z"'
            + self.request_params[134:]
        )
        return config

    @property
    def report_date_start(self):
        return self.request_params[72:82]

    @property
    def report_date_end(self):
        return self.request_params[113:123]

    @property
    def report_date_list(self):
        date_dt_list = pd.date_range(
            start=self.report_date_start, end=self.report_date_end, freq="D"
        )
        date_list = [x.isoformat()[0:10] for x in date_dt_list]
        return date_list

    def get_rows(self) -> Iterable[Dict]:
        extra_cols = dict(
            updated_at=pendulum.DateTime.utcnow().isoformat(),
        )
        print(f"request params are {self.request_params}")
        for date in self.report_date_list:
            response = self.hook.make_request(
                path="query", payload=self.build_config(date)
            )
            data = response.json()
            print(f"loading {data['data']['advertiserCommissions']['count']} to file")

            if data["data"]["advertiserCommissions"]["count"] != 0:
                for item in data["data"]["advertiserCommissions"]["records"]:
                    if item.get("verticalAttributes"):
                        item["custSegment"] = item["verticalAttributes"]["custSegment"]
                        item.pop("verticalAttributes")
                    else:
                        item["custSegment"] = item["verticalAttributes"]
                        item.pop("verticalAttributes")
                    yield dict(item, **extra_cols)
            else:
                self.log.warning(data)
                self.log.warning(
                    f"unable to pull records for {self.build_config(date)} "
                )
