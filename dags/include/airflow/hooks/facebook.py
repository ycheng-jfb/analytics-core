import csv
import gzip
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List
from urllib.parse import urlencode

import pendulum
import requests
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from include.config import conn_ids
from facebook_business import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.user import User
from facebook_business.exceptions import FacebookError, FacebookRequestError

from include.utils.exponential_wait import Waiter, exponential_backoff_wait_times


@dataclass
class AsyncStatus:
    status: str
    percent_complete: str

    @property
    def is_complete(self):
        return self.status in {"Job Completed"}

    @property
    def is_processing(self):
        return self.status in {"Job Not Started", "Job Started", "Job Running"}

    @property
    def is_failed(self):
        return self.status in {"Job Failed", "Job Skipped"}


class FacebookAdsHook(BaseHook):
    def __init__(
        self,
        facebook_ads_conn_id=conn_ids.Facebook.ads_default,
        s3_conn_id=conn_ids.AWS.tfg_default,
        rows_per_req=500,
    ):
        self.facebook_ads_conn_id = facebook_ads_conn_id
        self.s3_conn_id = s3_conn_id
        self.api_version = self.get_connection(
            self.facebook_ads_conn_id
        ).extra_dejson.get("api_version")
        self.base_url = f"https://graph.facebook.com/{self.api_version}"
        self.rows_exported = 0
        self.rows_per_req = rows_per_req
        self._connection = None
        self._api = None
        self._session = None
        self._access_token = None

    @property
    def connection(self):
        if self._connection is None:
            self._connection = self.get_connection(self.facebook_ads_conn_id)
        return self._connection

    @property
    def access_token(self):
        if self._access_token is None:
            self._access_token = (
                self.connection.extra_dejson.get("access_token")
                or self.connection.password
            )
        return self._access_token

    @property
    def headers(self):
        return {"Authorization": f"Bearer {self.access_token}"}

    @property
    def session(self):
        if self._session is None:
            session = requests.Session()
            session.headers = self.headers
            self._session = session
        return self._session

    @property
    def business_id(self):
        business_id = self.connection.extra_dejson.get("business_id")
        return business_id

    @property
    def api(self) -> FacebookAdsApi:
        if self._api:
            return self._api
        else:
            self._api = FacebookAdsApi.init(
                access_token=self.access_token, api_version=self.api_version
            )
        return self._api

    def get_account_list(self) -> List[AdAccount]:
        me = User(fbid="me", api=self.api)
        response = me.get_ad_accounts(
            [AdAccount.Field.id, AdAccount.Field.name, AdAccount.Field.business]
        )
        account_list = list(response)
        return account_list

    def is_rate_limit_error(self, e: FacebookError):
        if isinstance(e, FacebookRequestError):
            message = e.get_message()
            print(f"error message: {message}")
            rate_limiting_message = (
                "(#80000) There have been too many calls from this ad-account. Wait a bit"
                " and try again. For more info, please refer to "
                "https://developers.facebook.com/docs/graph-api/overview/rate-limiting."
            )
            if message == rate_limiting_message:
                print(e.http_headers())
                return True
            else:
                return False

    def __make_request(self, method, url, max_tries=3):
        try_count = 0
        waiter = exponential_backoff_wait_times()
        while True:
            try_count += 1
            try:
                response = self.session.request(method=method, url=url)
                if response.status_code != 200:
                    raise FacebookError(response.json())
                else:
                    return response
            except FacebookError as e:
                if self.is_rate_limit_error(e):
                    raise e

                if try_count == max_tries:
                    print(f"request failed {try_count} times; throwing FacebookError.")
                    raise e
                else:
                    wait_seconds = next(waiter)
                    print(
                        f"got exception {e}, waiting {wait_seconds} second before retry"
                    )
                    time.sleep(wait_seconds)

    def get_async_report_status(self, report_run_id):
        print(f"checking async status for report {report_run_id}")
        req_str = f"{self.base_url}/{report_run_id}"
        response = self.__make_request("GET", req_str)
        rdict = response.json()
        s = AsyncStatus(
            status=rdict["async_status"],
            percent_complete=rdict.get("async_percent_completion"),
        )
        print(
            f"status for report {report_run_id} is '{s.status}'; {s.percent_complete} percent complete"
        )
        return s

    def await_async_report(
        self, report_run_id, initial_wait=2, max_wait_time_seconds=60 * 60
    ):
        max_minutes_before_progress = 10
        print(f"waiting for job {report_run_id}")
        s = self.get_async_report_status(report_run_id=report_run_id)
        waiter = Waiter(
            growth_param=1.2,
            initial_wait=initial_wait,
            max_wait_time_seconds=max_wait_time_seconds,
        )
        curr_percent = 0
        loops_since_change = 0
        while s.is_processing:
            wait_seconds = waiter.next()
            print(
                f"job {report_run_id} has status '{s.status}'; waiting {wait_seconds} seconds"
            )
            time.sleep(wait_seconds)
            s = self.get_async_report_status(report_run_id=report_run_id)
            if (
                s.percent_complete == 0
                and waiter.elapsed_time > 60 * max_minutes_before_progress
            ):
                raise TimeoutError(
                    f"no progress made on report completion after {max_minutes_before_progress} minutes"
                )
            if curr_percent == int(s.percent_complete) and wait_seconds >= 60:
                loops_since_change += 1
            else:
                loops_since_change = 0
            curr_percent = int(s.percent_complete)

            if loops_since_change >= 5:
                raise TimeoutError(
                    f"Processing stuck at {s.percent_complete} percent complete"
                )

        if s.is_complete:
            return True
        else:
            raise ValueError(
                f"Unsuccesful job status returned ('{s.status}'). Failing job."
            )

    def create_async_report(self, endpoint: str, payload: dict):
        req_str = f"{self.base_url}/{endpoint}?{urlencode(payload)}"
        response = self.__make_request("POST", req_str)
        report_run_id = response.json()["report_run_id"]
        print(f"created async report with id {report_run_id}")
        return report_run_id

    def get_rows(self, endpoint: str, payload: dict = None, max_retries_per_req=3):
        if not payload:
            payload = {"limit": self.rows_per_req}
        else:
            payload.update(limit=self.rows_per_req)
        req_str = f"{self.base_url}/{endpoint}?{urlencode(payload)}"
        response = self.__make_request("GET", req_str, max_tries=max_retries_per_req)
        print(f"fetched {req_str} in {response.elapsed} seconds")
        print(
            f"x-business-use-case-usage: {response.headers.get('x-business-use-case-usage')}"
        )
        response_dict = response.json()
        if "data" in response_dict:
            for row in response_dict["data"]:
                self.rows_exported += 1
                yield row
        while "next" in response_dict.get("paging", {}):
            time.sleep(0.5)
            req_str = response_dict["paging"]["next"]
            response = self.__make_request(
                "GET", req_str, max_tries=max_retries_per_req
            )
            print(f"fetched {req_str} in {response.elapsed} seconds")
            print(
                f"x-business-use-case-usage: {response.headers.get('x-business-use-case-usage')}"
            )
            response_dict = response.json()
            if "data" in response_dict:
                for row in response_dict["data"]:
                    self.rows_exported += 1
                    yield row

    def get_async_report_rows(self, report_run_id, max_retries_per_req=3):
        endpoint = f"{report_run_id}/insights"
        rows = self.get_rows(
            endpoint=endpoint, payload=None, max_retries_per_req=max_retries_per_req
        )
        return rows

    @staticmethod
    def get_rows_to_file(
        rows, filename, column_list, compression="gzip", add_timestamp=False
    ):
        dt = pendulum.DateTime.utcnow().isoformat()
        timestamp_col = {"api_call_timestamp": dt}
        open_func = gzip.open if compression == "gzip" else open
        with open_func(filename, "at") as f:
            writer = csv.DictWriter(
                f=f,
                fieldnames=column_list,
                extrasaction="ignore",
                delimiter="\t",
                dialect="unix",
                quoting=csv.QUOTE_MINIMAL,
            )
            if add_timestamp:
                for row in rows:
                    writer.writerow({**row, **timestamp_col})
            else:
                for row in rows:
                    writer.writerow(row)

    def get_rows_to_s3(
        self, rows, column_list, bucket, key, s3_replace=True, add_timestamp=False
    ):
        with tempfile.TemporaryDirectory() as td:
            filename = Path(td, "report")
            self.get_rows_to_file(
                rows=rows,
                filename=filename,
                column_list=column_list,
                add_timestamp=add_timestamp,
            )
            if self.rows_exported > 0:
                s3_hook = S3Hook(self.s3_conn_id)
                print(f"uploading to {key}")
                s3_hook.load_file(
                    filename=filename.as_posix(),
                    key=key,
                    bucket_name=bucket,
                    replace=s3_replace,
                )
