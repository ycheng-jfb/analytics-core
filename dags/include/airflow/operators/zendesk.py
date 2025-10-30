import time
from typing import Dict, Iterable, Iterator, Optional

import pendulum
from functools import cached_property

from include.airflow.hooks.zendesk import ZendeskHook
from include.airflow.operators.rows_to_s3 import (
    BaseRowsToS3CsvOperator,
    BaseRowsToS3JsonWatermarkOperator,
)
from include.airflow.utils.utils import flatten_json
from include.config import conn_ids


class ZendeskToS3Operator(BaseRowsToS3JsonWatermarkOperator):
    def __init__(
        self,
        endpoint: str,
        extra_params: Optional[dict],
        column_list: list,
        is_incremental: bool = False,
        use_cursor: bool = False,
        api_version="v2",
        zendesk_conn_id=conn_ids.Zendesk.default,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.is_incremental = is_incremental
        self.api_version = api_version
        self.endpoint = endpoint
        self._request_params = extra_params
        self.column_list = column_list
        self.zendesk_conn_id = zendesk_conn_id
        self.use_cursor = use_cursor
        self.endpoint_prefix = 'incremental/' if self.is_incremental else ''
        self.endpoint_suffix = '/cursor.json' if self.use_cursor else '.json'
        api_url = "https://techstyletap.zendesk.com"
        self.base_url = (
            f"{api_url}/api/{self.api_version}/"
            f"{self.endpoint_prefix}{self.endpoint}{self.endpoint_suffix}"
        )

    def get_high_watermark(self):
        return pendulum.DateTime.utcnow().isoformat()

    @staticmethod
    def raise_for_status(response):
        if response.status_code not in (200, 429):
            print(f"headers: {response.headers}")
            print(f"content: {response.content}")
            print(f"status_code: {response.status_code}")
            response.raise_for_status()
            raise Exception(response.content)

    @property
    def request_params(self):
        if self.is_incremental:
            low_watermark = pendulum.parse(self.low_watermark).int_timestamp
            self._request_params['start_time'] = low_watermark
        return self._request_params

    @cached_property
    def hook(self):
        return ZendeskHook(zendesk_conn_id=self.zendesk_conn_id)

    @cached_property
    def session(self):
        return self.hook.session

    def get_response(self, url, params):
        response = self.session.get(url, params=params)
        self.raise_for_status(response)
        if response.status_code == 429:
            retry_after = response.headers['Retry-After']
            print(
                f"No. of hits exceeded for the endpoint: {self.endpoint}. "
                f"Wait for {retry_after} seconds..."
            )
            time.sleep(int(retry_after) + 1)
            response = self.session.get(url)
        return response

    def should_break(self, data):
        """
        When doing incremental and not using the cursor endpoint, we have to manually force an exit
        once we have caught up to present.  Otherwise it will keep paging forever.

        Args:
            data: the json response (dict)

        Returns: True if we should exit the while loop

        """
        return (
            self.is_incremental
            and not self.use_cursor
            and int(data['end_time']) > pendulum.parse(self.new_high_watermark).int_timestamp
        )

    def get_rows(self) -> Iterator[dict]:
        url = self.base_url
        params = self.request_params
        while url:
            response = self.get_response(url, params)
            data = response.json()
            for row in data[self.endpoint]:
                yield {k: row[k] for k in self.column_list}
            params = {}
            url = data.get("next_page") or data.get("after_url")
            if self.should_break(data):
                break
            if url:
                print(f"Found next page: {url}")

    def execute(self, context=None):
        if self.is_incremental:
            self.watermark_pre_execute()
            self.watermark_execute(context=context)
            self.watermark_post_execute()
        else:
            self.watermark_execute(context=context)


class ZendeskS3CsvOperator(BaseRowsToS3CsvOperator):
    """
    Basic operator to git Zendesk API, completely flatten the json, and save to csv in S3
    """

    def __init__(
        self,
        endpoint,
        params=None,
        zendesk_conn_id=conn_ids.Zendesk.default,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.endpoint = endpoint
        self.params = params
        self.zendesk_conn_id = zendesk_conn_id

    @cached_property
    def hook(self):
        return ZendeskHook(zendesk_conn_id=self.zendesk_conn_id)

    def get_rows(self) -> Iterable[Dict]:
        tfg_updated_at = {"tfg_updated_at": pendulum.DateTime.utcnow()}
        metrics = self.hook.get_response_all_pages(self.endpoint, self.params)
        for metric in metrics:
            yield {**flatten_json(metric), **tfg_updated_at}
