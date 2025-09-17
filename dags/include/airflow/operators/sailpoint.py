import pendulum
import requests
from functools import cached_property

from include.airflow.hooks.sailpoint import SailpointHook
from include.airflow.operators.rows_to_s3 import BaseRowsToS3CsvOperator
from include.airflow.utils.utils import flatten_json


class SailpointGetAccounts(BaseRowsToS3CsvOperator):
    def __init__(self, source_ids, url_endpoint, **kwargs):
        self.source_ids = source_ids
        self.url_endpoint = url_endpoint
        super().__init__(**kwargs)

    @cached_property
    def sailpoint_hook(self):
        return SailpointHook(conn_id=self.hook_conn_id)

    def get_rows(self):
        updated_at = {"updated_at": pendulum.DateTime.utcnow()}
        params = {
            "limit": 50,
            "offset": 0,
            "count": "false",
            "filters": f"sourceId in ({self.source_ids})"
            if self.source_ids != ""
            else "",
        }

        accounts = self.sailpoint_hook.make_get_request_all(
            f"/{self.url_endpoint}", params=params
        )
        for account in accounts:
            yield {**flatten_json(account, flatten_list=False), **updated_at}


class SailpointGetAccessProfilesByIdentity(BaseRowsToS3CsvOperator):
    def __init__(self, source_ids, url_endpoint, **kwargs):
        self.source_ids = source_ids
        self.url_endpoint = url_endpoint
        super().__init__(**kwargs)

    @cached_property
    def sailpoint_hook(self):
        return SailpointHook(conn_id=self.hook_conn_id)

    def get_rows(self):
        updated_at = {"updated_at": pendulum.DateTime.utcnow()}
        params = {
            "limit": 50,
            "offset": 0,
            "count": "false",
            "filters": f"sourceId in ({self.source_ids})",
        }

        accounts = self.sailpoint_hook.make_get_request_all(
            f"/{self.url_endpoint}", params=params
        )
        identity_ids = set()
        for account in accounts:
            identity_ids.add(account.get("identityId"))

        for identity_id in identity_ids:
            try:
                access_profiles = self.sailpoint_hook.make_request(
                    "GET",
                    f"/historical-identities/{identity_id}/access-items",
                    params={"type": "accessProfile"},
                ).json()
            except requests.exceptions.HTTPError as err:
                if (
                    err.response.status_code == 404
                    or err.response.status_code == 500
                    or err.response.status_code == 502
                ):
                    continue
                else:
                    raise
            for profile in access_profiles:
                yield {
                    "identity_id": identity_id,
                    **profile,
                    **updated_at,
                }
