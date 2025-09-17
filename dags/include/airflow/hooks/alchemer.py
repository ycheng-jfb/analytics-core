import time

from functools import cached_property

from include.airflow.hooks.base_web_api_hook import BaseWebApiHook
from include.config import conn_ids


class AlchemerHook(BaseWebApiHook):
    def __init__(
        self,
        conn_id="alchemer_default",
        host="https://api.alchemer.com",
        version="v5",
    ):
        super().__init__(
            conn_id=conn_id,
            host=host,
            version=version,
        )

    @cached_property
    def auth_params(self):
        return {
            "api_token": self.extras.get("api_token"),
            "api_token_secret": self.extras.get("api_token_secret"),
        }

    def get_base_url(self):
        return f"{self.host}/{self.version}"

    def get_base_headers(self):
        return {}

    def make_request(
        self, method, endpoint, headers=None, params=None, json=None, data=None
    ):
        params = params if params else {}
        count = 0
        while True:
            count += 1
            try:
                r = super().make_request(
                    method,
                    endpoint,
                    headers=None,
                    params={**params, **self.auth_params},
                    json=None,
                    data=None,
                )
                break
            except Exception as err:
                time.sleep(5)
                if count < 3:
                    continue
                else:
                    assert err

        return r

    def make_get_request_all(
        self,
        endpoint,
        list_name="data",
        headers=None,
        params=None,
        json=None,
        data=None,
    ):
        params = params if params else {}
        while True:
            r = self.make_request(
                "get", endpoint, headers=headers, params=params, json=json, data=data
            )
            data = r.json()
            if list_name:
                for obj in data[list_name]:
                    yield obj
            else:
                for obj in data:
                    yield obj
            if (
                data.get("page")
                and data.get("total_pages")
                and data["page"] < data["total_pages"]
            ):
                params = {
                    **params,
                    "page": data["page"] + 1,
                }
            else:
                break
