from include.airflow.hooks.base_web_api_hook import BaseWebApiHook
from include.config import conn_ids


class SailpointHook(BaseWebApiHook):
    def __init__(
        self,
        conn_id=conn_ids.Sailpoint.default,
        host="https://sailpoint-gateway-api.azure-api.net",
        base_path="production",
    ):
        self.base_path = base_path
        super().__init__(
            conn_id=conn_id,
            host=host,
        )

    def get_base_headers(self):
        return {}

    def get_base_url(self):
        return f"{self.host}/{self.base_path}"

    def make_request(
        self, method, endpoint, headers=None, params=None, json=None, data=None
    ):
        if params:
            params = {**params, "subscription-key": self.extras.get("subscription_key")}
        else:
            params = {"subscription-key": self.extras.get("subscription_key")}

        return super().make_request(
            method, endpoint, headers=headers, params=params, json=json, data=data
        )

    def make_get_request_all(
        self, endpoint, headers=None, params=None, json=None, data=None
    ):
        while True:
            r = self.make_request(
                "get", endpoint, headers=headers, params=params, json=json, data=data
            ).json()
            if len(r) < 1:
                break

            for obj in r:
                yield obj
            params["offset"] = params["offset"] + params["limit"]
