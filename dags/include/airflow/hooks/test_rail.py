import base64

from include.airflow.hooks.base_web_api_hook import BaseWebApiHook
from include.config import conn_ids


class TestRailHook(BaseWebApiHook):
    def __init__(
        self,
        conn_id=conn_ids.TestRail.default,
        host="https://testrail.techstyle.net",
        base_path="testrail/index.php?/api",
        version="v2",
    ):
        self.base_path = base_path
        super().__init__(conn_id=conn_id, host=host, version=version)

    def get_base_headers(self):
        bytes = f"{self.login}:{self.password}".encode("ascii")
        auth_str = base64.b64encode(bytes).decode("utf-8")
        return {
            "Authorization": f"Basic {auth_str}",
            "Content-Type": "application/json",
        }

    def get_base_url(self):
        return f"{self.host}/{self.base_path}/{self.version}"

    def make_get_request_all(
        self, endpoint, list_name=None, headers=None, params=None, json=None, data=None
    ):
        next_endpoint = endpoint
        while True:
            r = self.make_request(
                "get",
                next_endpoint,
                headers=headers,
                params=params,
                json=json,
                data=data,
            )
            data = r.json()
            if list_name:
                for obj in data[list_name]:
                    yield obj
            else:
                for obj in data:
                    yield obj

            if isinstance(data, list):
                break
            elif data.get("size") <= data.get("limit") and data.get("offset") == 0:
                break
            else:
                next_endpoint = data.get("_links").get("next")
                if next_endpoint is None:
                    break
