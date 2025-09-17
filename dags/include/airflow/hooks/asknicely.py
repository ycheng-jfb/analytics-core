import time

import requests
from airflow.hooks.base import BaseHook


class AsknicelyHook(BaseHook):
    def __init__(
        self,
        asknicely_conn_id,
    ):
        self.asknicely_conn_id = asknicely_conn_id
        self._conn = None

    @property
    def conn(self):
        if not self._conn:
            self._conn = BaseHook.get_connection(conn_id=self.asknicely_conn_id)
        return self._conn

    @property
    def api_key(self):
        return self.conn.extra_dejson.get("api_key")

    @property
    def domain_key(self):
        return self.conn.extra_dejson.get("domain_key")

    def get_request_uri(self, process_start_date, process_end_date):
        start_date_unix = time.mktime(process_start_date.timetuple())
        end_date_unix = time.mktime(process_end_date.timetuple())
        host = (
            "https://"
            + self.domain_key
            + ".asknice.ly/api/v1/responses/asc/50000/1/"
            + str(start_date_unix)
            + "/json/answered/sent/"
            + str(end_date_unix)
        )

        request_uri = host + "?X-apikey=" + self.api_key

        return request_uri

    def get_conn(self):
        sc = requests.get(self.get_request_uri())
        return sc
