from airflow.hooks.base import BaseHook
from elasticsearch import Elasticsearch


class ESHook(BaseHook):
    def __init__(
        self, es_conn_id, host=None, protocol="https", port=9200, *args, **kwargs
    ):
        self.es_conn_id = es_conn_id
        self.creds = self.get_connection(self.es_conn_id)
        self.host = host or self.creds.host
        self.protocol = protocol
        self.port = port or self.creds.port
        self.base_url = f"{protocol}://{self.host}:{self.port}/"

    @property
    def conn(self):
        http_auth_creds = []
        if self.creds.login and self.creds.password:
            http_auth_creds = (self.creds.login, self.creds.password)
        connect_kwargs = dict(hosts=[self.base_url])
        if http_auth_creds:
            connect_kwargs.update(http_auth=http_auth_creds)
        return Elasticsearch(**connect_kwargs)

    def search_all(self, index, body, size=10000, **kwargs):
        while True:
            results = self.conn.search(index=index, body=body, size=size, **kwargs)
            if len(results["hits"]["hits"]) == 0:
                break
            for result in results["hits"]["hits"]:
                yield result
            body["search_after"] = results["hits"]["hits"][-1]["sort"]
