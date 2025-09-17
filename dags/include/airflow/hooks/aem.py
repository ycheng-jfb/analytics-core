from include.airflow.hooks.base_web_api_hook import BaseWebApiHook
from include.config import conn_ids


class AemHook(BaseWebApiHook):
    def __init__(
        self,
        conn_id=conn_ids.AEM.metadata_api,
        host='http://tfgelsvmlxaem01.corp.brandideas.com:4502',
        path='bin/techstyle/',
    ):
        self.path = path
        super().__init__(
            conn_id=conn_id,
            host=host,
        )

    def get_base_url(self):
        return f'{self.host}/{self.path}'

    def get_base_headers(self):
        return {
            'userName': self.conn.login,
            'password': self.conn.password,
        }
