from include.airflow.hooks.base_web_api_hook import BaseWebApiHook


class Pivot88Hook(BaseWebApiHook):
    def __init__(
        self,
        conn_id="pivot88_default",
        host='https://techstyle.pivot88.com/rest/operation',
        version="v1",
    ):
        super().__init__(
            conn_id=conn_id,
            host=host,
            version=version,
        )

    def get_base_url(self):
        return f'{self.host}/{self.version}'

    def get_base_headers(self):
        return {
            'api-key': self.extras.get('api_key'),
        }
