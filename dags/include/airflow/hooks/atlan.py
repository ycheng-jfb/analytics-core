from include.airflow.hooks.base_web_api_hook import BaseWebApiHook


class AtlanHook(BaseWebApiHook):
    def __init__(
        self,
        conn_id='atlan_api_token',
        host='https://data-techstyle.atlan.com',
        version='v1',
    ):
        super().__init__(
            conn_id=conn_id,
            host=host,
            version=version,
        )

    def get_base_url(self):
        return f'{self.host}'

    def get_base_headers(self):
        return {'Authorization': f'Bearer {self.extras.get("api_token")}'}
