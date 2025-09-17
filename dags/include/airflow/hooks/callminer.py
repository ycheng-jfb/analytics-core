import requests

from include.airflow.hooks.base_web_api_hook import BaseWebApiHook
from include.config import conn_ids


class CallMinerHook(BaseWebApiHook):
    def __init__(
        self,
        conn_id=conn_ids.Callminer.default,
        host='https://feapi.callminer.net/api',
        version='v2',
    ):
        super().__init__(conn_id=conn_id, host=host, version=version)

    def get_base_url(self):
        return f'{self.host}/{self.version}'

    def get_base_headers(self):
        data = {
            'ApiKey': self.extras.get('ApiKey'),
            'Username': self.conn.login,
            'Password': self.conn.password,
        }
        r = requests.post('https://sapi.callminer.net/security/getToken', json=data)
        r.raise_for_status()
        token = r.json()
        return {'Authorization': f'JWT {token}'}

    def make_request(self, method, endpoint, headers=None, params=None, json=None, data=None):
        r = super().make_request(
            method=method, endpoint=endpoint, headers=headers, params=params, json=json, data=data
        )
        new_token = r.headers.get('auth-token-updated')
        self.session.headers.update({'Authorization': f'JWT {new_token}'})

        return r

    def make_get_request_all(
        self, endpoint, records=50, headers=None, params=None, json=None, data=None
    ):
        if not params:
            params = {}
        params |= {
            'page': 1,
            'records': records,
        }

        while True:
            r = self.make_request(
                'GET', endpoint, headers=headers, params=params, json=json, data=data
            )

            resp_json = r.json()
            if len(resp_json) == 0:
                break
            for obj in resp_json:
                yield obj

            if last_record_info := resp_json[-1].get('RecordInfo'):
                if last_record_info.get('RowNumber') < last_record_info.get('TotalRowCount'):
                    params['page'] += 1
                    continue
                else:
                    break
            else:
                raise Exception('No RecordInfo info object to manage pagination.')
