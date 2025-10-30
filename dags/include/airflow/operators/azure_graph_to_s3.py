import json
import logging

import pendulum

from include.airflow.hooks.azure_hook import AzureHook
from include.airflow.operators.rows_to_s3 import BaseRowsToS3CsvOperator


class AzureGraphToS3(BaseRowsToS3CsvOperator):
    """
    :param azure_url: Graph URL from where we will fetch data
    :param s3_path: Temporary path to store results of API
    :param file_name: file_name to be stored
    :param prefix_list: List of prefixes to be filtered from groups, None for users
    :param group_request_url: Group filter URL from which we filter the prefix list, None for Users
    """

    template_fields = ["key"]

    def __init__(self, azure_url, prefix_list, group_request_url, **kwargs):
        super().__init__(**kwargs)
        self.request_url = azure_url
        self.prefix_list = prefix_list
        self.group_request_url = group_request_url

    def get_azure_conn(self):
        hook = AzureHook('azure_default')
        client = hook.azure_get_conn()
        return client

    def get_data_from_azure(self, request_url, group_param_tuple={}):
        updated_at = pendulum.DateTime.utcnow()
        client = self.get_azure_conn()
        while request_url:
            result = client.get(url=request_url)
            json_resp = json.loads(result.text)
            if 'Users' in request_url:
                for resp in json_resp['value']:
                    yield {**resp, "updated_at": updated_at}
            elif 'groups' in request_url:
                group_params = {
                    'group_name': group_param_tuple.get('displayName'),
                    'group_id': group_param_tuple.get('id'),
                }
                for resp in json_resp['value']:
                    resp.pop('@odata.type')
                    yield {**group_params, **resp, "updated_at": updated_at}
            request_url = (
                json_resp['@odata.nextLink'] if '@odata.nextLink' in json_resp.keys() else False
            )
            logging.info("Loaded 100 records & fetching next set of records...")

    def get_group_details_from_azure(self, prefix_name):
        client = self.get_azure_conn()
        group_request_url = self.group_request_url.format(groupName=prefix_name)
        result = client.get(url=group_request_url)
        json_resp = json.loads(result.text)
        return json_resp['value']

    def get_rows(self):
        if 'Users' in self.request_url:
            for record in self.get_data_from_azure(self.request_url):
                yield record
        elif 'groups' in self.request_url:
            for prefix in self.prefix_list:
                logging.info(
                    f"Looking for prefix - {prefix}",
                )
                group_details = self.get_group_details_from_azure(prefix)
                for group in group_details:
                    group_request_url = self.request_url.format(groupID=group['id'])
                    for record in self.get_data_from_azure(group_request_url, group):
                        yield record
