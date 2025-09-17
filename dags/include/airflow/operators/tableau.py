import time

import pandas as pd
import pendulum
import requests
from airflow.models import BaseOperator
from functools import cached_property

from include.airflow.hooks.tableau import TableauHook
from include.airflow.operators.rows_to_s3 import BaseRowsToS3CsvOperator
from include.airflow.utils.utils import flatten_json
from include.utils.exponential_wait import Waiter
from include.config import conn_ids


class TableauRefreshOperator(BaseOperator):
    def __init__(
        self,
        tableau_conn_id=conn_ids.Tableau.default,
        wait_till_complete: bool = False,
        data_source_name: str = None,
        data_source_id: str = None,
        **kwargs,
    ):
        self._data_source_id = data_source_id
        self.data_source_name = data_source_name
        self.wait_till_complete = wait_till_complete
        self.tableau_conn_id = tableau_conn_id
        self.validate_args()
        super().__init__(**kwargs)

    def validate_args(self):
        if self._data_source_id is not None and self.data_source_name is not None:
            raise ValueError("only one of data_source_id and data_source_id may be given")
        if self._data_source_id is None and self.data_source_name is None:
            raise ValueError("one of data_source_id and data_source_id must be given")

    @cached_property
    def hook(self):
        hook = TableauHook(tableau_conn_id=self.tableau_conn_id)
        return hook

    @cached_property
    def data_source_id(self):
        if self._data_source_id:
            return self._data_source_id
        else:
            return self.hook.get_data_source_id(data_source_name=self.data_source_name)

    def execute(self, context=None):
        self.log.info(f"requesting data source refresh for id {self.data_source_id}")
        response = self.hook.refresh_data_source(data_source_id=self.data_source_id)
        self.log.info(response)
        if self.wait_till_complete:
            job_id = response['job']['id']
            waiter = Waiter(
                growth_param=1.2,
                initial_wait=10,
                max_wait_time_seconds=2 * 60 * 60,
            )
            while True:
                self.log.info('pulling job status')
                status = self.hook.get_job_status(job_id=job_id)
                if 'finishCode' in status.keys():
                    if status['finishCode'] == '0':
                        self.log.info(f'Extract completed {status}')
                        break
                    else:
                        raise Exception(f"job {job_id} failed with response {status}")
                else:
                    wait_seconds = waiter.next()
                    self.log.info(status)
                    self.log.info(f"waiting {wait_seconds} seconds before next try")
                    time.sleep(wait_seconds)


class BaseTableauToS3Operator(BaseRowsToS3CsvOperator):
    def __init__(
        self,
        tableau_conn_id=conn_ids.Tableau.default,
        tableau_api_version="3.24",
        **kwargs,
    ):
        self.tableau_conn_id = tableau_conn_id
        self.tableau_api_version = tableau_api_version
        super().__init__(**kwargs)

    @cached_property
    def tableau_hook(self):
        hook = TableauHook(tableau_conn_id=self.tableau_conn_id, version=self.tableau_api_version)
        return hook


class TableauUsersByGroupsToS3Operator(BaseTableauToS3Operator):
    def get_rows(self):
        self.log.info("retrieving all groups")
        updated_at_dict = {"updated_at": pendulum.DateTime.utcnow()}

        groups = self.tableau_hook.get_groups()
        for group in groups:
            group_id = group.get("id")
            prefixed_group = {"grp_" + key: val for key, val in group.items()}

            self.log.info(f"retrieving users for group id {group_id}")
            users_in_group = self.tableau_hook.get_users_in_group(group.get("id"))
            for user in users_in_group:
                prefixed_user = {"usr_" + key: val for key, val in user.items()}
                yield {**prefixed_group, **prefixed_user, **updated_at_dict}


class TableauWorkbooksToS3Operator(BaseTableauToS3Operator):
    def get_rows(self):
        updated_at_dict = {"updated_at": pendulum.DateTime.utcnow()}
        workbooks = self.tableau_hook.api_get_call('workbooks', 'workbooks')
        for workbook in workbooks:
            df = pd.json_normalize(workbook, sep='_')
            yield {**df.to_dict(orient='records')[0], **updated_at_dict}


class TableauWorkbookConnectionsToS3Operator(BaseTableauToS3Operator):
    def get_rows(self):
        updated_at_dict = {"updated_at": pendulum.DateTime.utcnow()}
        workbooks = self.tableau_hook.api_get_call('workbooks', 'workbooks')
        for workbook in workbooks:
            workbook_id_dict = {'workbook_id': workbook.get('id')}
            connections = self.tableau_hook.api_get_call(
                f'workbooks/{workbook.get("id")}/connections', 'connections'
            )
            for connection in connections:
                df = pd.json_normalize(connection, sep='_')
                yield {**df.to_dict(orient='records')[0], **updated_at_dict, **workbook_id_dict}


class TableauDataSourcesToS3Operator(BaseTableauToS3Operator):
    def get_rows(self):
        updated_at_dict = {"updated_at": pendulum.DateTime.utcnow()}
        datasources = self.tableau_hook.api_get_call('datasources', 'datasources')
        for datasource in datasources:
            df = pd.json_normalize(datasource, sep='_')
            yield {**df.to_dict(orient='records')[0], **updated_at_dict}


class TableauDeleteUsers(BaseOperator):
    def __init__(
        self,
        remove_users: list,
        tableau_conn_id=conn_ids.Tableau.default,
        tableau_api_version="3.12",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.remove_users = remove_users
        self.tableau_conn_id = tableau_conn_id
        self.tableau_api_version = tableau_api_version

    @cached_property
    def tableau_hook(self):
        return (
            TableauHook(tableau_conn_id=self.tableau_conn_id, version=self.tableau_api_version)
            if self.tableau_conn_id
            else TableauHook(version=self.tableau_api_version)
        )

    def execute(self, context=None):
        all_users_gen = self.tableau_hook.api_get_call(api_path='users', api_object_name='users')
        all_users_list = list(all_users_gen)
        for remove_user in self.remove_users:
            user_found = False
            remove_user_up = remove_user.upper()
            for user in all_users_list:
                if user['name'].upper() == remove_user_up:
                    user_found = True
                    print(f'Deleting user: {remove_user} | id: {user["id"]}')
                    try:
                        r = self.tableau_hook.make_request(
                            method='DELETE', endpoint=f'users/{user["id"]}'
                        )
                        print(f'User is deleted {r}')
                    except requests.exceptions.HTTPError as e:
                        print('Delete failed')
                        print(e)

            if not user_found:
                print(f'User {remove_user} not found')


class TableauPermissions(BaseTableauToS3Operator):
    def get_rows(self):
        updated_at_dict = {"updated_at": pendulum.DateTime.utcnow()}
        projects = self.tableau_hook.api_get_call('projects', 'projects')
        for project in projects:
            permissions = self.tableau_hook.make_request(
                'GET', f'projects/{project["id"]}/permissions'
            ).json()
            for capabilities in permissions['permissions']['granteeCapabilities']:
                for capability in capabilities['capabilities']['capability']:
                    if capabilities.get('group'):
                        yield {
                            'project_id': project['id'],
                            'grantee_type': 'group',
                            'grantee_id': capabilities['group']['id'],
                            'capability_name': capability['name'],
                            'capability_mode': capability['mode'],
                            **updated_at_dict,
                        }
                    elif capabilities.get('user'):
                        yield {
                            'project_id': project['id'],
                            'grantee_type': 'user',
                            'grantee_id': capabilities['user']['id'],
                            'capability_name': capability['name'],
                            'capability_mode': capability['mode'],
                            **updated_at_dict,
                        }
                    else:
                        raise Exception('Unknown permissions entity type.')
