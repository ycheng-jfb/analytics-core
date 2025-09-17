import pendulum
from functools import cached_property

from include.airflow.hooks.test_rail import TestRailHook
from include.airflow.operators.rows_to_s3 import BaseRowsToS3CsvOperator


class TestRailBase(BaseRowsToS3CsvOperator):
    def __init__(
        self,
        cases_config,
        **kwargs,
    ):
        self.cases_config = cases_config
        super().__init__(**kwargs)

    @cached_property
    def test_rail_hook(self):
        return TestRailHook(conn_id=self.hook_conn_id) if self.hook_conn_id else TestRailHook()


class TestRailCases(TestRailBase):
    def get_suite_ids(self, project_id):
        suites = self.test_rail_hook.make_get_request_all(f'get_suites/{project_id}')
        return [x.get('id') for x in suites]

    def get_rows(self):
        updated_at = pendulum.DateTime.utcnow()
        for cfg in self.cases_config:
            if not (suite_ids := cfg.suite_ids):
                suite_ids = self.get_suite_ids(cfg.project_id)

            for suite_id in suite_ids:
                cases = self.test_rail_hook.make_get_request_all(
                    f'get_cases/{cfg.project_id}&suite_id={suite_id}', list_name='cases'
                )

                for case in cases:
                    yield {'project_id': cfg.project_id, **case, 'updated_at': updated_at}
