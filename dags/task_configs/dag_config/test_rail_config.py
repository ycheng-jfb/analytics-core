from dataclasses import dataclass
from typing import List, Union

from include.utils.snowflake import Column


@dataclass
class CasesConfig:
    project_id: str
    suite_ids: Union[List, None] = None


cases_config = [
    CasesConfig(project_id='217'),
    CasesConfig(project_id='25', suite_ids=['8785']),
]


@dataclass
class Config:
    database: str
    schema: str
    table: str
    column_list: List
    version: int

    @property
    def s3_prefix(self):
        return f'lake/{self.database}.{self.schema}.{self.table}/{self.version}'


test_rail_configs = [
    Config(
        database='lake',
        schema='test_rail',
        table='cases',
        column_list=[
            Column('id', 'NUMBER', source_name='id', uniqueness=True),
            Column('suite_id', 'NUMBER', source_name='suite_id', uniqueness=True),
            Column('project_id', 'NUMBER', source_name='project_id', uniqueness=True),
            Column('title', 'VARCHAR', source_name='title'),
            Column('section_id', 'NUMBER', source_name='section_id'),
            Column('template_id', 'NUMBER', source_name='template_id'),
            Column('type_id', 'NUMBER', source_name='type_id'),
            Column('priority_id', 'NUMBER', source_name='priority_id'),
            Column('milestone_id', 'NUMBER', source_name='milestone_id'),
            Column('refs', 'VARCHAR', source_name='refs'),
            Column('created_by', 'NUMBER', source_name='created_by'),
            Column('created_on', 'TIMESTAMP_LTZ(3)', source_name='created_on'),
            Column('updated_by', 'NUMBER', source_name='updated_by'),
            Column('updated_on', 'TIMESTAMP_LTZ(3)', source_name='updated_on'),
            Column('estimate', 'VARCHAR', source_name='estimate'),
            Column('estimate_forecast', 'VARCHAR', source_name='estimate_forecast'),
            Column('custom_automated', 'BOOLEAN', source_name='custom_automated'),
            Column('custom_archived', 'BOOLEAN', source_name='custom_archived'),
            Column('custom_reviewed', 'BOOLEAN', source_name='custom_reviewed'),
            Column('custom_version', 'NUMBER', source_name='custom_version'),
            Column('custom_automation_status', 'NUMBER', source_name='custom_automation_status'),
            Column('custom_preconds', 'VARCHAR', source_name='custom_preconds'),
            Column('custom_steps', 'VARCHAR', source_name='custom_steps'),
            Column('custom_expected', 'VARCHAR', source_name='custom_expected'),
            Column('custom_flow', 'VARCHAR', source_name='custom_flow'),
            Column('custom_sxfusertype', 'VARIANT', source_name='custom_sxfusertype'),
            Column('updated_at', 'TIMESTAMP_LTZ(3)', source_name='updated_at', delta_column=True),
        ],
        version=1,
    ),
]
