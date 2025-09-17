from dataclasses import dataclass
from pathlib import Path

from include import TASK_CONFIGS_DIR


@dataclass
class Config:
    subject: str
    distribution_list: list[str]

    @property
    def sql_list(self):
        return [
            Path(TASK_CONFIGS_DIR, 'sql', 'tableau.audit_terminated_users.sql'),
            Path(TASK_CONFIGS_DIR, 'sql', 'tableau.audit_missing_users.sql'),
        ]

    @property
    def body_list(self):
        return [
            (
                'The following users have been terminated in Workday or Azure but still exist'
                ' in Tableau. Please remove them ASAP.'
            ),
            (
                'The following users are setup in Tableau with non-corporate domains and a'
                ' corresponding account in Workday or Azure could not be found.'
            ),
        ]


config = Config(
    subject='ALERT: Terminated / Non - Corp Accounts in Tableau',
    distribution_list=[
        'mhernick@techstyle.com',
    ],
)
