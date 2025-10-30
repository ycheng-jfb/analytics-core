from dataclasses import dataclass
from typing import Optional

from include.utils.snowflake import Column


@dataclass
class SnowFlakeConfig:
    name: str
    column_list: list
    prefix_list: Optional[list] = None
    request_url: Optional[str] = None
    db = 'lake'
    schema = 'azure'
    api_prefix = "https://graph.microsoft.com/v1.0"

    @property
    def get_table(self):
        return f"{self.name}_details"

    @property
    def get_file_name(self):
        return f"{self.get_table}_{{{{ ts_nodash }}}}.csv.gz"

    @property
    def get_S3_path(self):
        return (
            f"lake/{self.db}.{self.schema}.{self.name}_details/v2/"
            "{{macros.datetime.now().strftime('%Y%m')}}/"
        )

    @property
    def get_url_column_list(self):
        if self.name == 'user':
            return_string = '/Users?$select=' + ",".join(
                column.source_name for column in self.column_list
            )
        else:
            return_string = "/groups/{groupID}/members?$select=id,displayName,mail"
        return return_string


config_list = [
    SnowFlakeConfig(
        name='user',
        column_list=[
            Column('display_name', 'VARCHAR', source_name='displayName'),
            Column('given_name', 'VARCHAR', source_name='givenName'),
            Column('surname', 'VARCHAR', source_name='surname'),
            Column('job_title', 'VARCHAR', source_name='jobTitle'),
            Column('mail', 'VARCHAR', source_name='mail'),
            Column('user_principal_name', 'VARCHAR', source_name='userPrincipalName'),
            Column('id', 'VARCHAR', source_name='id', uniqueness=True),
            Column('mobile_phone', 'VARCHAR', source_name='mobilePhone'),
            Column('office_location', 'VARCHAR', source_name='officeLocation'),
            Column('preferred_language', 'VARCHAR', source_name='preferredLanguage'),
            Column('user_type', 'VARCHAR', source_name='userType'),
            Column('department', 'VARCHAR', source_name='department'),
            Column('account_enabled', 'BOOLEAN', source_name='accountEnabled'),
            Column('usage_location', 'VARCHAR', source_name='usageLocation'),
            Column('street_address', 'VARCHAR', source_name='streetAddress'),
            Column('city', 'VARCHAR', source_name='city'),
            Column('state', 'VARCHAR', source_name='state'),
            Column('country', 'VARCHAR', source_name='country'),
            Column('postal_code', 'VARCHAR', source_name='postalCode'),
            Column('age_group', 'VARCHAR', source_name='ageGroup'),
            Column('consent_provided_for_minor', 'VARCHAR', source_name='consentProvidedForMinor'),
            Column(
                'legal_age_group_classification',
                'VARCHAR',
                source_name='legalAgeGroupClassification',
            ),
            Column('company_name', 'VARCHAR', source_name='companyName'),
            Column('creation_type', 'VARCHAR', source_name='creationType'),
            Column('created_datetime', 'TIMESTAMP_LTZ(3)', source_name='createdDateTime'),
            Column('updated_at', 'TIMESTAMP_LTZ(3)', source_name='updated_at', delta_column=True),
        ],
    ),
    SnowFlakeConfig(
        name='group',
        column_list=[
            Column('group_name', 'VARCHAR', source_name='group_name'),
            Column('group_id', 'VARCHAR', source_name='group_id', uniqueness=True),
            Column('user_id', 'VARCHAR', source_name='id', uniqueness=True),
            Column('user_name', 'VARCHAR', source_name='displayName'),
            Column('user_mail', 'VARCHAR', source_name='mail'),
            Column('updated_at', 'TIMESTAMP_LTZ(3)', source_name='updated_at', delta_column=True),
        ],
        prefix_list=['snowflake', 'JFB/tableau'],
        request_url=(
            "https://graph.microsoft.com/v1.0/groups?$filter=startswith(displayName,"
            "'{groupName}')&$select=id,displayName"
        ),
    ),
]
