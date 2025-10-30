from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='membership_invitation',
    company_join_sql="""
SELECT DISTINCT
         L.membership_invitation_id,
         DS.company_id
     FROM {database}.{source_schema}.membership_invitation AS L
     JOIN {database}.{source_schema}.membership_invitation_link AS mil
     ON L.MEMBERSHIP_INVITATION_LINK_ID = MIL.MEMBERSHIP_INVITATION_LINK_ID
     JOIN {database}.{source_schema}.membership AS m
     ON MIL.MEMBERSHIP_ID = M.MEMBERSHIP_ID
     JOIN {database}.REFERENCE.DIM_STORE AS DS
      ON DS.STORE_iD= M.STORE_ID""",
    column_list=[
        Column('membership_invitation_id', 'INT', uniqueness=True, key=True),
        Column('membership_invitation_link_id', 'INT', key=True),
        Column('invitee_membership_id', 'INT', key=True),
        Column('session_id', 'INT', key=True),
        Column('type', 'VARCHAR(15)'),
        Column('code', 'VARCHAR(50)'),
        Column('recipient_email', 'VARCHAR(75)'),
        Column('recipient_foreign_account_id', 'VARCHAR(50)'),
        Column('recipient_firstname', 'VARCHAR(25)'),
        Column('recipient_lastname', 'VARCHAR(25)'),
        Column('comment', 'VARCHAR(512)'),
        Column('sends', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)'),
        Column('datetime_last_sent', 'TIMESTAMP_NTZ(3)'),
        Column('statuscode', 'INT'),
        Column('hide', 'INT'),
        Column('incentive_object_id', 'INT'),
        Column('incentive_event_object_id', 'INT'),
        Column('third_party_invite', 'BOOLEAN'),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)'),
        Column('membership_membership_invitation_campaign_id', 'INT'),
        Column('membership_invite_source_id', 'INT'),
        Column('automatic_reminder', 'BOOLEAN'),
    ],
    watermark_column='datetime_modified',
)
