from dataclasses import dataclass

from include.utils.snowflake import Column


@dataclass
class Config:
    sailpoint_operator: str
    database: str
    schema: str
    table: str
    source_ids: str
    column_list: list
    endpoint: str | None = None
    yr_mth = "{{macros.datetime.now().strftime('%Y%m')}}"

    @property
    def s3_prefix(self):
        return f'lake/{self.database}.{self.schema}.{self.table}/v2'


config_list = [
    Config(
        sailpoint_operator='SailpointGetAccounts',
        database='lake',
        schema='sailpoint',
        table='accounts',
        endpoint='accounts',
        source_ids='"2c9180867efe8d96017efffc51432516","2c9180877b36c489017b57cdd2d94e5b"',
        column_list=[
            Column('id', 'VARCHAR', source_name='id', uniqueness=True),
            Column('name', 'VARCHAR', source_name='name'),
            Column('created', 'TIMESTAMP_LTZ(3)', source_name='created'),
            Column('modified', 'TIMESTAMP_LTZ(3)', source_name='modified'),
            Column('source_id', 'VARCHAR', source_name='sourceId'),
            Column('identity_id', 'VARCHAR', source_name='identityId'),
            Column('description', 'VARCHAR', source_name='description'),
            Column('has_entitlements', 'BOOLEAN', source_name='hasEntitlements'),
            Column('disabled', 'BOOLEAN', source_name='disabled'),
            Column('locked', 'BOOLEAN', source_name='locked'),
            Column('uuid', 'VARCHAR', source_name='uuid'),
            Column('native_identity', 'VARCHAR', source_name='nativeIdentity'),
            Column('features', 'VARCHAR', source_name='features'),
            Column('uncorrelated', 'BOOLEAN', source_name='uncorrelated'),
            Column('manually_correlated', 'BOOLEAN', source_name='manuallyCorrelated'),
            Column('system_account', 'BOOLEAN', source_name='systemAccount'),
            Column('authoritative', 'BOOLEAN', source_name='authoritative'),
            Column('attributes_manager', 'VARCHAR', source_name='attributes_manager'),
            Column('attributes_city', 'VARCHAR', source_name='attributes_city'),
            Column(
                'attributes_risk_last_updated_date_time',
                'TIMESTAMP_LTZ(3)',
                source_name='attributes_riskLastUpdatedDateTime',
            ),
            Column(
                'attributes_password_policies', 'VARCHAR', source_name='attributes_passwordPolicies'
            ),
            Column('attributes_country', 'VARCHAR', source_name='attributes_country'),
            Column('attributes_groups_alt', 'VARIANT', source_name='attributes_groups'),
            Column('attributes_risk_level', 'VARCHAR', source_name='attributes_riskLevel'),
            Column(
                'attributes_preferred_language',
                'VARCHAR',
                source_name='attributes_preferredLanguage',
            ),
            Column('attributes', 'VARCHAR', source_name='attributes'),
            Column(
                'attributes_proxy_addresses', 'VARIANT', source_name='attributes_proxyAddresses'
            ),
            Column(
                'attributes_site_collections', 'VARIANT', source_name='attributes_SiteCollections'
            ),
            Column('attributes_domain', 'VARCHAR', source_name='attributes_domain'),
            Column('attributes_name', 'VARCHAR', source_name='attributes_Name'),
            Column('attributes_risk_state', 'VARCHAR', source_name='attributes_riskState'),
            Column(
                'attributes_facsimile_telephone_number',
                'VARCHAR',
                source_name='attributes_facsimileTelephoneNumber',
            ),
            Column(
                'attributes_on_premises_security_identifier',
                'VARCHAR',
                source_name='attributes_onPremisesSecurityIdentifier',
            ),
            Column('attributes_login_name', 'VARCHAR', source_name='attributes_LoginName'),
            Column('attributes_risk_detail', 'VARCHAR', source_name='attributes_riskDetail'),
            Column('attributes_mobile', 'VARCHAR', source_name='attributes_mobile'),
            Column('attributes_email_alt', 'VARCHAR', source_name='attributes_email'),
            Column('attributes_email', 'VARCHAR', source_name='attributes_Email'),
            Column('attributes_id', 'VARCHAR', source_name='attributes_ID'),
            Column('attributes_id_alt', 'VARCHAR', source_name='attributes_id'),
            Column('attributes_last_name', 'VARCHAR', source_name='attributes_LastName'),
            Column('attributes_job_title', 'VARCHAR', source_name='attributes_jobTitle'),
            Column(
                'attributes_last_dir_sync_time',
                'TIMESTAMP_LTZ(3)',
                source_name='attributes_lastDirSyncTime',
            ),
            Column('attributes_sign_in_names', 'VARCHAR', source_name='attributes_signInNames'),
            Column('attributes_iiqdisabled', 'BOOLEAN', source_name='attributes_IIQDisabled'),
            Column(
                'attributes_user_principal_name',
                'VARCHAR',
                source_name='attributes_userPrincipalName',
            ),
            Column(
                'attributes_account_enabled', 'BOOLEAN', source_name='attributes_accountEnabled'
            ),
            Column(
                'attributes_physical_delivery_office_name',
                'VARCHAR',
                source_name='attributes_physicalDeliveryOfficeName',
            ),
            Column('attributes_object_id', 'VARCHAR', source_name='attributes_objectId'),
            Column('attributes_state', 'VARCHAR', source_name='attributes_state'),
            Column('attributes_disabled_plans', 'VARCHAR', source_name='attributes_disabledPlans'),
            Column('attributes_user_type', 'VARCHAR', source_name='attributes_userType'),
            Column('attributes_creation_type', 'VARCHAR', source_name='attributes_creationType'),
            Column('attributes_street_address', 'VARCHAR', source_name='attributes_streetAddress'),
            Column('attributes_assigned_plans', 'VARIANT', source_name='attributes_assignedPlans'),
            Column(
                'attributes_telephone_number', 'VARCHAR', source_name='attributes_telephoneNumber'
            ),
            Column(
                'attributes_id_now_description',
                'VARCHAR',
                source_name='attributes_idNowDescription',
            ),
            Column('attributes_surname', 'VARCHAR', source_name='attributes_surname'),
            Column('attributes_postal_code', 'NUMBER', source_name='attributes_postalCode'),
            Column('attributes_user_id', 'VARCHAR', source_name='attributes_UserID'),
            Column('attributes_display_name', 'VARCHAR', source_name='attributes_displayName'),
            Column('attributes_system_role', 'VARCHAR', source_name='attributes_systemRole'),
            Column('attributes_mail', 'VARCHAR', source_name='attributes_mail'),
            Column('attributes_immutable_id', 'VARCHAR', source_name='attributes_immutableId'),
            Column('attributes_given_name', 'VARCHAR', source_name='attributes_givenName'),
            Column(
                'attributes_dir_sync_enabled', 'BOOLEAN', source_name='attributes_dirSyncEnabled'
            ),
            Column('attributes_department', 'VARCHAR', source_name='attributes_department'),
            Column(
                'attributes_assigned_licenses', 'VARIANT', source_name='attributes_assignedLicenses'
            ),
            Column('attributes_other_mails', 'VARIANT', source_name='attributes_otherMails'),
            Column(
                'attributes_service_principals',
                'VARIANT',
                source_name='attributes_servicePrincipals',
            ),
            Column('attributes_mail_nickname', 'VARCHAR', source_name='attributes_mailNickname'),
            Column(
                'attributes_user_identities', 'VARCHAR', source_name='attributes_userIdentities'
            ),
            Column('attributes_groups', 'VARIANT', source_name='attributes_Groups'),
            Column('attributes_usage_location', 'VARCHAR', source_name='attributes_usageLocation'),
            Column('attributes_admin_of_sites', 'VARCHAR', source_name='attributes_AdminOfSites'),
            Column('attributes_first_name', 'VARCHAR', source_name='attributes_FirstName'),
            Column('attributes_roles', 'VARCHAR', source_name='attributes_roles'),
            Column('updated_at', 'TIMESTAMP_LTZ(3)', source_name='updated_at', delta_column=True),
        ],
    ),
    Config(
        sailpoint_operator='SailpointGetAccessProfilesByIdentity',
        database='lake',
        schema='sailpoint',
        table='access_profiles_by_identity',
        endpoint='accounts',
        source_ids='"2c9180867efe8d96017efffc51432516","2c9180877b36c489017b57cdd2d94e5b"',
        column_list=[
            Column('identity_id', 'VARCHAR', source_name='identity_id', uniqueness=True),
            Column('id', 'VARCHAR', source_name='id', uniqueness=True),
            Column('accessType', 'VARCHAR', source_name='accessType'),
            Column('displayName', 'VARCHAR', source_name='displayName'),
            Column('sourceName', 'VARCHAR', source_name='sourceName'),
            Column('entitlementCount', 'NUMBER', source_name='entitlementCount'),
            Column('description', 'VARCHAR', source_name='description'),
            Column('sourceId', 'VARCHAR', source_name='sourceId'),
            Column('appRefs', 'VARIANT', source_name='appRefs'),
            Column('updated_at', 'TIMESTAMP_LTZ(3)', source_name='updated_at', delta_column=True),
        ],
    ),
    Config(
        sailpoint_operator='SailpointGetAccounts',
        database='lake',
        schema='sailpoint',
        table='workday_accounts',
        endpoint='accounts',
        source_ids='"2c918086843dce5d018459b0aab94d89","2c9180877a3f688b017a4b6756353e30"',
        column_list=[
            Column('id', 'VARCHAR', source_name='id', uniqueness=True),
            Column('name', 'VARCHAR', source_name='name'),
            Column('created', 'TIMESTAMP_LTZ(3)', source_name='created'),
            Column('modified', 'TIMESTAMP_LTZ(3)', source_name='modified'),
            Column('authoritative', 'BOOLEAN', source_name='authoritative'),
            Column('system_account', 'BOOLEAN', source_name='systemAccount'),
            Column('uncorrelated', 'BOOLEAN', source_name='uncorrelated'),
            Column('features', 'VARCHAR', source_name='features'),
            Column('uuid', 'VARCHAR', source_name='uuid'),
            Column('native_identity', 'NUMBER', source_name='nativeIdentity'),
            Column('description', 'VARCHAR', source_name='description'),
            Column('disabled', 'BOOLEAN', source_name='disabled'),
            Column('locked', 'BOOLEAN', source_name='locked'),
            Column('manually_correlated', 'BOOLEAN', source_name='manuallyCorrelated'),
            Column('has_entitlements', 'BOOLEAN', source_name='hasEntitlements'),
            Column('source_id', 'VARCHAR', source_name='sourceId', uniqueness=True),
            Column('source_name', 'VARCHAR', source_name='sourceName'),
            Column('identity_id', 'VARCHAR', source_name='identityId'),
            Column('attributes_location', 'VARCHAR', source_name='attributes_LOCATION'),
            Column('attributes_iiq_disabled', 'BOOLEAN', source_name='attributes_IIQDisabled'),
            Column('attributes_home_phone__c', 'VARCHAR', source_name='attributes_home_phone__c'),
            Column('attributes_job_title__c', 'VARCHAR', source_name='attributes_Job_Title__c'),
            Column('attributes_state', 'VARCHAR', source_name='attributes_STATE'),
            Column('attributes_cost_center', 'VARCHAR', source_name='attributes_COST_CENTER'),
            Column(
                'attributes_provision_15five__c',
                'BOOLEAN',
                source_name='attributes_Provision_15Five__c',
            ),
            Column('attributes_class', 'VARCHAR', source_name='attributes_CLASS'),
            Column('attributes_worker_type__c', 'VARCHAR', source_name='attributes_Worker_Type__c'),
            Column('attributes_last_name', 'VARCHAR', source_name='attributes_LAST_NAME'),
            Column('attributes_worker_name', 'VARCHAR', source_name='attributes_WORKER_NAME'),
            Column(
                'attributes_worker_s_manager__c',
                'VARCHAR',
                source_name='attributes_Worker_s_Manager__c',
            ),
            Column(
                'attributes_location_ref_id__c',
                'VARCHAR',
                source_name='attributes_Location_RefID__c',
            ),
            Column('attributes_first_name', 'VARCHAR', source_name='attributes_FIRST_NAME'),
            Column('attributes_country', 'VARCHAR', source_name='attributes_COUNTRY'),
            Column(
                'attributes_management_level__c',
                'VARCHAR',
                source_name='attributes_Management_Level__c',
            ),
            Column('attributes_postal_code', 'VARCHAR', source_name='attributes_POSTAL_CODE'),
            Column('attributes_job_code', 'VARCHAR', source_name='attributes_JOBCODE'),
            Column('attributes_user_id', 'VARCHAR', source_name='attributes_USERID'),
            Column('attributes_home_email__c', 'VARCHAR', source_name='attributes_home_email__c'),
            Column(
                'attributes_provision_lilearn__c',
                'VARCHAR',
                source_name='attributes_provision_lilearn__c',
            ),
            Column(
                'attributes_retail_district__c',
                'VARCHAR',
                source_name='attributes_Retail_District__c',
            ),
            Column(
                'attributes_id_now_description',
                'VARCHAR',
                source_name='attributes_idNowDescription',
            ),
            Column('attributes_domain_name__c', 'VARCHAR', source_name='attributes_DomainName__c'),
            Column('attributes_address_line_1', 'VARCHAR', source_name='attributes_ADDRESS_LINE_1'),
            Column(
                'attributes_preferred_name__c',
                'VARCHAR',
                source_name='attributes_Preferred_Name__c',
            ),
            Column('attributes_full_part_time', 'VARCHAR', source_name='attributes_FULLPARTTIME'),
            Column(
                'attributes_wd_role_type__c', 'VARCHAR', source_name='attributes_WD_Role_Type__c'
            ),
            Column(
                'attributes_supervisory_organization__c',
                'VARCHAR',
                source_name='attributes_Supervisory_Organization__c',
            ),
            Column(
                'attributes_worker_status__c', 'VARCHAR', source_name='attributes_Worker_Status__c'
            ),
            Column('attributes_department', 'VARCHAR', source_name='attributes_DEPARTMENT'),
            Column('attributes_ad_req__c', 'BOOLEAN', source_name='attributes_ADReq__c'),
            Column('attributes_file_number', 'NUMBER', source_name='attributes_FILENUMBER'),
            Column('attributes_manager_id', 'NUMBER', source_name='attributes_MANAGER_ID'),
            Column('attributes_manager_id__c', 'NUMBER', source_name='attributes_Manager_ID__c'),
            Column('attributes_employee_type', 'VARCHAR', source_name='attributes_EMPLOYEE_TYPE'),
            Column('attributes_cost_center__c', 'VARCHAR', source_name='attributes_Cost_Center__c'),
            Column('attributes_city', 'VARCHAR', source_name='attributes_CITY'),
            Column(
                'attributes_cost_center_reference_id',
                'VARCHAR',
                source_name='attributes_COST_CENTER_REFERENCE_ID',
            ),
            Column(
                'attributes_manager_email__c', 'VARCHAR', source_name='attributes_managerEmail__c'
            ),
            Column('attributes_is_retail__c', 'VARCHAR', source_name='attributes_is_retail__c'),
            Column('attributes_hire_date', 'DATE', source_name='attributes_HIREDATE'),
            Column('attributes_address_work', 'VARCHAR', source_name='attributes_ADDRESS_WORK'),
            Column('attributes_brand_id__c', 'NUMBER', source_name='attributes_Brand_ID__c'),
            Column(
                'attributes_retail_region__c', 'VARCHAR', source_name='attributes_Retail_Region__c'
            ),
            Column('attributes_job_title', 'VARCHAR', source_name='attributes_JOBTITLE'),
            Column('attributes_email_work__c', 'VARCHAR', source_name='attributes_Email_-_Work__c'),
            Column('attributes_middle_name', 'VARCHAR', source_name='attributes_MIDDLE_NAME'),
            Column(
                'attributes_email_address_work',
                'VARCHAR',
                source_name='attributes_EMAIL_ADDRESS_WORK',
            ),
            Column(
                'attributes_legal_middle_name',
                'VARCHAR',
                source_name='attributes_LEGAL_MIDDLE_NAME',
            ),
            Column('updated_at', 'TIMESTAMP_LTZ(3)', source_name='updated_at', delta_column=True),
        ],
    ),
    Config(
        sailpoint_operator='SailpointGetAccounts',
        database='lake',
        schema='sailpoint',
        table='access_request_items',
        endpoint='access-request-items',
        source_ids='',
        column_list=[
            Column('state', 'VARCHAR', source_name='state'),
            Column('reviewer_comment', 'VARCHAR', source_name='reviewerComment'),
            Column('reviewed_by_type', 'VARCHAR', source_name='reviewedBy_type'),
            Column('reviewed_by_id', 'VARCHAR', source_name='reviewedBy_id'),
            Column('reviewed_by_name', 'VARCHAR', source_name='reviewedBy_name'),
            Column('requester_type', 'VARCHAR', source_name='requester_type'),
            Column('requester_id', 'VARCHAR', source_name='requester_id'),
            Column('requester_name', 'VARCHAR', source_name='requester_name'),
            Column('requested_for_type', 'VARCHAR', source_name='requestedFor_type'),
            Column('requested_for_id', 'VARCHAR', source_name='requestedFor_id'),
            Column('requested_for_name', 'VARCHAR', source_name='requestedFor_name'),
            Column('owner_type', 'VARCHAR', source_name='owner_type'),
            Column('owner_id', 'VARCHAR', source_name='owner_id'),
            Column('owner_name', 'VARCHAR', source_name='owner_name'),
            Column('requester_comment_comment', 'VARCHAR', source_name='requesterComment_comment'),
            Column(
                'requester_comment_author_type',
                'VARCHAR',
                source_name='requesterComment_author_type',
            ),
            Column(
                'requester_comment_author_id', 'VARCHAR', source_name='requesterComment_author_id'
            ),
            Column(
                'requester_comment_author_name',
                'VARCHAR',
                source_name='requesterComment_author_name',
            ),
            Column('requester_comment_created', 'VARCHAR', source_name='requesterComment_created'),
            Column(
                'previous_reviewers_comments', 'VARCHAR', source_name='previousReviewersComments'
            ),
            Column('forward_history', 'VARCHAR', source_name='forwardHistory'),
            Column('request_created', 'VARCHAR', source_name='requestCreated'),
            Column('requested_object_name', 'VARCHAR', source_name='requestedObject_name'),
            Column(
                'requested_object_description', 'VARCHAR', source_name='requestedObject_description'
            ),
            Column('requested_object_type', 'VARCHAR', source_name='requestedObject_type'),
            Column('requested_object_id', 'VARCHAR', source_name='requestedObject_id'),
            Column('request_type', 'VARCHAR', source_name='requestType'),
            Column('remove_date', 'VARCHAR', source_name='removeDate'),
            Column('current_remove_date', 'VARCHAR', source_name='currentRemoveDate'),
            Column(
                'remove_date_update_requested', 'BOOLEAN', source_name='removeDateUpdateRequested'
            ),
            Column(
                'comment_required_when_rejected',
                'BOOLEAN',
                source_name='commentRequiredWhenRejected',
            ),
            Column('sod_violation_context', 'VARCHAR', source_name='sodViolationContext'),
            Column(
                'pre_approval_trigger_result_comment',
                'VARCHAR',
                source_name='preApprovalTriggerResult_comment',
            ),
            Column(
                'pre_approval_trigger_result_decision',
                'VARCHAR',
                source_name='preApprovalTriggerResult_decision',
            ),
            Column(
                'pre_approval_trigger_result_reviewer',
                'VARCHAR',
                source_name='preApprovalTriggerResult_reviewer',
            ),
            Column(
                'pre_approval_trigger_result_date',
                'VARCHAR',
                source_name='preApprovalTriggerResult_date',
            ),
            Column(
                'client_metadata_requested_app_icon',
                'VARCHAR',
                source_name='clientMetadata_requestedAppIcon',
            ),
            Column(
                'client_metadata_requested_app_name',
                'VARCHAR',
                source_name='clientMetadata_requestedAppName',
            ),
            Column(
                'client_metadata_requested_app_id',
                'VARCHAR',
                source_name='clientMetadata_requestedAppId',
            ),
            Column('requested_accounts', 'VARCHAR', source_name='requestedAccounts'),
            Column('assignment_context', 'VARCHAR', source_name='assignmentContext'),
            Column('id', 'VARCHAR', source_name='id', uniqueness=True),
            Column('name', 'VARCHAR', source_name='name'),
            Column('created', 'VARCHAR', source_name='created'),
            Column('modified', 'VARCHAR', source_name='modified'),
            Column('updated_at', 'TIMESTAMP_LTZ(3)', source_name='updated_at', delta_column=True),
        ],
    ),
]
