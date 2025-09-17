from include.airflow.hooks.google_export import GoogleExportHook
from include.airflow.operators.snowflake import SnowflakeWatermarkSqlOperator
import hashlib
import sys

from include.airflow.hooks.snowflake import SnowflakeHook

from google.ads.googleads.errors import GoogleAdsException


class GoogleConversionsExportOperator(SnowflakeWatermarkSqlOperator):
    def __init__(
        self, customer_id, conversion_action_id, store, country, *args, **kwargs
    ):
        super().__init__(sql_or_path="", *args, **kwargs)
        self.customer_id = customer_id
        self.conversion_action_id = conversion_action_id
        self.store = store
        self.country = country

    def post_conversions(
        self,
        client,
        customer_id,
        conversion_action_id,
        offline_user_data_job_type,
        external_id=None,
        advertiser_upload_date_time=None,
        bridge_map_version_id=None,
        partner_id=None,
        custom_key=None,
        custom_value=None,
        item_id=None,
        merchant_center_account_id=None,
        country_code=None,
        language_code=None,
        quantity=None,
        ad_user_data_consent=None,
        ad_personalization_consent=None,
    ):
        offline_user_data_job_service = client.get_service("OfflineUserDataJobService")

        # Create an offline user data job for uploading transactions.
        offline_user_data_job_resource_name = self.create_offline_user_data_job(
            client,
            offline_user_data_job_service,
            customer_id,
            offline_user_data_job_type,
            external_id,  # null
            advertiser_upload_date_time,  # null
            bridge_map_version_id,  # null
            partner_id,  # null
            custom_key,  # null
        )

        # Add transactions to the job.
        self.add_transactions_to_offline_user_data_job(
            client,
            offline_user_data_job_service,
            customer_id,
            offline_user_data_job_resource_name,
            conversion_action_id,
            custom_value,
            item_id,
            merchant_center_account_id,
            country_code,
            language_code,
            quantity,
            ad_user_data_consent,
            ad_personalization_consent,
        )
        offline_user_data_job_service.run_offline_user_data_job(
            resource_name=offline_user_data_job_resource_name
        )

        self.check_job_status(client, customer_id, offline_user_data_job_resource_name)

    def create_offline_user_data_job(
        self,
        client,
        offline_user_data_job_service,
        customer_id,
        offline_user_data_job_type,
        external_id,
        advertiser_upload_date_time,
        bridge_map_version_id,
        partner_id,
        custom_key,
    ):
        offline_user_data_job = client.get_type("OfflineUserDataJob")
        offline_user_data_job.type_ = offline_user_data_job_type
        if external_id is not None:
            offline_user_data_job.external_id = external_id

        store_sales_metadata = offline_user_data_job.store_sales_metadata
        store_sales_metadata.loyalty_fraction = 0.7
        store_sales_metadata.transaction_upload_fraction = 1.0

        if custom_key:
            store_sales_metadata.custom_key = custom_key

        if (
            offline_user_data_job_type
            == client.enums.OfflineUserDataJobTypeEnum.STORE_SALES_UPLOAD_THIRD_PARTY
        ):
            store_sales_third_party_metadata = store_sales_metadata.third_party_metadata
            store_sales_third_party_metadata.advertiser_upload_date_time = (
                advertiser_upload_date_time
            )
            store_sales_third_party_metadata.valid_transaction_fraction = 1.0
            store_sales_third_party_metadata.partner_match_fraction = 1.0
            store_sales_third_party_metadata.partner_upload_fraction = 1.0
            store_sales_third_party_metadata.bridge_map_version_id = (
                bridge_map_version_id
            )
            store_sales_third_party_metadata.partner_id = partner_id

        create_offline_user_data_job_response = (
            offline_user_data_job_service.create_offline_user_data_job(
                customer_id=customer_id, job=offline_user_data_job
            )
        )
        offline_user_data_job_resource_name = (
            create_offline_user_data_job_response.resource_name
        )
        print(
            "Created an offline user data job with resource name "
            f"'{offline_user_data_job_resource_name}'."
        )
        return offline_user_data_job_resource_name

    def add_transactions_to_offline_user_data_job(
        self,
        client,
        offline_user_data_job_service,
        customer_id,
        offline_user_data_job_resource_name,
        conversion_action_id,
        custom_value,
        item_id,
        merchant_center_account_id,
        country_code,
        language_code,
        quantity,
        ad_user_data_consent,
        ad_personalization_consent,
    ):
        operations = self.build_offline_user_data_job_operations(
            client,
            customer_id,
            conversion_action_id,
            custom_value,
            item_id,
            merchant_center_account_id,
            country_code,
            language_code,
            quantity,
            ad_user_data_consent,
            ad_personalization_consent,
        )

        request = client.get_type("AddOfflineUserDataJobOperationsRequest")
        request.resource_name = offline_user_data_job_resource_name
        request.enable_partial_failure = True
        request.enable_warnings = True
        request.operations = operations
        print("posting")

        response = offline_user_data_job_service.add_offline_user_data_job_operations(
            request=request,
        )

        # Print the error message for any partial failure error that is returned.
        if response.partial_failure_error:
            self.print_google_ads_failures(client, response.partial_failure_error)
        else:
            print(
                f"Successfully added {len(operations)} to the offline user data " "job."
            )

        # Print the message for any warnings that are returned.
        if response.warning:
            self.print_google_ads_failures(client, response.warning)

    def print_google_ads_failures(self, client, status):
        for detail in status.details:
            google_ads_failure = client.get_type("GoogleAdsFailure")
            failure_instance = type(google_ads_failure).deserialize(detail.value)
            for error in failure_instance.errors:
                print(
                    "A partial failure or warning at index "
                    f"{error.location.field_path_elements[0].index} occurred.\n"
                    f"Message: {error.message}\n"
                    f"Code: {error.error_code}"
                )

    def build_offline_user_data_job_operations(
        self,
        client,
        customer_id,
        conversion_action_id,
        custom_value,
        item_id,
        merchant_center_account_id,
        country_code,
        language_code,
        quantity,
        ad_user_data_consent,
        ad_personalization_consent,
    ):
        query = f"""
        select dc.email, dc.first_name, dc.last_name, dc.default_state_province,
            dc.default_postal_code, dc.country_code, fo.order_local_datetime,
            fo.subtotal_excl_tariff_local_amount
        from edw_prod.data_model.fact_order fo
        join edw_prod.data_model.dim_order_membership_classification domc on
            domc.order_membership_classification_key = fo.order_membership_classification_key
        join edw_prod.data_model.dim_store ds on fo.store_id = ds.store_id
        join edw_prod.data_model.dim_customer dc on dc.customer_id = fo.customer_id
        where store_brand_abbr = '{self.store}' and store_type = 'Retail'
            and domc.membership_order_type_l1 = 'Activating VIP'
            and store_country = '{self.country}'
            and fo.meta_update_datetime >= '{self.low_watermark}'

        """
        snowflake_hook = SnowflakeHook()
        cnx = snowflake_hook.get_conn()
        cur = cnx.cursor()
        cur.execute(query)
        rows = cur.fetchmany(100000)
        operations = []

        for row in rows:
            user_data_with_email_address_operation = client.get_type(
                "OfflineUserDataJobOperation"
            )
            user_data_with_email_address = user_data_with_email_address_operation.create
            email_identifier = client.get_type("UserIdentifier")
            email_identifier.hashed_email = self.normalize_and_hash(row[0])

            address_identifier = client.get_type("UserIdentifier")
            address_identifier.address_info.hashed_first_name = self.normalize_and_hash(
                row[1]
            )
            address_identifier.address_info.hashed_last_name = self.normalize_and_hash(
                row[2]
            )
            address_identifier.address_info.country_code = row[5]
            address_identifier.address_info.postal_code = row[4]
            address_identifier.address_info.state = row[3]

            user_data_with_email_address.user_identifiers.extend(
                [email_identifier, address_identifier]
            )
            user_data_with_email_address.transaction_attribute.conversion_action = (
                client.get_service("ConversionActionService").conversion_action_path(
                    customer_id, conversion_action_id
                )
            )
            user_data_with_email_address.transaction_attribute.currency_code = "USD"
            user_data_with_email_address.transaction_attribute.transaction_amount_micros = (
                row[7] * 1000000
            )
            user_data_with_email_address.transaction_attribute.transaction_date_time = (
                row[6].replace(microsecond=0)
            ).strftime("%Y-%m-%d %H:%M:%S")
            operations.append(user_data_with_email_address_operation)

        return operations

    def normalize_and_hash(self, s):
        return hashlib.sha256(s.strip().lower().encode()).hexdigest()

    def check_job_status(
        self, client, customer_id, offline_user_data_job_resource_name
    ):
        # Get the GoogleAdsService client.
        googleads_service = client.get_service("GoogleAdsService")

        # Construct a query to fetch the job status.
        query = f"""
            SELECT
              offline_user_data_job.resource_name,
              offline_user_data_job.id,
              offline_user_data_job.status,
              offline_user_data_job.type,
              offline_user_data_job.failure_reason
            FROM offline_user_data_job
            FROM offline_user_data_job
            ORDER BY offline_user_data_job.id DESC
            LIMIT 10"""

        response = googleads_service.search(customer_id=customer_id, query=query)
        for row in response:
            job = row.offline_user_data_job
            print(f"Resource Name: {job.resource_name}")
            print(f"Job ID: {job.id}")
            print(f"Status: {job.status.name}")  # Use .name to get the enum name
            print(f"Type: {job.type.name}")
            if job.status == client.enums.OfflineUserDataJobStatusEnum.FAILED:
                print(f"Failure Reason: {job.failure_reason}")
            else:
                print("No failure reason. Job is not in a failed state.")

    def watermark_execute(self, context=None):
        hook = GoogleExportHook()
        googleads_client = hook.get_conn()
        try:
            self.post_conversions(
                googleads_client,
                self.customer_id,
                self.conversion_action_id,
                googleads_client.enums.OfflineUserDataJobTypeEnum.STORE_SALES_UPLOAD_FIRST_PARTY,
            )
        except GoogleAdsException as ex:
            print(
                f"Request with ID '{ex.request_id}' failed with status "
                f"'{ex.error.code().name}' and includes the following errors:"
            )
            for error in ex.failure.errors:
                print(f"\tError with message '{error.message}'.")
                if error.location:
                    for field_path_element in error.location.field_path_elements:
                        print(f"\t\tOn field: {field_path_element.field_name}")
            sys.exit(1)
