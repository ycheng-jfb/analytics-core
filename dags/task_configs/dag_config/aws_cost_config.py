from include.utils.snowflake import Column

column_list = [
    Column(
        "identity_line_item_id",
        "VARCHAR",
        source_name="identity/LineItemId",
        uniqueness=True,
    ),
    Column("identity_time_interval", "VARCHAR", source_name="identity/TimeInterval"),
    Column("bill_invoice_id", "VARCHAR", source_name="bill/InvoiceId"),
    Column("bill_invoicing_entity", "VARCHAR", source_name="bill/InvoicingEntity"),
    Column("bill_billing_entity", "VARCHAR", source_name="bill/BillingEntity"),
    Column("bill_bill_type", "VARCHAR", source_name="bill/BillType"),
    Column("bill_payer_account_id", "NUMBER", source_name="bill/PayerAccountId"),
    Column(
        "bill_billing_period_start_date",
        "TIMESTAMP_LTZ(3)",
        source_name="bill/BillingPeriodStartDate",
    ),
    Column(
        "bill_billing_period_end_date",
        "TIMESTAMP_LTZ(3)",
        source_name="bill/BillingPeriodEndDate",
    ),
    Column(
        "line_item_usage_account_id", "NUMBER", source_name="lineItem/UsageAccountId"
    ),
    Column("line_item_line_item_type", "VARCHAR", source_name="lineItem/LineItemType"),
    Column(
        "line_item_usage_start_date",
        "TIMESTAMP_LTZ(3)",
        source_name="lineItem/UsageStartDate",
    ),
    Column(
        "line_item_usage_end_date",
        "TIMESTAMP_LTZ(3)",
        source_name="lineItem/UsageEndDate",
    ),
    Column("line_item_product_code", "VARCHAR", source_name="lineItem/ProductCode"),
    Column("line_item_usage_type", "VARCHAR", source_name="lineItem/UsageType"),
    Column("line_item_operation", "VARCHAR", source_name="lineItem/Operation"),
    Column(
        "line_item_availability_zone",
        "VARCHAR",
        source_name="lineItem/AvailabilityZone",
    ),
    Column("line_item_resource_id", "VARCHAR", source_name="lineItem/ResourceId"),
    Column(
        "line_item_usage_amount", "NUMBER(38,10)", source_name="lineItem/UsageAmount"
    ),
    Column(
        "line_item_normalization_factor",
        "VARCHAR",
        source_name="lineItem/NormalizationFactor",
    ),
    Column(
        "line_item_normalized_usage_amount",
        "VARCHAR",
        source_name="lineItem/NormalizedUsageAmount",
    ),
    Column("line_item_currency_code", "VARCHAR", source_name="lineItem/CurrencyCode"),
    Column(
        "line_item_unblended_rate",
        "NUMBER(38,10)",
        source_name="lineItem/UnblendedRate",
    ),
    Column(
        "line_item_unblended_cost",
        "NUMBER(38,10)",
        source_name="lineItem/UnblendedCost",
    ),
    Column(
        "line_item_blended_rate", "NUMBER(38,10)", source_name="lineItem/BlendedRate"
    ),
    Column(
        "line_item_blended_cost", "NUMBER(38,10)", source_name="lineItem/BlendedCost"
    ),
    Column(
        "line_item_line_item_description",
        "VARCHAR",
        source_name="lineItem/LineItemDescription",
    ),
    Column("line_item_tax_type", "VARCHAR", source_name="lineItem/TaxType"),
    Column("line_item_legal_entity", "VARCHAR", source_name="lineItem/LegalEntity"),
    Column("product_product_name", "VARCHAR", source_name="product/ProductName"),
    Column("product_purchase_option", "VARCHAR", source_name="product/PurchaseOption"),
    Column(
        "product_account_assistance", "VARCHAR", source_name="product/accountAssistance"
    ),
    Column("product_alarm_type", "VARCHAR", source_name="product/alarmType"),
    Column(
        "product_architectural_review",
        "VARCHAR",
        source_name="product/architecturalReview",
    ),
    Column(
        "product_architecture_support",
        "VARCHAR",
        source_name="product/architectureSupport",
    ),
    Column("product_attachment_type", "VARCHAR", source_name="product/attachmentType"),
    Column("product_availability", "VARCHAR", source_name="product/availability"),
    Column(
        "product_availability_zone", "VARCHAR", source_name="product/availabilityZone"
    ),
    Column("product_best_practices", "VARCHAR", source_name="product/bestPractices"),
    Column("product_cache_engine", "VARCHAR", source_name="product/cacheEngine"),
    Column("product_capacity", "VARCHAR", source_name="product/capacity"),
    Column("product_capacitystatus", "VARCHAR", source_name="product/capacitystatus"),
    Column(
        "product_case_severityresponse_times",
        "VARCHAR",
        source_name="product/caseSeverityresponseTimes",
    ),
    Column("product_category", "VARCHAR", source_name="product/category"),
    Column("product_ci_type", "VARCHAR", source_name="product/ciType"),
    Column(
        "product_classicnetworkingsupport",
        "VARCHAR",
        source_name="product/classicnetworkingsupport",
    ),
    Column("product_clock_speed", "VARCHAR", source_name="product/clockSpeed"),
    Column("product_component", "VARCHAR", source_name="product/component"),
    Column("product_compute_family", "VARCHAR", source_name="product/computeFamily"),
    Column("product_compute_type", "VARCHAR", source_name="product/computeType"),
    Column("product_connection_type", "VARCHAR", source_name="product/connectionType"),
    Column("product_content_type", "VARCHAR", source_name="product/contentType"),
    Column("product_cputype", "VARCHAR", source_name="product/cputype"),
    Column(
        "product_current_generation", "VARCHAR", source_name="product/currentGeneration"
    ),
    Column(
        "product_customer_service_and_communities",
        "VARCHAR",
        source_name="product/customerServiceAndCommunities",
    ),
    Column("product_data", "VARCHAR", source_name="product/data"),
    Column("product_database_engine", "VARCHAR", source_name="product/databaseEngine"),
    Column("product_datatransferout", "VARCHAR", source_name="product/datatransferout"),
    Column(
        "product_dedicated_ebs_throughput",
        "VARCHAR",
        source_name="product/dedicatedEbsThroughput",
    ),
    Column(
        "product_deployment_option", "VARCHAR", source_name="product/deploymentOption"
    ),
    Column("product_description", "VARCHAR", source_name="product/description"),
    Column(
        "product_direct_connect_location",
        "VARCHAR",
        source_name="product/directConnectLocation",
    ),
    Column("product_durability", "VARCHAR", source_name="product/durability"),
    Column("product_ecu", "VARCHAR", source_name="product/ecu"),
    Column("product_endpoint", "VARCHAR", source_name="product/endpoint"),
    Column("product_endpoint_type", "VARCHAR", source_name="product/endpointType"),
    Column("product_engine_code", "VARCHAR", source_name="product/engineCode"),
    Column(
        "product_enhanced_networking_supported",
        "VARCHAR",
        source_name="product/enhancedNetworkingSupported",
    ),
    Column(
        "product_equivalentondemandsku",
        "VARCHAR",
        source_name="product/equivalentondemandsku",
    ),
    Column("product_event_type", "VARCHAR", source_name="product/eventType"),
    Column("product_fee_code", "VARCHAR", source_name="product/feeCode"),
    Column("product_fee_description", "VARCHAR", source_name="product/feeDescription"),
    Column("product_finding_group", "VARCHAR", source_name="product/findingGroup"),
    Column("product_finding_source", "VARCHAR", source_name="product/findingSource"),
    Column("product_finding_storage", "VARCHAR", source_name="product/findingStorage"),
    Column("product_free_query_types", "VARCHAR", source_name="product/freeQueryTypes"),
    Column("product_from_location", "VARCHAR", source_name="product/fromLocation"),
    Column(
        "product_from_location_type", "VARCHAR", source_name="product/fromLocationType"
    ),
    Column("product_from_region_code", "VARCHAR", source_name="product/fromRegionCode"),
    Column("product_georegioncode", "VARCHAR", source_name="product/georegioncode"),
    Column("product_gpu", "VARCHAR", source_name="product/gpu"),
    Column("product_gpu_memory", "VARCHAR", source_name="product/gpuMemory"),
    Column("product_group", "VARCHAR", source_name="product/group"),
    Column(
        "product_group_description", "VARCHAR", source_name="product/groupDescription"
    ),
    Column(
        "product_included_services", "VARCHAR", source_name="product/includedServices"
    ),
    Column("product_instance", "VARCHAR", source_name="product/instance"),
    Column("product_instance_family", "VARCHAR", source_name="product/instanceFamily"),
    Column("product_instance_name", "VARCHAR", source_name="product/instanceName"),
    Column("product_instance_type", "VARCHAR", source_name="product/instanceType"),
    Column(
        "product_instance_type_family",
        "VARCHAR",
        source_name="product/instanceTypeFamily",
    ),
    Column(
        "product_intel_avx_2_available",
        "VARCHAR",
        source_name="product/intelAvx2Available",
    ),
    Column(
        "product_intel_avx_available",
        "VARCHAR",
        source_name="product/intelAvxAvailable",
    ),
    Column(
        "product_intel_turbo_available",
        "VARCHAR",
        source_name="product/intelTurboAvailable",
    ),
    Column("product_launch_support", "VARCHAR", source_name="product/launchSupport"),
    Column("product_license_model", "VARCHAR", source_name="product/licenseModel"),
    Column("product_location", "VARCHAR", source_name="product/location"),
    Column("product_location_type", "VARCHAR", source_name="product/locationType"),
    Column(
        "product_logs_destination", "VARCHAR", source_name="product/logsDestination"
    ),
    Column("product_marketoption", "VARCHAR", source_name="product/marketoption"),
    Column(
        "product_max_iops_burst_performance",
        "VARCHAR",
        source_name="product/maxIopsBurstPerformance",
    ),
    Column("product_max_iopsvolume", "VARCHAR", source_name="product/maxIopsvolume"),
    Column(
        "product_max_throughputvolume",
        "VARCHAR",
        source_name="product/maxThroughputvolume",
    ),
    Column("product_max_volume_size", "VARCHAR", source_name="product/maxVolumeSize"),
    Column(
        "product_maximum_capacity", "VARCHAR", source_name="product/maximumCapacity"
    ),
    Column(
        "product_maximum_extended_storage",
        "VARCHAR",
        source_name="product/maximumExtendedStorage",
    ),
    Column("product_memory", "VARCHAR", source_name="product/memory"),
    Column("product_memorytype", "VARCHAR", source_name="product/memorytype"),
    Column(
        "product_message_delivery_frequency",
        "VARCHAR",
        source_name="product/messageDeliveryFrequency",
    ),
    Column(
        "product_message_delivery_order",
        "VARCHAR",
        source_name="product/messageDeliveryOrder",
    ),
    Column("product_min_volume_size", "VARCHAR", source_name="product/minVolumeSize"),
    Column(
        "product_network_performance",
        "VARCHAR",
        source_name="product/networkPerformance",
    ),
    Column(
        "product_normalization_size_factor",
        "VARCHAR",
        source_name="product/normalizationSizeFactor",
    ),
    Column(
        "product_operating_system", "VARCHAR", source_name="product/operatingSystem"
    ),
    Column("product_operation", "VARCHAR", source_name="product/operation"),
    Column(
        "product_operations_support", "VARCHAR", source_name="product/operationsSupport"
    ),
    Column("product_origin", "VARCHAR", source_name="product/origin"),
    Column("product_physical_cpu", "VARCHAR", source_name="product/physicalCpu"),
    Column("product_physical_gpu", "VARCHAR", source_name="product/physicalGpu"),
    Column(
        "product_physical_processor", "VARCHAR", source_name="product/physicalProcessor"
    ),
    Column(
        "product_platoclassificationtype",
        "VARCHAR",
        source_name="product/platoclassificationtype",
    ),
    Column(
        "product_platoinstancename", "VARCHAR", source_name="product/platoinstancename"
    ),
    Column(
        "product_platoinstancetype", "VARCHAR", source_name="product/platoinstancetype"
    ),
    Column(
        "product_platopricingtype", "VARCHAR", source_name="product/platopricingtype"
    ),
    Column(
        "product_platoprotocoltype", "VARCHAR", source_name="product/platoprotocoltype"
    ),
    Column(
        "product_platostoragetype", "VARCHAR", source_name="product/platostoragetype"
    ),
    Column(
        "product_platotrafficdirection",
        "VARCHAR",
        source_name="product/platotrafficdirection",
    ),
    Column(
        "product_platotransfertype", "VARCHAR", source_name="product/platotransfertype"
    ),
    Column("product_platousagetype", "VARCHAR", source_name="product/platousagetype"),
    Column("product_platovolumetype", "VARCHAR", source_name="product/platovolumetype"),
    Column("product_port_speed", "VARCHAR", source_name="product/portSpeed"),
    Column("product_pre_installed_sw", "VARCHAR", source_name="product/preInstalledSw"),
    Column(
        "product_proactive_guidance", "VARCHAR", source_name="product/proactiveGuidance"
    ),
    Column(
        "product_processor_architecture",
        "VARCHAR",
        source_name="product/processorArchitecture",
    ),
    Column(
        "product_processor_features", "VARCHAR", source_name="product/processorFeatures"
    ),
    Column("product_product_family", "VARCHAR", source_name="product/productFamily"),
    Column(
        "product_programmatic_case_management",
        "VARCHAR",
        source_name="product/programmaticCaseManagement",
    ),
    Column("product_purchaseterm", "VARCHAR", source_name="product/purchaseterm"),
    Column("product_queue_type", "VARCHAR", source_name="product/queueType"),
    Column("product_recipient", "VARCHAR", source_name="product/recipient"),
    Column("product_region", "VARCHAR", source_name="product/region"),
    Column("product_region_code", "VARCHAR", source_name="product/regionCode"),
    Column(
        "product_request_description",
        "VARCHAR",
        source_name="product/requestDescription",
    ),
    Column("product_request_type", "VARCHAR", source_name="product/requestType"),
    Column(
        "product_resource_endpoint", "VARCHAR", source_name="product/resourceEndpoint"
    ),
    Column("product_routing_target", "VARCHAR", source_name="product/routingTarget"),
    Column("product_routing_type", "VARCHAR", source_name="product/routingType"),
    Column("product_servicecode", "VARCHAR", source_name="product/servicecode"),
    Column("product_servicename", "VARCHAR", source_name="product/servicename"),
    Column("product_sku", "VARCHAR", source_name="product/sku"),
    Column("product_standard_group", "VARCHAR", source_name="product/standardGroup"),
    Column(
        "product_standard_storage", "VARCHAR", source_name="product/standardStorage"
    ),
    Column(
        "product_standard_storage_retention_included",
        "VARCHAR",
        source_name="product/standardStorageRetentionIncluded",
    ),
    Column("product_storage", "VARCHAR", source_name="product/storage"),
    Column("product_storage_class", "VARCHAR", source_name="product/storageClass"),
    Column(
        "product_storage_description",
        "VARCHAR",
        source_name="product/storageDescription",
    ),
    Column("product_storage_media", "VARCHAR", source_name="product/storageMedia"),
    Column("product_storage_type", "VARCHAR", source_name="product/storageType"),
    Column(
        "product_technical_support", "VARCHAR", source_name="product/technicalSupport"
    ),
    Column("product_tenancy", "VARCHAR", source_name="product/tenancy"),
    Column(
        "product_thirdparty_software_support",
        "VARCHAR",
        source_name="product/thirdpartySoftwareSupport",
    ),
    Column("product_tiertype", "VARCHAR", source_name="product/tiertype"),
    Column("product_to_location", "VARCHAR", source_name="product/toLocation"),
    Column("product_to_location_type", "VARCHAR", source_name="product/toLocationType"),
    Column("product_to_region_code", "VARCHAR", source_name="product/toRegionCode"),
    Column("product_training", "VARCHAR", source_name="product/training"),
    Column("product_transfer_type", "VARCHAR", source_name="product/transferType"),
    Column("product_type", "VARCHAR", source_name="product/type"),
    Column("product_usagetype", "VARCHAR", source_name="product/usagetype"),
    Column("product_vcpu", "VARCHAR", source_name="product/vcpu"),
    Column("product_version", "VARCHAR", source_name="product/version"),
    Column(
        "product_virtual_interface_type",
        "VARCHAR",
        source_name="product/virtualInterfaceType",
    ),
    Column("product_volume_api_name", "VARCHAR", source_name="product/volumeApiName"),
    Column("product_volume_type", "VARCHAR", source_name="product/volumeType"),
    Column(
        "product_vpcnetworkingsupport",
        "VARCHAR",
        source_name="product/vpcnetworkingsupport",
    ),
    Column(
        "product_who_can_open_cases", "VARCHAR", source_name="product/whoCanOpenCases"
    ),
    Column(
        "pricing_lease_contract_length",
        "VARCHAR",
        source_name="pricing/LeaseContractLength",
    ),
    Column("pricing_offering_class", "VARCHAR", source_name="pricing/OfferingClass"),
    Column("pricing_purchase_option", "VARCHAR", source_name="pricing/PurchaseOption"),
    Column("pricing_rate_code", "VARCHAR", source_name="pricing/RateCode"),
    Column("pricing_rate_id", "VARCHAR", source_name="pricing/RateId"),
    Column("pricing_currency", "VARCHAR", source_name="pricing/currency"),
    Column(
        "pricing_public_on_demand_cost",
        "NUMBER(38,10)",
        source_name="pricing/publicOnDemandCost",
    ),
    Column(
        "pricing_public_on_demand_rate",
        "NUMBER(38,10)",
        source_name="pricing/publicOnDemandRate",
    ),
    Column("pricing_term", "VARCHAR", source_name="pricing/term"),
    Column("pricing_unit", "VARCHAR", source_name="pricing/unit"),
    Column(
        "reservation_amortized_upfront_cost_for_usage",
        "NUMBER(38,10)",
        source_name="reservation/AmortizedUpfrontCostForUsage",
    ),
    Column(
        "reservation_amortized_upfront_fee_for_billing_period",
        "NUMBER(38,10)",
        source_name="reservation/AmortizedUpfrontFeeForBillingPeriod",
    ),
    Column(
        "reservation_effective_cost", "VARCHAR", source_name="reservation/EffectiveCost"
    ),
    Column("reservation_end_time", "VARCHAR", source_name="reservation/EndTime"),
    Column(
        "reservation_modification_status",
        "VARCHAR",
        source_name="reservation/ModificationStatus",
    ),
    Column(
        "reservation_normalized_units_per_reservation",
        "VARCHAR",
        source_name="reservation/NormalizedUnitsPerReservation",
    ),
    Column(
        "reservation_number_of_reservations",
        "VARCHAR",
        source_name="reservation/NumberOfReservations",
    ),
    Column(
        "reservation_recurring_fee_for_usage",
        "VARCHAR",
        source_name="reservation/RecurringFeeForUsage",
    ),
    Column("reservation_start_time", "VARCHAR", source_name="reservation/StartTime"),
    Column(
        "reservation_subscription_id",
        "VARCHAR",
        source_name="reservation/SubscriptionId",
    ),
    Column(
        "reservation_total_reserved_normalized_units",
        "VARCHAR",
        source_name="reservation/TotalReservedNormalizedUnits",
    ),
    Column(
        "reservation_total_reserved_units",
        "VARCHAR",
        source_name="reservation/TotalReservedUnits",
    ),
    Column(
        "reservation_units_per_reservation",
        "VARCHAR",
        source_name="reservation/UnitsPerReservation",
    ),
    Column(
        "reservation_unused_amortized_upfront_fee_for_billing_period",
        "NUMBER",
        source_name="reservation/UnusedAmortizedUpfrontFeeForBillingPeriod",
    ),
    Column(
        "reservation_unused_normalized_unit_quantity",
        "VARCHAR",
        source_name="reservation/UnusedNormalizedUnitQuantity",
    ),
    Column(
        "reservation_unused_quantity",
        "VARCHAR",
        source_name="reservation/UnusedQuantity",
    ),
    Column(
        "reservation_unused_recurring_fee",
        "VARCHAR",
        source_name="reservation/UnusedRecurringFee",
    ),
    Column(
        "reservation_upfront_value", "VARCHAR", source_name="reservation/UpfrontValue"
    ),
    Column(
        "savings_plan_total_commitment_to_date",
        "VARCHAR",
        source_name="savingsPlan/TotalCommitmentToDate",
    ),
    Column(
        "savings_plan_savings_plan_arn",
        "VARCHAR",
        source_name="savingsPlan/SavingsPlanARN",
    ),
    Column(
        "savings_plan_savings_plan_rate",
        "VARCHAR",
        source_name="savingsPlan/SavingsPlanRate",
    ),
    Column(
        "savings_plan_used_commitment",
        "VARCHAR",
        source_name="savingsPlan/UsedCommitment",
    ),
    Column(
        "savings_plan_savings_plan_effective_cost",
        "VARCHAR",
        source_name="savingsPlan/SavingsPlanEffectiveCost",
    ),
    Column(
        "savings_plan_amortized_upfront_commitment_for_billing_period",
        "VARCHAR",
        source_name="savingsPlan/AmortizedUpfrontCommitmentForBillingPeriod",
    ),
    Column(
        "savings_plan_recurring_commitment_for_billing_period",
        "VARCHAR",
        source_name="savingsPlan/RecurringCommitmentForBillingPeriod",
    ),
    Column("savings_plan_start_time", "VARCHAR", source_name="savingsPlan/StartTime"),
    Column("savings_plan_end_time", "VARCHAR", source_name="savingsPlan/EndTime"),
    Column(
        "savings_plan_offering_type", "VARCHAR", source_name="savingsPlan/OfferingType"
    ),
    Column(
        "savings_plan_payment_option",
        "VARCHAR",
        source_name="savingsPlan/PaymentOption",
    ),
    Column(
        "savings_plan_purchase_term", "VARCHAR", source_name="savingsPlan/PurchaseTerm"
    ),
    Column("savings_plan_region", "VARCHAR", source_name="savingsPlan/Region"),
    Column(
        "resource_tags_aws_autoscaling_group_name",
        "VARCHAR",
        source_name="resourceTags/aws:autoscaling:groupName",
    ),
    Column(
        "resource_tags_aws_cloudformation_logical_id",
        "VARCHAR",
        source_name="resourceTags/aws:cloudformation:logical-id",
    ),
    Column(
        "resource_tags_aws_cloudformation_stack_id",
        "VARCHAR",
        source_name="resourceTags/aws:cloudformation:stack-id",
    ),
    Column(
        "resource_tags_aws_cloudformation_stack_name",
        "VARCHAR",
        source_name="resourceTags/aws:cloudformation:stack-name",
    ),
    Column(
        "resource_tags_aws_created_by",
        "VARCHAR",
        source_name="resourceTags/aws:createdBy",
    ),
    Column(
        "resource_tags_aws_ec2_fleet_id",
        "VARCHAR",
        source_name="resourceTags/aws:ec2:fleet-id",
    ),
    Column(
        "resource_tags_aws_ec2launchtemplate_id",
        "VARCHAR",
        source_name="resourceTags/aws:ec2launchtemplate:id",
    ),
    Column(
        "resource_tags_aws_ec2launchtemplate_version",
        "VARCHAR",
        source_name="resourceTags/aws:ec2launchtemplate:version",
    ),
    Column(
        "resource_tags_user_account_id",
        "VARCHAR",
        source_name="resourceTags/user:AccountId",
    ),
    Column(
        '"resource_tags_user_Application"',
        "VARCHAR",
        source_name="resourceTags/user:Application",
    ),
    Column(
        '"resource_tags_user_Application Name"',
        "VARCHAR",
        source_name="resourceTags/user:Application Name",
    ),
    Column(
        "resource_tags_user_billedto",
        "VARCHAR",
        source_name="resourceTags/user:BILLEDTO",
    ),
    Column(
        "resource_tags_user_cluster_id",
        "VARCHAR",
        source_name="resourceTags/user:ClusterId",
    ),
    Column(
        "resource_tags_user_cluster_name",
        "VARCHAR",
        source_name="resourceTags/user:ClusterName",
    ),
    Column(
        '"resource_tags_user_Company-Department"',
        "VARCHAR",
        source_name="resourceTags/user:Company-Department",
    ),
    Column("resource_tags_user_dept", "VARCHAR", source_name="resourceTags/user:Dept"),
    Column(
        "resource_tags_user_description",
        "VARCHAR",
        source_name="resourceTags/user:Description",
    ),
    Column(
        "resource_tags_user_git_2_ebdemo",
        "VARCHAR",
        source_name="resourceTags/user:Git2EBDemo",
    ),
    Column("resource_tags_user_name", "VARCHAR", source_name="resourceTags/user:Name"),
    Column(
        '"resource_tags_user_Owner"', "VARCHAR", source_name="resourceTags/user:Owner"
    ),
    Column(
        '"resource_tags_user_Project"',
        "VARCHAR",
        source_name="resourceTags/user:Project",
    ),
    Column(
        "resource_tags_user_stage", "VARCHAR", source_name="resourceTags/user:STAGE"
    ),
    Column(
        "resource_tags_user_ticket", "VARCHAR", source_name="resourceTags/user:Ticket"
    ),
    Column(
        '"resource_tags_user_account-id"',
        "VARCHAR",
        source_name="resourceTags/user:account-id",
    ),
    Column("resource_tags_user_api", "VARCHAR", source_name="resourceTags/user:api"),
    Column(
        '"resource_tags_user_api gateway"',
        "VARCHAR",
        source_name="resourceTags/user:api gateway",
    ),
    Column(
        "resource_tags_user_api_gateway",
        "VARCHAR",
        source_name="resourceTags/user:api_gateway",
    ),
    Column("resource_tags_user_app", "VARCHAR", source_name="resourceTags/user:app"),
    Column(
        "resource_tags_user_app_name",
        "VARCHAR",
        source_name="resourceTags/user:app-name",
    ),
    Column(
        "resource_tags_user_application",
        "VARCHAR",
        source_name="resourceTags/user:application",
    ),
    Column(
        "resource_tags_user_application_alias",
        "VARCHAR",
        source_name="resourceTags/user:application-alias",
    ),
    Column(
        "resource_tags_user_application_name",
        "VARCHAR",
        source_name="resourceTags/user:application-name",
    ),
    Column(
        "resource_tags_user_company_department",
        "VARCHAR",
        source_name="resourceTags/user:company-department",
    ),
    Column(
        "resource_tags_user_department",
        "VARCHAR",
        source_name="resourceTags/user:department",
    ),
    Column(
        "resource_tags_user_elasticbeanstalk_environment_id",
        "VARCHAR",
        source_name="resourceTags/user:elasticbeanstalk:environment-id",
    ),
    Column(
        "resource_tags_user_elasticbeanstalk_environment_name",
        "VARCHAR",
        source_name="resourceTags/user:elasticbeanstalk:environment-name",
    ),
    Column(
        "resource_tags_user_environment",
        "VARCHAR",
        source_name="resourceTags/user:environment",
    ),
    Column(
        "resource_tags_user_lambda_created_by",
        "VARCHAR",
        source_name="resourceTags/user:lambda:createdBy",
    ),
    Column(
        "resource_tags_user_owner", "VARCHAR", source_name="resourceTags/user:owner"
    ),
    Column(
        "resource_tags_user_project", "VARCHAR", source_name="resourceTags/user:project"
    ),
    Column("resource_tags_user_team", "VARCHAR", source_name="resourceTags/user:team"),
    Column(
        "resource_tags_user_username",
        "VARCHAR",
        source_name="resourceTags/user:username",
    ),
    Column("updated_at", "TIMESTAMP_LTZ(3)", delta_column=True),
]
