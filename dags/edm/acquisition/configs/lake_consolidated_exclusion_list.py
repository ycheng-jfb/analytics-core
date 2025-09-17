lake_consolidated_exclusion_table_list = {
    "lake_consolidated.ultra_merchant.featured_product_delete_log",
    "lake_consolidated.ultra_merchant.order_credit_delete_log",
    "lake_consolidated.ultra_merchant.order_delete_log",
    "lake_consolidated.ultra_merchant.product_bundle_component_delete_log",
    "lake_consolidated.ultra_merchant.product_product_category_delete_log",
    "lake_consolidated.ultra_merchant.product_tag_delete_log",
    "lake_consolidated.ultra_merchant.variant_pricing_delete_log",
    # INFO: tables are empty for the below tables
    "lake_consolidated.ultra_merchant.gift_certificate_transaction_reason",
    "lake_consolidated.ultra_merchant.quiz_question_category",
    # INFO: cdr_incontact is decomissioned. No active data since 2020
    "lake_consolidated.ultra_merchant.cdr_incontact",
    # INFO: media_code is handled by a dedicated SQL Script as the company_join_sql is complex
    "lake_consolidated.ultra_merchant.media_code",
    # INFO: item_preallocation will be implemented as a truncate load using a dedicated sql script
    "lake_consolidated.ultra_merchant.item_preallocation",
    # INFO: the tables below are excluded from ultra_merchant and will be loaded from ultra_identity
    "lake_consolidated.ultra_merchant.administrator_session",
    "lake_consolidated.ultra_merchant.administrator_session_log",
    "lake_consolidated.ultra_merchant.administrator_store_group",
    "lake_consolidated.ultra_merchant.administrator_survey",
    "lake_consolidated.ultra_merchant.administrator_survey_question_answer",
    "lake_consolidated.ultra_merchant.administrator_warehouse",
    # INFO: these tables are excluded in existing exclusion_list.py
    "lake_consolidated.ultra_merchant.product_instance_product_option_value",
    "lake_consolidated.ultra_merchant.product_option",
    "lake_consolidated.ultra_merchant.product_option_value",
    # INFO: gdpr.request_job is handled by edm_outbound_gdpr_update as it needs custom sql
    "lake_consolidated.gdpr.request_job",
    # INFO: There is no data after 2019-08-13 and is causing slowness downstream.
    "lake_consolidated.ultra_merchant.session_media_data",
}
