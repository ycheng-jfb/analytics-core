USE LAKE;
alter table if exists LAKE.EPLMV14PROD.LINE_PLAN_STYLES modify column CREATED_BY
set masking policy default_mask;
alter table if exists LAKE.EVOLVE01_SSRS_REPORTS.PO_AUDIT_TRAIL modify column USER_NAME
set masking policy default_mask;
alter table if exists LAKE.EXCEL.DHL_INVOICE modify column BILLING_ACCOUNT_ADDRESS_LINE_1
set masking policy default_mask;
alter table if exists LAKE.EXCEL.DHL_INVOICE modify column BILLING_ACCOUNT_ADDRESS_LINE_2
set masking policy default_mask;
alter table if exists LAKE.EXCEL.DHL_INVOICE modify column BILLING_ACCOUNT_ADDRESS_CITY_NAME
set masking policy default_mask;
alter table if exists LAKE.EXCEL.DHL_INVOICE modify column BILLING_ACCOUNT_ADDRESS_STATE_CODE
set masking policy default_mask;
alter table if exists LAKE.EXCEL.DHL_INVOICE modify column BILLING_ACCOUNT_POSTAL_CODE
set masking policy default_mask;
alter table if exists LAKE.EXCEL.DHL_INVOICE modify column SHIPPER_ACCOUNT_NAME
set masking policy default_mask;
alter table if exists LAKE.EXCEL.DHL_INVOICE modify column CONSIGNOR_NAME
set masking policy default_mask;
alter table if exists LAKE.EXCEL.DHL_INVOICE modify column CONSIGNOR_CONTACT_NAME
set masking policy default_mask;
alter table if exists LAKE.EXCEL.DHL_INVOICE modify column CONSIGNOR_ADDRESS_LINE_1
set masking policy default_mask;
alter table if exists LAKE.EXCEL.DHL_INVOICE modify column CONSIGNOR_ADDRESS_LINE_2
set masking policy default_mask;
alter table if exists LAKE.EXCEL.DHL_INVOICE modify column CONSIGNOR_ADDRESS_LINE_3
set masking policy default_mask;
alter table if exists LAKE.EXCEL.DHL_INVOICE modify column CONSIGNOR_CITY_NAME
set masking policy default_mask;
alter table if exists LAKE.EXCEL.DHL_INVOICE modify column CONSIGNOR_STATE_CODE
set masking policy default_mask;
alter table if exists LAKE.EXCEL.DHL_INVOICE modify column CONSIGNOR_ZIP_CODE
set masking policy default_mask;
alter table if exists LAKE.EXCEL.DHL_INVOICE modify column CONSIGNOR_COUNTRY_CODE
set masking policy default_mask;
alter table if exists LAKE.EXCEL.DHL_INVOICE modify column CONSIGNOR_COUNTRY_NAME
set masking policy default_mask;
alter table if exists LAKE.EXCEL.DHL_INVOICE modify column CONSIGNOR_TELEPHONE_NUMBER
set masking policy default_mask;
alter table if exists LAKE.EXCEL.DHL_INVOICE modify column CONSIGNEE_NAME
set masking policy default_mask;
alter table if exists LAKE.EXCEL.DHL_INVOICE modify column CONSIGNEE_CONTACT_NAME
set masking policy default_mask;
alter table if exists LAKE.EXCEL.DHL_INVOICE modify column CONSIGNEE_ADDRESS_LINE_1
set masking policy default_mask;
alter table if exists LAKE.EXCEL.DHL_INVOICE modify column CONSIGNEE_ADDRESS_LINE_2
set masking policy default_mask;
alter table if exists LAKE.EXCEL.DHL_INVOICE modify column CONSIGNEE_ADDRESS_LINE_3
set masking policy default_mask;
alter table if exists LAKE.EXCEL.DHL_INVOICE modify column CONSIGNEE_CITY_NAME
set masking policy default_mask;
alter table if exists LAKE.EXCEL.DHL_INVOICE modify column CONSIGNEE_STATE_CODE
set masking policy default_mask;
alter table if exists LAKE.EXCEL.DHL_INVOICE modify column CONSIGNEE_ZIP_CODE
set masking policy default_mask;
alter table if exists LAKE.EXCEL.DHL_INVOICE modify column CONSIGNEE_COUNTRY_CODE
set masking policy default_mask;
alter table if exists LAKE.EXCEL.DHL_INVOICE modify column CONSIGNEE_COUNTRY_NAME
set masking policy default_mask;
alter table if exists LAKE.EXCEL.DHL_INVOICE modify column CONSIGNEE_TELEPHONE_NUMBER
set masking policy default_mask;
alter table if exists LAKE.EXCEL.GPS_SCORECARD modify column FASHION_CONSULTANT
set masking policy default_mask;
alter table if exists LAKE.EXCEL.GSC_FACTORY_AUDIT modify column VENDOR
set masking policy default_mask;
alter table if exists LAKE.EXCEL.GSC_FACTORY_AUDIT modify column COUNTRY
set masking policy default_mask;
alter table if exists LAKE.EXCEL.GSC_FACTORY_AUDIT modify column OFFICIAL_FACTORY_NAME
set masking policy default_mask;
alter table if exists LAKE.EXCEL.GSC_FACTORY_AUDIT modify column ADDRESS
set masking policy default_mask;
alter table if exists LAKE.EXCEL.GSC_FACTORY_AUDIT modify column PHONE_NUMBER
set masking policy default_mask;
alter table if exists LAKE.EXCEL.GSC_FACTORY_AUDIT modify column AUDITOR
set masking policy default_mask;
alter table if exists LAKE.EXCEL.GSC_FACTORY_AUDIT_3RD_PARTY modify column VENDOR
set masking policy default_mask;
alter table if exists LAKE.EXCEL.GSC_FACTORY_AUDIT_3RD_PARTY modify column COUNTRY
set masking policy default_mask;
alter table if exists LAKE.EXCEL.GSC_FACTORY_AUDIT_3RD_PARTY modify column FACTORY_NAME
set masking policy default_mask;
alter table if exists LAKE.EXCEL.GSC_FACTORY_AUDIT_3RD_PARTY modify column ADDRESS
set masking policy default_mask;
alter table if exists LAKE.EXCEL.GSC_FACTORY_AUDIT_3RD_PARTY modify column PHONE_NUMBER
set masking policy default_mask;
alter table if exists LAKE.EXCEL.GSC_FACTORY_AUDIT_3RD_PARTY modify column AUDITOR
set masking policy default_mask;
alter table if exists LAKE.EXCEL.GSC_FACTORY_AUDIT_COMBINED modify column VENDOR
set masking policy default_mask;
alter table if exists LAKE.EXCEL.GSC_FACTORY_AUDIT_COMBINED modify column COUNTRY
set masking policy default_mask;
alter table if exists LAKE.EXCEL.GSC_FACTORY_AUDIT_COMBINED modify column OFFICIAL_FACTORY_NAME
set masking policy default_mask;
alter table if exists LAKE.EXCEL.GSC_FACTORY_AUDIT_COMBINED modify column CONTACT_INFO
set masking policy default_mask;
alter table if exists LAKE.EXCEL.GSC_FACTORY_AUDIT_COMBINED modify column AUDITOR
set masking policy default_mask;
alter table if exists LAKE.GDPR.REQUEST modify column EMAIL
set masking policy default_mask;
alter table if exists LAKE.GDPR.REQUEST modify column FIRSTNAME
set masking policy default_mask;
alter table if exists LAKE.GDPR.REQUEST modify column LASTNAME
set masking policy default_mask;
alter table if exists LAKE.GENESYS.CONVERSATIONS modify column PARTICIPANTS
set masking policy variant_mask;
alter table if exists LAKE.GENESYS.GENESYS_USER modify column USER_NAME
set masking policy default_mask;
alter table if exists LAKE.GENESYS.GENESYS_USER modify column USER_LOGIN
set masking policy default_mask;
alter table if exists LAKE.GMS.ZENDESK_USERS modify column NAME
set masking policy default_mask;
alter table if exists LAKE.GMS.ZENDESK_USERS modify column EMAIL
set masking policy default_mask;
alter table if exists LAKE.JFGC.GIFT_CARD modify column EMAIL
set masking policy default_mask;
alter table if exists LAKE.JFGC.GIFT_CARD_TRANSACTION modify column EMAIL
set masking policy default_mask;
alter table if exists LAKE.JF_PORTAL.CHANGE_LOG_HDR2 modify column USER_NAME
set masking policy default_mask;
alter table if exists LAKE.JF_PORTAL.PO_DTL modify column USER_CREATE
set masking policy default_mask;
alter table if exists LAKE.JF_PORTAL.PO_DTL modify column USER_UPDATE
set masking policy default_mask;
alter table if exists LAKE.JF_PORTAL.PO_HDR modify column USER_CREATE
set masking policy default_mask;
alter table if exists LAKE.JF_PORTAL.PO_HDR modify column USER_UPDATE
set masking policy default_mask;
alter table if exists LAKE.JF_PORTAL.PO_HTS modify column USER_CREATE
set masking policy default_mask;
alter table if exists LAKE.JF_PORTAL.PO_HTS modify column USER_UPDATE
set masking policy default_mask;
alter table if exists LAKE.JF_PORTAL.PO_MILESTONE modify column USER_CREATE
set masking policy default_mask;
alter table if exists LAKE.JF_PORTAL.PO_MILESTONE modify column USER_UPDATE
set masking policy default_mask;
alter table if exists LAKE.JF_PORTAL.PO_MISC_COST modify column USER_CREATE
set masking policy default_mask;
alter table if exists LAKE.JF_PORTAL.PO_MISC_COST modify column USER_UPDATE
set masking policy default_mask;
alter table if exists LAKE.JF_PORTAL.STYLE_TYPE modify column USER_CREATE
set masking policy default_mask;
alter table if exists LAKE.JF_PORTAL.STYLE_TYPE modify column USER_UPDATE
set masking policy default_mask;
alter table if exists LAKE.LITHIUM.LITHIUM_RESPONSE modify column RESPONDINGAGENTNAME
set masking policy default_mask;
alter table if exists LAKE.LITHIUM.LITHIUM_RESPONSE modify column RESPONDINGAGENTEMAIL
set masking policy default_mask;
alter table if exists LAKE.MERLIN.FACTORY modify column FIRST_NAME
set masking policy default_mask;
alter table if exists LAKE.MERLIN.FACTORY modify column LAST_NAME
set masking policy default_mask;
alter table if exists LAKE.MERLIN.FACTORY modify column PHONE_NUMBER
set masking policy default_mask;
alter table if exists LAKE.MERLIN.FACTORY modify column FACTORY_EMAIL_ADDRESS
set masking policy default_mask;
alter table if exists LAKE.MERLIN.FACTORY modify column CONTACT_EMAIL_ADDRESS
set masking policy default_mask;
alter table if exists LAKE.NGC_PLM_PRODCOMP01.CATEGORY_FIELD modify column CREATED_BY
set masking policy default_mask;
alter table if exists LAKE.NGC_PLM_PRODCOMP01.CATEGORY_FIELD modify column MODIFIED_BY
set masking policy default_mask;
alter table if exists LAKE.NGC_PLM_PRODCOMP01.CATEGORY_FIELD_DETAIL modify column CREATED_BY
set masking policy default_mask;
alter table if exists LAKE.NGC_PLM_PRODCOMP01.CATEGORY_FIELD_DETAIL modify column MODIFIED_BY
set masking policy default_mask;
alter table if exists LAKE.NGC_PLM_PRODCOMP01.CATEGORY_FIELD_ENTITY modify column CREATED_BY
set masking policy default_mask;
alter table if exists LAKE.NGC_PLM_PRODCOMP01.CATEGORY_FIELD_ENTITY modify column MODIFIED_BY
set masking policy default_mask;
alter table if exists LAKE.NGC_PLM_PRODCOMP01.DEVELOPMENT_STYLE modify column CREATED_BY
set masking policy default_mask;
alter table if exists LAKE.NGC_PLM_PRODCOMP01.DEVELOPMENT_STYLE modify column MODIFIED_BY
set masking policy default_mask;
alter table if exists LAKE.NGC_PLM_PRODCOMP01.METADATA_TABLE modify column CREATED_BY
set masking policy default_mask;
alter table if exists LAKE.NGC_PLM_PRODCOMP01.METADATA_TABLE modify column MODIFIED_BY
set masking policy default_mask;
alter table if exists LAKE.PIVOT88.INSPECTIONS modify column CONTACT_NAME
set masking policy default_mask;
alter table if exists LAKE.PIVOT88.INSPECTIONS modify column CONTACT_NUMBER
set masking policy default_mask;
alter table if exists LAKE.SAILTHRU.DATA_EXPORTER_PROFILE modify column EMAIL
set masking policy default_mask;
alter table if exists LAKE.SAILTHRU.DATA_EXPORTER_PROFILE modify column VARS
set masking policy variant_mask;
alter table if exists LAKE.SAILTHRU.DATA_EXPORTER_PROFILE modify column GEO
set masking policy variant_mask;
alter table if exists LAKE.SAILTHRU.DATA_EXPORTER_PROFILE modify column BROWSER
set masking policy variant_mask;
alter table if exists LAKE.SPRINKLR.AGENT_CASE_ASSIGNMENT modify column AFFECTED_USER_ID
set masking policy default_mask;
alter table if exists LAKE.SPRINKLR.AGENT_RESPONSE_SLA modify column BRAND_RESPONSE_BY_USER
set masking policy default_mask;
alter table if exists LAKE.SPRINKLR.AGENT_RESPONSE_VOLUME modify column USER_ID
set masking policy default_mask;
alter table if exists LAKE.SPRINKLR.AGENT_SCORECARD_PROCESS modify column USER_ID
set masking policy default_mask;
alter table if exists LAKE.SPRINKLR.AGENT_SCORECARD_RESPONSE modify column USER_ID
set masking policy default_mask;
alter table if exists LAKE.SPRINKLR.CASES modify column SOCIAL_NETWORK
set masking policy default_mask;
alter table if exists LAKE.SPRINKLR.CASES modify column CASE_SN_PROFILE_ID
set masking policy default_mask;
alter table if exists LAKE.SPRINKLR.LISTENING_STREAMS modify column SENDER_PROFILE
set masking policy variant_mask;
alter table if exists LAKE.SPRINKLR.LISTENING_STREAMS modify column RECEIVER_PROFILE
set masking policy variant_mask;
alter table if exists LAKE.SPS.CARRIER_INVOICE modify column BILLING
set masking policy variant_mask;
alter table if exists LAKE.SPS.CARRIER_INVOICE modify column PACKAGE
set masking policy variant_mask;
alter table if exists LAKE.SPS.CARRIER_INVOICE_BKP_20200722 modify column BILLING
set masking policy variant_mask;
alter table if exists LAKE.SPS.CARRIER_INVOICE_BKP_20200722 modify column PACKAGE
set masking policy variant_mask;
alter table if exists LAKE.SPS.CARRIER_MILESTONE modify column ADDRESS
set masking policy variant_mask;
alter table if exists LAKE.SPS.CARRIER_MILESTONE modify column SHIPMENT
set masking policy variant_mask;
alter table if exists LAKE.TABLEAU.USERS_BY_GROUP modify column USER_NAME
set masking policy default_mask;
alter table if exists LAKE.TABLEAU.WRITEBACK_SYSTEM_USERS modify column DISPLAY_NAME
set masking policy default_mask;
alter table if exists LAKE.TABLEAU.WRITEBACK_SYSTEM_USERS modify column USERNAME
set masking policy default_mask;
alter table if exists LAKE.ULTRA_CMS.INFLUENCER modify column FIRSTNAME
set masking policy default_mask;
alter table if exists LAKE.ULTRA_CMS.INFLUENCER modify column LASTNAME
set masking policy default_mask;
alter table if exists LAKE.ULTRA_CMS.INFLUENCER modify column EMAIL
set masking policy default_mask;
alter table if exists LAKE.ULTRA_IDENTITY.ADMINISTRATOR modify column LOGIN
set masking policy default_mask;
alter table if exists LAKE.ULTRA_IDENTITY.ADMINISTRATOR modify column PASSWORD
set masking policy default_mask;
alter table if exists LAKE.ULTRA_IDENTITY.ADMINISTRATOR modify column FIRSTNAME
set masking policy default_mask;
alter table if exists LAKE.ULTRA_IDENTITY.ADMINISTRATOR modify column LASTNAME
set masking policy default_mask;
alter table if exists LAKE.ULTRA_IDENTITY.USER modify column FIRSTNAME
set masking policy default_mask;
alter table if exists LAKE.ULTRA_IDENTITY.USER modify column LASTNAME
set masking policy default_mask;
alter table if exists LAKE.ULTRA_IDENTITY.USER modify column EMAIL
set masking policy default_mask;
alter table if exists LAKE.ULTRA_IDENTITY.USER modify column USERNAME
set masking policy default_mask;
alter table if exists LAKE.ULTRA_IDENTITY.USER modify column PASSWORD
set masking policy default_mask;
alter table if exists LAKE.WORKDAY.EMPLOYEES modify column FIRST_NAME
set masking policy default_mask;
alter table if exists LAKE.WORKDAY.EMPLOYEES modify column LAST_NAME
set masking policy default_mask;
alter table if exists LAKE.WORKDAY.EMPLOYEES modify column MANAGER
set masking policy default_mask;
alter table if exists LAKE.WORKDAY.EMPLOYEES modify column EMAIL
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.address modify column address1
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.address modify column address2
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.address modify column company
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.address modify column email
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.address modify column firstname
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.address modify column lastname
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.address modify column name
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.address modify column phone
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.address modify column phone_digits
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.address modify column us_phone_areacode
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.address modify column us_phone_linenumber
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.address modify column us_phone_prefix
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.address modify column zip
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.address_validation_request_log modify column address1
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.address_validation_request_log modify column address2
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.address_validation_request_log modify column zip
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.address_validation_response_log modify column address1
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.address_validation_response_log modify column address2
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.address_validation_response_log modify column zip
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.box_invitation modify column recipient_email
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.box_invitation modify column recipient_firstname
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.box_invitation modify column recipient_lastname
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.cart_gift_certificate modify column recipient_address1
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.cart_gift_certificate modify column recipient_address2
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.cart_gift_certificate modify column recipient_email
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.cart_gift_certificate modify column recipient_firstname
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.cart_gift_certificate modify column recipient_lastname
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.cart_gift_certificate modify column recipient_zip
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.case_call_recording modify column called_phone_number
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.case_call_recording modify column caller_phone_number
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.credit_transfer_transaction modify column email
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.credit_transfer_transaction_log modify column email
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.creditcard modify column name_on_card
set masking policy default_mask;
alter table if exists LAKE.ULTRA_WAREHOUSE.address modify column address1
set masking policy default_mask;
alter table if exists LAKE.ULTRA_WAREHOUSE.address modify column address2
set masking policy default_mask;
alter table if exists LAKE.ULTRA_WAREHOUSE.address modify column address3
set masking policy default_mask;
alter table if exists LAKE.ULTRA_WAREHOUSE.address modify column address4
set masking policy default_mask;
alter table if exists LAKE.ULTRA_WAREHOUSE.address modify column address5
set masking policy default_mask;
alter table if exists LAKE.ULTRA_WAREHOUSE.address modify column address6
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.customer_call modify column phone
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.customer_carrier_retail_point modify column package_reference_email
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.customer_carrier_retail_point modify column package_reference_phone
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.customer_detail modify column value
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.customer_email_change_history modify column new_email
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.customer_email_change_history modify column old_email
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.customer_phone modify column phone
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.customer_quiz_data modify column age
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.customer_quiz_data modify column age_group_id
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.customer_referral modify column recipient_email
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.customer_referral modify column recipient_firstname
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.customer_referral modify column recipient_lastname
set masking policy default_mask;
alter table if exists LAKE.ULTRA_WAREHOUSE.address modify column address7
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.ddc_subscription_message modify column email
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.ddc_suppression_list_email modify column email
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.email_template_queue modify column email
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.email_template_queue_error modify column email
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.gift_certificate modify column recipient_email
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.gift_certificate modify column recipient_firstname
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.gift_certificate modify column recipient_lastname
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.lead modify column email
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.membership_invitation modify column recipient_email
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.membership_invitation modify column recipient_firstname
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.membership_invitation modify column recipient_lastname
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.membership_profile modify column age
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.membership_profile modify column name
set masking policy default_mask;
alter table if exists LAKE.ULTRA_WAREHOUSE.address modify column address8
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.partner_order modify column email
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.partner_order modify column firstname
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.partner_order modify column lastname
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.payment_transaction_creditcard modify column ip
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.payment_transaction_psp modify column ip
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.psp modify column acct_name
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.psp_chargeback_log modify column customer_email
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.session modify column ip
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.sms_log modify column phone
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.sms_solicitation_permission modify column phone
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.sms_template_queue modify column phone
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.testimonial modify column address1
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.testimonial modify column address2
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.testimonial modify column age
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.testimonial modify column email
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.testimonial modify column firstname
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.testimonial modify column lastname
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.testimonial modify column phone
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.testimonial modify column zip
set masking policy default_mask;
alter table if exists LAKE.ULTRA_WAREHOUSE.address modify column company
set masking policy default_mask;
alter table if exists LAKE.ULTRA_WAREHOUSE.address modify column firstname
set masking policy default_mask;
alter table if exists LAKE.ULTRA_WAREHOUSE.address modify column lastname
set masking policy default_mask;
alter table if exists LAKE.ULTRA_WAREHOUSE.address modify column misc1
set masking policy default_mask;
alter table if exists LAKE.ULTRA_WAREHOUSE.address modify column misc2
set masking policy default_mask;
alter table if exists LAKE.ULTRA_WAREHOUSE.address modify column misc3
set masking policy default_mask;
alter table if exists LAKE.ULTRA_WAREHOUSE.address modify column phone
set masking policy default_mask;
alter table if exists LAKE.ULTRA_WAREHOUSE.address modify column zip
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.customer modify column company
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.customer modify column email
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.customer modify column firstname
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.customer modify column lastname
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.customer modify column name
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.customer modify column username
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.customer_solicitation_permission modify column ip
set masking policy default_mask;
alter table if exists LAKE.ULTRA_MERCHANT.order modify column ip
set masking policy default_mask;
alter table if exists LAKE.ULTRA_WAREHOUSE.Invoice modify column email
set masking policy default_mask;
