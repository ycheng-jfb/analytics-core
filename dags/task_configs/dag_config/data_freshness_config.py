from dataclasses import dataclass


@dataclass
class HvrConfig:
    lake_db: str
    table_group: str
    minute_window: int
    email_body: str = None

    def get_email_subject(self):
        if self.table_group != '':
            subject = f"Alert: Data is stale for {self.lake_db}_{self.table_group}"
        else:
            subject = f"Alert: Data is stale for {self.lake_db}_all"
        return subject

    def get_task_id(self):
        if self.table_group != '':
            task_id = f"alert_for_hvr_{self.lake_db}_{self.table_group}_{self.minute_window}"
        else:
            task_id = f"alert_for_hvr_{self.lake_db}_all"
        return task_id.replace(' ', '_').lower()

    def get_sql_query(self):
        sql_query = (
            f"SELECT * \n"
            f"FROM LAKE_CONSOLIDATED.REFERENCE.HVR_LAKE_STATUS \n"
            f"WHERE DATABASE = '{self.lake_db}' \n"
            f"AND (UPDATED_TO < DATEADD(MINUTE, -{self.minute_window}, CURRENT_TIMESTAMP()) \n"
            f"OR ( \n"
            f"    UPDATED_TO = '1969-12-31 16:00:00 -0800' \n"
            f"    AND COMPLETED_AT < DATEADD(MINUTE, -{self.minute_window}, CURRENT_TIMESTAMP()) \n"
            f"    ) \n"
            f") \n"
        )
        if self.table_group != '':
            sql_query += f"AND TABLE_GROUP = '{self.table_group}'"
        sql_query += ';'
        return sql_query

    def get_email_body(self):
        if self.email_body:
            return self.email_body
        else:
            return (
                f"The data for HVR {self.lake_db}'s {self.table_group} Table Group is outdated "
                f"and stale for more than {self.minute_window} minutes. \n"
                "Investigate immediately!"
            )


@dataclass
class NonHvrConfig:
    table_name: str
    column_name: str
    minute_window: int
    email_subject: str = "Alert: Data is stale"
    email_body: str = None

    def get_email_subject(self):
        return f"Alert: Data is stale for {self.table_name}"

    def get_task_id(self):
        return (
            'alert_for_'
            + self.table_name.replace('"', '').replace('.', '_').lower()
            + f"_{self.minute_window}"
        )

    def get_sql_query(self):
        return (
            f"SELECT MAX({self.column_name}) AS last_updated_at,'{self.table_name}' as table_name  \n"
            f"FROM {self.table_name} \n"
            f"WHERE {self.column_name} >= DATEADD(DAY, -14, CURRENT_TIMESTAMP()) "
            f"HAVING MAX({self.column_name}) < DATEADD(MINUTE, -{self.minute_window}, CURRENT_TIMESTAMP())\n"
        )

    def get_email_body(self):
        if self.email_body:
            return self.email_body
        else:
            return (
                f"The data for {self.table_name} is outdated and stale for more than {self.minute_window} minutes. \n"
                "Investigate immediately!"
            )


hvr_config_list = [
    HvrConfig('FL', 'HIGH FREQ', 45),
    HvrConfig('JFB', 'HIGH FREQ', 45),
    HvrConfig('SXF', 'HIGH FREQ', 45),
    HvrConfig('FL', '', 120),
    HvrConfig('JFB', '', 120),
    HvrConfig('SXF', '', 120),
    HvrConfig('EV', '', 120),
]

non_hvr_config_list = [
    # Lake Layer
    # Fivetran Critical Tables
    NonHvrConfig(
        'lake_fivetran.med_facebook_ads_30min_eu_v1.ad_insights_by_hour', '_fivetran_synced', 120
    ),
    NonHvrConfig(
        'lake_fivetran.med_facebook_ads_30min_na_v1.ad_insights_by_hour', '_fivetran_synced', 120
    ),
    NonHvrConfig('lake_fivetran.med_google_ads_v1.ad_spend', '_fivetran_synced', 120),
    # todo: uncomment SA360 once the connector issue is fixed.
    # NonHvrConfig('lake_fivetran.med_google_sa_360_v1.campaign_stats', '_fivetran_synced', 360),
    # Segment Critical Tables
    NonHvrConfig(
        'lake.segment_fl.java_fabletics_complete_registration', 'meta_create_datetime', 120
    ),
    NonHvrConfig(
        'lake.segment_fl.java_fabletics_ecom_mobile_app_order_completed',
        'meta_create_datetime',
        120,
    ),
    NonHvrConfig(
        'lake.segment_fl.java_fabletics_ecom_mobile_app_product_added', 'meta_create_datetime', 120
    ),
    NonHvrConfig(
        'lake.segment_fl.java_fabletics_ecom_mobile_app_signed_in', 'meta_create_datetime', 120
    ),
    NonHvrConfig('lake.segment_fl.java_fabletics_order_completed', 'meta_create_datetime', 120),
    NonHvrConfig('lake.segment_fl.java_fabletics_product_added', 'meta_create_datetime', 120),
    NonHvrConfig('lake.segment_fl.java_fabletics_signed_in', 'meta_create_datetime', 120),
    NonHvrConfig('lake.segment_fl.javascript_fabletics_page', 'meta_create_datetime', 120),
    NonHvrConfig(
        'lake.segment_fl.javascript_fabletics_product_viewed', 'meta_create_datetime', 120
    ),
    NonHvrConfig('lake.segment_fl.react_native_fabletics_branch_open', 'meta_create_datetime', 120),
    NonHvrConfig('lake.segment_fl.react_native_fabletics_screen', 'meta_create_datetime', 120),
    NonHvrConfig(
        'lake.segment_gfb.java_fabkids_complete_registration', 'meta_create_datetime', 120
    ),
    NonHvrConfig('lake.segment_gfb.java_fabkids_order_completed', 'meta_create_datetime', 120),
    NonHvrConfig('lake.segment_gfb.java_fabkids_product_added', 'meta_create_datetime', 120),
    NonHvrConfig('lake.segment_gfb.java_fabkids_signed_in', 'meta_create_datetime', 120),
    NonHvrConfig('lake.segment_gfb.java_jf_ecom_app_order_completed', 'meta_create_datetime', 120),
    NonHvrConfig('lake.segment_gfb.java_jf_ecom_app_product_added', 'meta_create_datetime', 120),
    NonHvrConfig('lake.segment_gfb.java_jf_ecom_app_signed_in', 'meta_create_datetime', 120),
    NonHvrConfig(
        'lake.segment_gfb.java_justfab_complete_registration', 'meta_create_datetime', 120
    ),
    NonHvrConfig('lake.segment_gfb.java_justfab_order_completed', 'meta_create_datetime', 120),
    NonHvrConfig('lake.segment_gfb.java_justfab_product_added', 'meta_create_datetime', 120),
    NonHvrConfig('lake.segment_gfb.java_justfab_signed_in', 'meta_create_datetime', 120),
    NonHvrConfig(
        'lake.segment_gfb.java_shoedazzle_complete_registration', 'meta_create_datetime', 120
    ),
    NonHvrConfig('lake.segment_gfb.java_shoedazzle_order_completed', 'meta_create_datetime', 120),
    NonHvrConfig('lake.segment_gfb.java_shoedazzle_product_added', 'meta_create_datetime', 120),
    NonHvrConfig('lake.segment_gfb.java_shoedazzle_signed_in', 'meta_create_datetime', 120),
    NonHvrConfig('lake.segment_gfb.javascript_fabkids_page', 'meta_create_datetime', 120),
    NonHvrConfig('lake.segment_gfb.javascript_fabkids_product_viewed', 'meta_create_datetime', 120),
    NonHvrConfig('lake.segment_gfb.javascript_justfab_page', 'meta_create_datetime', 120),
    NonHvrConfig('lake.segment_gfb.javascript_justfab_product_viewed', 'meta_create_datetime', 120),
    NonHvrConfig('lake.segment_gfb.javascript_shoedazzle_page', 'meta_create_datetime', 120),
    NonHvrConfig(
        'lake.segment_gfb.javascript_shoedazzle_product_viewed', 'meta_create_datetime', 120
    ),
    NonHvrConfig('lake.segment_gfb.react_native_justfab_screen', 'meta_create_datetime', 120),
    NonHvrConfig('lake.segment_sxf.java_sxf_complete_registration', 'meta_create_datetime', 120),
    NonHvrConfig('lake.segment_sxf.java_sxf_order_completed', 'meta_create_datetime', 120),
    NonHvrConfig('lake.segment_sxf.java_sxf_product_added', 'meta_create_datetime', 120),
    NonHvrConfig('lake.segment_sxf.java_sxf_signed_in', 'meta_create_datetime', 120),
    NonHvrConfig('lake.segment_sxf.javascript_sxf_page', 'meta_create_datetime', 120),
    NonHvrConfig('lake.segment_sxf.javascript_sxf_product_viewed', 'meta_create_datetime', 120),
    # Lake_Consolidated High Freq Critical Tables
    NonHvrConfig('lake_consolidated.ultra_merchant.cart', 'meta_update_datetime', 1680),
    NonHvrConfig('lake_consolidated.ultra_merchant.customer', 'meta_update_datetime', 240),
    NonHvrConfig('lake_consolidated.ultra_merchant.membership', 'meta_update_datetime', 240),
    NonHvrConfig('lake_consolidated.ultra_merchant."ORDER"', 'meta_update_datetime', 240),
    NonHvrConfig('lake_consolidated.ultra_merchant.session', 'meta_update_datetime', 240),
    # EDW Layer
    # EDW_PROD Critical Tables
    NonHvrConfig('edw_prod.data_model.fact_activation', 'meta_update_datetime', 600),
    NonHvrConfig('edw_prod.data_model.fact_registration', 'meta_update_datetime', 600),
    # Reporting Layer
    # REPORTING_BASE_PROD Critical Tables
    NonHvrConfig('reporting_base_prod.shared.session', 'meta_update_datetime', 600),
    # Media conversions
    NonHvrConfig(
        'reporting_media_base_prod.dbo.conversions',
        "convert_timezone('UTC','America/Los_Angeles',timestamp)",
        180,
    ),
]
