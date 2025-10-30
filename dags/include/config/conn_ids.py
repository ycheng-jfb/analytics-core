from enum import Enum, unique


@unique
class ConnIdEnum(str, Enum):
    def __new__(cls, value):
        obj = str.__new__(cls, value)
        obj._value_ = value
        return obj

    def __str__(self):
        return self._value_


class AWS(ConnIdEnum):
    default = 'aws_default'

    tfg_default = 'tfg_aws_default'

    amazon_default = 'amazon_default'


class Azure(ConnIdEnum):
    azure_default = 'azure_default'


class S3(ConnIdEnum):
    tsos_da_int_prod = 'aws_s3_tsos_da_int_prod'
    """prod tsos-da-int-* buckets"""

    tsos_da_int_dev = 'aws_s3_tsos_da_int_dev'
    """prod tsos-da-int-*-dev buckets"""

    tsos_da_slm_export = 'aws_s3_slm_export'  # TODO: references temporarily hard coded
    """prod slm-global-incoming-fabletics buckets"""

    tfg_media_key = 'aws_s3_tfg_media_key'
    """was for buckets tfg-media and tfg-media-dev, now just for attentive owned bucket access"""


class Facebook(ConnIdEnum):
    default = 'facebook_default'

    ads_default = 'facebook_ads_default'

    na = 'facebook_na'


class Google(ConnIdEnum):
    ads_default = 'google_ads_default'

    cloud_default = 'google_cloud_default'

    export_default = 'google_offline_export_default'

    cloud_rfid = 'google_cloud_rfid'


class BigQuery(ConnIdEnum):
    default = 'bigquery_default'


class Lithium(ConnIdEnum):
    default = 'lithium_default'


class SMB(ConnIdEnum):
    default = 'smb_default'

    app01 = 'smb_app01'
    """file share itbelspmw8app01"""
    nas01 = 'smb_nas01'
    """file share itbtsnnas01"""
    ngc13 = 'smb_ngc13'
    """file share tfgelsvm16ngc13"""


class Fivetran(ConnIdEnum):
    default = 'fivetran_default'


class SlackAlert(ConnIdEnum):
    slack_default = "slack_default"
    edm = 'slack_alert_edm'
    edm_p1 = 'slack_alert_p1'
    edm_p1_edw = 'slack_alert_p1_edw'
    media = 'slack_alert_media'
    media_p1 = 'slack_alert_media_p1'
    gsc = 'slack_alert_gsc'
    testing = 'slack_alert_testing'
    data_science = 'slack_alert_data_science'
    marketing_science = 'slack_alert_marketing_science'
    sxf_data_team = 'slack_alert_sxf_data_team'
    data_integrations = 'slack_data_integrations'


class MsSql(ConnIdEnum):

    default = 'mssql_default'
    db166_app_airflow_rw = 'mssql_db166_app_airflow_rw'
    justfab_app_airflow_rw = 'mssql_justfab_app_airflow_rw'
    justfab_app_airflow = 'mssql_justfab_app_airflow'
    fabletics_app_airflow_rw = 'mssql_fabletics_app_airflow_rw'
    fabletics_app_airflow_rw_analytic = 'mssql_fabletics_app_airflow_rw_analytic'
    fabletics_app_airflow = 'mssql_fabletics_app_airflow'
    savagex_app_airflow_rw = 'mssql_savagex_app_airflow_rw'
    savagex_app_airflow_rw_analytic = 'mssql_savagex_app_airflow_rw_analytic'
    savagex_app_airflow = 'mssql_savagex_app_airflow'
    db50_app_airflow_rw = 'mssql_db50_app_airflow_rw'
    dbd01_app_airflow = 'mssql_dbd01_app_airflow'
    dbp40_app_airflow_rw = 'mssql_dbp40_app_airflow_rw'
    ds_qa_justfab_app_airflow_rw = 'mssql_ds_qa_justfab_app_airflow_rw'
    ds_qa_fabletics_app_airflow_rw = 'mssql_ds_qa_fabletics_app_airflow_rw'
    evolve01_app_airflow_rw = 'mssql_evolve01_app_airflow_rw'
    edw01_app_airflow = 'mssql_edw01_app_airflow'

    """
    For these databases for JFB brand:

        * ``gdpr``
        * ``ultracms``
        * ``ultramerchant``
        * ``ultrarollup``
        * ``ultracart``
    """

    evolve01_app_airflow = 'mssql_evolve01_app_airflow'
    """
    For these databases:

        * ``ePLMv14Prod``
        * ``JF_Portal``
        * ``Merlin``
        * ``ngc_plm_prodcomp01``
        * ``ngc_plm_prodcomp02``
        * ``SSRS_Reports``
        * ``ultrawarehouse``
        * ``countwise``
    """

    bento_prod_reader_app_airflow = 'mssql_bento_prod_reader_app_airflow'
    """
    For these databases for non-JFB brands:

        * ``gdpr``
        * ``ultracms``
        * ``ultramerchant``
        * ``ultrarollup``
        * ``ultracart``
    """

    jfgcdb01_app_airflow = 'mssql_jfgcdb01_app_airflow'
    """
    For ``jfgc`` database only.
    """
    giftco_app_airflow = 'mssql_giftco_app_airflow'  # 2019 server

    dbp61_app_airflow = 'mssql_dbp61_app_airflow'
    """
    For um_replicated database, where CDC tables are located.
    """

    dbp40_app_airflow = 'mssql_dbp40_app_airflow_rw'

    dbd05_app_airflow = 'mssql_dbd05_app_airflow'


class MySQL(ConnIdEnum):
    default = 'mysql_default'


class Postgres(ConnIdEnum):
    aws_mwaa_prod = 'postgres_aws_mwaa_prod'
    """Metastore for production airflow instance"""

    default = 'postgres_default'


class SFTP(ConnIdEnum):

    default = 'sftp_default'

    ssh_default = 'ssh_default'

    sftp_justfab = 'sftp_justfab'
    """For JustFab sftp"""

    sftp_techstyle_optout = 'sftp_techstyle_optout'
    """For Techstyle optout"""

    sftp_storeforce = 'sftp_storeforce'

    sftp_truevuesps = 'sftp_truevuesps'

    sftp_storeforce_sxf = 'sftp_storeforce_sxf'
    """For storeforce feed"""

    sftp_blisspoint = 'sftp_blisspoint'
    """For media blisspoint"""

    sftp_fl_local_feed = 'sftp_fl_local_feed'
    """For media Local Inventory Feed"""

    sftp_impactradius = 'sftp_impactradius'
    """For media impactradius"""

    sftp_emarsys = 'sftp_emarsys'
    """For media emarsys"""

    sftp_cj_sxna = 'sftp_cj_sxna'
    """For media CJ SXNA"""

    sftp_cj_flna = 'sftp_cj_flna'
    """For media CJ FLNA"""

    sftp_cj_fkna = 'sftp_cj_fkna'
    """For media CJ FKNA"""

    sftp_cj_jfna = 'sftp_cj_jfna'
    """For media CJ JFNA"""

    sftp_cj_sdna = 'sftp_cj_sdna'
    """For media CJ SDNA"""

    sftp_cj_sxeu = 'sftp_cj_sxeu'
    """For media CJ SXEU and SXUK"""

    sftp_salesfloor = 'sftp_salesfloor'
    """For Salesfloor"""

    sftp_salesfloor_qa = 'sftp_salesfloor_qa'
    """For Salesfloor"""

    sftp_nice_workforce = 'sftp_nice_workforce'
    """For Nice Workforce Management"""

    sftp_rokt = 'sftp_rokt'
    """For ROKT """

    sftp_callminer = "sftp_callminer"
    """For CallMiner"""

    sftp_shoppertrak = 'sftp_shoppertrak'

    sftp_shoppertrak_sxf = 'sftp_shoppertrak_sxf'

    sftp_blue_yonder_prod = 'sftp_blue_yonder_prod'

    sftp_blue_yonder_test = 'sftp_blue_yonder_test'

    sftp_pebblepost = 'SFTP_PEBBLEPOST'

    sftp_techstyle_nordstrom = 'sftp_techstyle_nordstrom'

    ftp_fl_stylitics_product = 'ftp_fl_stylitics_product'

    sftp_sps_spscommerce = 'sftp_sps_spscommerce'

    ftp_booking_data = 'ftp_booking_data'

    sftp_sps_commercevan = 'sftp_sps_commercevan'

    sftp_purolator = 'sftp_purolator'


class CreatorIQ(ConnIdEnum):
    creatoriq_default = 'creatoriq_default'


class Snowflake(ConnIdEnum):
    default = 'snowflake_default'
    """currently only one Snowflake env"""

    accounting = 'snowflake_accounting'

    edw = 'snowflake_edw'

    account_admin = 'snowflake_account_admin'


class Genesys(ConnIdEnum):
    genesys_bond = 'genesys_bond'

    default = 'genesys_default'

    entities = 'genesys_entities'


class Northbeam(ConnIdEnum):
    default = 'northbeam_default'


class Openpath(ConnIdEnum):
    default = 'openpath_default'


class Rokt(ConnIdEnum):
    default = 'rokt_default'


class Roku(ConnIdEnum):
    default = 'roku_default'


class Smartly(ConnIdEnum):
    default = 'smartly_default'


class Snapchat(ConnIdEnum):
    default = 'snapchat_default'

    conversions_default = 'snapchat_conversions_default'


class StoreForce(ConnIdEnum):
    default = 'storeforce_default'


class Tableau(ConnIdEnum):
    default = 'tableau_default'


class Tatari(ConnIdEnum):
    default = 'tatari_default'


class Tiktok(ConnIdEnum):
    default = 'tiktok_default'

    shop_default = 'tiktok_shop_default'


class Zendesk(ConnIdEnum):
    default = 'zendesk_default'


class Stella(ConnIdEnum):
    default = 'stella_default'


class Sprinklr(ConnIdEnum):
    default = 'sprinklr_default'


class Databricks(ConnIdEnum):
    duplo = 'databricks_duplo'


class AEM(ConnIdEnum):
    metadata_api = 'aem_metadata_api'


class Callminer(ConnIdEnum):
    default = 'callminer_default'


class Elasticsearch(ConnIdEnum):
    fabletics_es7_prod = 'elasticsearch_fabletics-es7_prod'


class GPG(ConnIdEnum):
    default = 'gpg_default'

    nordstrom_private_key = 'nordstrom_private_key'


class Sharepoint(ConnIdEnum):
    default = 'sharepoint_default'

    nordstrom = 'sharepoint_nordstrom'


class TestRail(ConnIdEnum):
    default = 'test_rail_default'


class Sailpoint(ConnIdEnum):
    default = 'sailpoint_default'


class CallminerBulk(ConnIdEnum):
    default = 'callminer_bulk_default'  # TODO: references temporarily hard coded


class Sentry(ConnIdEnum):
    default = 'sentry_default'
