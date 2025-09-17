from dataclasses import dataclass
from typing import List, Union

import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_media
from include.airflow.operators.snowflake import SnowflakeAlertOperator
from include.config import owners, conn_ids
from include.config.email_lists import airflow_media_support
from include.utils.external.facebook import ACCOUNT_CONFIG
from include.utils.external.google import ADVERTISER_LIST, CLIENT_CUSTOMER_ID_LIST
from include.utils.external.pinterest import ADVERTISER_LIST as pinterest_act_list


@dataclass
class AccountAlert:
    source: str
    reference_column: str
    source_id_list: list
    distribution_list: list
    alert_type: str = "mail"
    slack_conn_id: str = conn_ids.SlackAlert.media
    slack_channel_name: str = "airflow-alerts-media"
    body: str = "Following accounts are not present in schedule"

    def __post_init__(self):
        self.task_id = f"alert_{self.source.replace(' ', '_')}_id_mismatch".lower()
        self.subject = f"Airflow alert: account mapping '{self.source}' is out of sync"

    @property
    def sql(self):
        source_id_csv = ", ".join(f"'{w}'" for w in self.source_id_list)
        sql_cmd = f"""SELECT
                            l.source,
                            l.reference_column,
                            l.source_id
                        FROM lake_view.sharepoint.med_account_mapping_media l
                        WHERE l.source = '{self.source}'
                            AND l.reference_column = '{self.reference_column}'
                            AND l.source_id NOT IN ({source_id_csv})
                        ORDER BY 1,2;
                        """
        return sql_cmd

    @property
    def alert_operator(self):
        return SnowflakeAlertOperator(
            task_id=self.task_id,
            database='lake',
            sql_or_path=self.sql,
            subject=self.subject,
            body=self.body,
            distribution_list=self.distribution_list,
            alert_type=self.alert_type,
            slack_conn_id=self.slack_conn_id,
        )


@dataclass
class Alert:
    sql: str
    subject: str
    body: str
    database: str
    distribution_list: list
    alert_type: str = "mail"
    slack_conn_id: str = conn_ids.SlackAlert.media
    slack_channel_name: str = "airflow-alerts-media"

    def __post_init__(self):
        self.task_id = f"alert_for_{self.subject.replace(' ', '_')}".lower()

    @property
    def alert_operator(self):
        return SnowflakeAlertOperator(
            task_id=self.task_id,
            sql_or_path=self.sql,
            database=self.database,
            subject=self.subject,
            body=self.body,
            distribution_list=self.distribution_list,
            alert_type=self.alert_type,
            slack_conn_id=self.slack_conn_id,
            slack_channel_name=self.slack_channel_name,
        )


config_list: List[Union[Alert, AccountAlert]] = [
    AccountAlert(
        source="Adwords GDN",
        reference_column="account_id",
        source_id_list=CLIENT_CUSTOMER_ID_LIST,
        distribution_list=airflow_media_support,
        alert_type=['slack', 'email'],
    ),
    AccountAlert(
        source="Doubleclick Advertiser",
        reference_column="advertiser_id",
        source_id_list=[adv.id for adv in ADVERTISER_LIST],
        distribution_list=airflow_media_support,
        alert_type=['slack', 'email'],
    ),
    AccountAlert(
        source="Facebook",
        reference_column="account_id",
        source_id_list=[
            x.replace("act_", "")
            for region, account_list in ACCOUNT_CONFIG.items()
            for x in account_list
        ],
        distribution_list=airflow_media_support,
        alert_type=['slack', 'email'],
    ),
    AccountAlert(
        source="Pinterest",
        reference_column="account_id",
        source_id_list=[adv.advertiser_id for adv in pinterest_act_list],
        distribution_list=airflow_media_support,
        alert_type=['slack', 'email'],
    ),
    Alert(
        sql="""SELECT
                   sg.label AS store_group,
                   pc.product_category_id,
                   pc.label || ' -> ' || pc2.label AS category,
                   CAST(pc.datetime_added AS DATE) AS datetime_created
            FROM ultra_merchant.product_category pc
                     JOIN ultra_merchant.store_group sg ON
                        sg.store_group_id = pc.store_group_id
                     LEFT JOIN ultra_merchant.product_category pc2
                               ON pc.parent_product_category_id = pc2.product_category_id
            WHERE pc.datetime_added >= DATEADD(DAY, -7, CURRENT_DATE());
            """,
        subject="New Product Categories",
        database='lake_consolidated_view',
        body="The following product categories have been added within the last week.",
        distribution_list=[
            'dragan@techstyle.com',
            'knowack@techstyle.com',
            'KXue@TechStyle.com',
            'Mhill@techstyle.com',
        ],
        alert_type=['slack', 'email'],
    ),
    Alert(
        sql="""
            SELECT creative_code, brand
            FROM (
                SELECT creative_code, 'scb' AS brand
                FROM lake_view.sharepoint.med_scb_creative_dimensions
                UNION ALL
                SELECT creative_code, 'fk' AS brand
                FROM lake_view.sharepoint.med_fk_creative_dimensions
                UNION ALL
                SELECT creative_code, 'jfb' AS brand
                FROM lake_view.sharepoint.med_jfb_creative_dimensions
                UNION ALL
                SELECT creative_code, 'fl' AS brand
                FROM lake_view.sharepoint.med_fl_creative_dimensions
                UNION ALL
                SELECT creative_code, 'flm' AS brand
                FROM lake_view.sharepoint.med_flm_creative_dimensions
                UNION ALL
                SELECT creative_code, 'sx' AS brand
                FROM lake_view.sharepoint.med_sx_creative_dimensions
                UNION ALL
                SELECT creative_code, 'yty' AS brand
                FROM lake_view.sharepoint.med_yty_creative_dimensions
            )
            WHERE creative_code IS NOT NULL
            QUALIFY row_number() OVER(PARTITION BY creative_code ORDER BY NULL)>1;
        """,
        database='lake_view',
        subject="Duplicate Creative Codes Found",
        body="Following are creative_codes with duplicates",
        distribution_list=[],
        alert_type="slack",
    ),
]

default_args = {
    "start_date": pendulum.datetime(2018, 7, 16, 0, tz="America/Los_Angeles"),
    "retries": 1,
    'owner': owners.media_analytics,
    "email": airflow_media_support,
    "on_failure_callback": slack_failure_media,
}


dag = DAG(
    dag_id="media_alerts",
    default_args=default_args,
    schedule="0 10 * * *",
    catchup=False,
    max_active_tasks=20,
    max_active_runs=1,
)

with dag:
    for alert_cfg in config_list:
        alert_cfg.alert_operator
