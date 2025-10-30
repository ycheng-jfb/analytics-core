from dataclasses import dataclass

import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.snowflake import SnowflakeAlertOperator, SnowflakeProcedureOperator
from include.config import email_lists, owners

default_args = {
    "start_date": pendulum.datetime(2022, 1, 25, 7, tz="America/Los_Angeles"),
    "retries": 3,
    "owner": owners.central_analytics,
    "email": email_lists.edw_support,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id="edw_p1_reporting_data_validation_and_alerting",
    default_args=default_args,
    schedule=None,
    catchup=False,
    max_active_tasks=2,
    max_active_runs=1,
)


@dataclass
class ValidationConfig:
    task_id: str
    sql_or_path: str
    distribution_list: list
    subject: str
    body: str


config_list = [
    ValidationConfig(
        task_id="finance_segments_missing_in_FSO",
        distribution_list=[
            "dragan@techstyle.com",
            "dataanalyticsdataplatforms@techstyle.com",
            "ralluri@techstyle.com",
        ],
        subject="Alert: EDW - Missing Finance Key Segments",
        body="The below segments in FSO are not available in reference.finance_segment_mapping:",
        sql_or_path="""SELECT DISTINCT fso.date,
                fso.date_object,
                fso.currency_object,
                fso.currency_type,
                fso.store_id,
                fso.vip_store_id,
                fso.is_retail_vip,
                fso.gender,
                fso.is_cross_promo,
                fso.finance_specialty_store
            FROM analytics_base.finance_sales_ops AS fso
                     LEFT JOIN reference.finance_segment_mapping AS fsm
                               ON fso.store_id = fsm.event_store_id
                                   AND fso.vip_store_id = fsm.vip_store_id
                                   AND fso.is_retail_vip = fsm.is_retail_vip
                                   AND fso.gender = fsm.customer_gender
                                   AND fso.is_cross_promo = fsm.is_cross_promo
                                   AND fso.finance_specialty_store = fsm.finance_specialty_store
                                   AND fso.is_scrubs_customer = fsm.is_scrubs_customer
                                   AND (
                                          fsm.is_daily_cash_usd = TRUE
                                          OR fsm.is_daily_cash_eur = TRUE
                                          OR fsm.report_mapping IN ('FLNA-OREV')
                                      )
                                   AND fsm.metric_type = 'Orders'
            WHERE fso.date IS NOT NULL
              AND fso.currency_object NOT IN ('hyperion', 'eur')
              AND fso.vip_store_id <> 3 -- vip store id 3 is legacy, so it should be ignored
              AND fsm.event_store_id IS NULL
            UNION ALL
            SELECT DISTINCT fso.date,
                            fso.date_object,
                            fso.currency_object,
                            fso.currency_type,
                            fso.store_id,
                            fso.vip_store_id,
                            fso.is_retail_vip,
                            fso.gender,
                            fso.is_cross_promo,
                            fso.finance_specialty_store
            FROM analytics_base.finance_sales_ops AS fso
                     LEFT JOIN reference.finance_segment_mapping fsm
                         ON fsm.event_store_id = fso.store_id
                AND fsm.vip_store_id = fso.vip_store_id
                AND fsm.is_retail_vip = fso.is_retail_vip
                AND fsm.customer_gender = fso.gender
                AND fsm.is_cross_promo = fso.is_cross_promo
                AND fsm.finance_specialty_store = fso.finance_specialty_store
                AND fso.is_scrubs_customer = fsm.is_scrubs_customer
                AND (
                    fsm.is_daily_cash_usd = TRUE
                    OR fsm.is_daily_cash_eur = TRUE
                    OR fsm.report_mapping IN ('FLNA-OREV')
                    )
                AND fsm.metric_type = 'Orders'
            WHERE fso.date IS NOT NULL
              AND fso.currency_object NOT IN ('hyperion', 'eur') --hyperion and eur are only for ddd
              AND date_object = 'placed'
              AND is_bop_vip = TRUE
              AND fso.product_order_count > 0
              AND fsm.event_store_id IS NULL;
""",
    ),
    ValidationConfig(
        task_id="finance_segments_duplicates",
        distribution_list=[
            "dragan@techstyle.com",
            "dataanalyticsdataplatforms@techstyle.com",
            "ralluri@techstyle.com",
        ],
        subject="Alert: EDW - Duplicate Finance Key Segments",
        body="The below segments are duplicated in reference.finance_segment_mappings :",
        sql_or_path="""SELECT metric_type,
                       event_store_id,
                       vip_store_id,
                       customer_gender,
                       is_cross_promo,
                       finance_specialty_store,
                       report_mapping,
                       count(*)
                FROM reference.finance_segment_mapping
                GROUP BY metric_type,
                         event_store_id,
                         vip_store_id,
                         is_retail_vip,
                         customer_gender,
                         is_cross_promo,
                         is_scrubs_customer,
                         finance_specialty_store,
                         mapping_start_date,
                         mapping_end_date,
                         is_ddd_consolidated,
                         ddd_consolidated_currency_type,
                         is_ddd_individual,
                         ddd_individual_currency_type,
                         is_ddd_hyperion,
                         ddd_hyperion_currency_type,
                         is_retail_attribution_ddd,
                         retail_attribution_ddd_currency_type,
                         is_daily_cash_usd,
                         daily_cash_usd_currency_type,
                         is_daily_cash_eur,
                         daily_cash_eur_currency_type,
                         is_vip_tenure,
                         vip_tenure_currency_type,
                         is_lead_to_vip_waterfall,
                         lead_to_vip_waterfall_currency_type,
                         report_mapping,
                         business_unit
                HAVING count(*) > 1;
""",
    ),
    ValidationConfig(
        task_id="send_daily_cash_variance_alert",
        distribution_list=[
            "dragan@techstyle.com",
            "dataanalyticsdataplatforms@techstyle.com",
            "ralluri@techstyle.com",
        ],
        subject="Alert: EDW - FSO and Daily Cash Variance",
        body="Below are the top 10 variances between FSO and reporting.daily_cash_base_calc",
        sql_or_path=""" SELECT date,
                date_object,
                currency_object,
                report_mapping,
                order_membership_classification_key,
                metric,
                fso_value,
                daily_cash_value,
                variance
            FROM validation.finance_sales_ops_daily_cash_base_comparison
            ORDER BY ABS(variance) DESC
            LIMIT 10;
""",
    ),
]


with dag:
    for i in config_list:
        op = SnowflakeAlertOperator(
            task_id=i.task_id,
            sql_or_path=i.sql_or_path,
            distribution_list=i.distribution_list,
            database='EDW_PROD',
            subject=i.subject,
            body=i.body,
        )
    fso_daily_cash_validation = SnowflakeProcedureOperator(
        procedure="validation.finance_sales_ops_daily_cash_base_comparison.sql",
        database="edw_prod",
    )


fso_daily_cash_validation >> op
