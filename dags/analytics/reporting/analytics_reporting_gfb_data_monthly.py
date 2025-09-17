import pendulum
from airflow.models import DAG
from airflow.operators.dummy import DummyOperator

from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.airflow.operators.tableau import TableauRefreshOperator
from include.config import owners
from include.config.email_lists import analytics_support

default_args = {
    "depends_on_past": False,
    "start_date": pendulum.datetime(2024, 1, 25, 7, tz="America/Los_Angeles"),
    "retries": 1,
    "owner": owners.jfb_analytics,
    "email": analytics_support,
}

dag = DAG(
    dag_id="analytics_reporting_gfb_data_monthly",
    default_args=default_args,
    schedule_interval="0 0 1 * *",
    max_active_tasks=15,
    catchup=False,
)

with dag:
    gfb001_02_customer_lifetime_value_no_cust_seg = SnowflakeProcedureOperator(
        procedure="gfb.gfb001_02_customer_lifetime_value_no_cust_seg.sql",
        database="reporting_prod",
        warehouse="DA_WH_ETL_HEAVY",
        autocommit=False,
    )

    tableau_refresh_gfb001_02_customer_lifetime_value_no_cust_seg = (
        TableauRefreshOperator(
            task_id="tableau_refresh_gfb001_02_customer_lifetime_value_no_cust_seg",
            data_source_name="GFB001_02_CUSTOMER_LIFETIME_VALUE_NO_CUST_SEG",
        )
    )

    gfb_vip_lifetime_value_monthly = SnowflakeProcedureOperator(
        procedure="gfb.gfb_vip_lifetime_value_monthly.sql",
        database="reporting_prod",
        warehouse="DA_WH_ETL_HEAVY",
        autocommit=False,
    )

    tableau_refresh_gfb_vip_lifetime_value_monthly = TableauRefreshOperator(
        task_id="tableau_refresh_gfb_vip_lifetime_value_monthly",
        data_source_name="GFB_VIP_LIFETIME_VALUE_MONTHLY",
    )

    gfb012_01_failed_payment_method = SnowflakeProcedureOperator(
        procedure="gfb.gfb012_01_failed_payment_method.sql",
        database="reporting_prod",
        autocommit=False,
    )

    tableau_refresh_gfb012_01_failed_payment_method = TableauRefreshOperator(
        task_id="tableau_refresh_gfb012_01_failed_payment_method",
        data_source_name="GFB012_01_FAILED_PAYMENT_METHOD",
    )

    gfb033_merch_kpi_over_time = SnowflakeProcedureOperator(
        procedure="gfb.gfb033_merch_kpi_over_time.sql",
        database="reporting_prod",
        autocommit=False,
    )

    gfb032_customer_promo_data_set = SnowflakeProcedureOperator(
        procedure="gfb.gfb032_customer_promo_data_set.sql",
        database="reporting_prod",
        autocommit=False,
    )

    tableau_refresh_gfb032_customer_promo_data_set = TableauRefreshOperator(
        task_id="tableau_refresh_gfb032_customer_promo_data_set",
        data_source_name="GFB032_CUSTOMER_PROMO_DATA_SET",
    )

    tableau_refresh_gfb033_merch_kpi_over_time = TableauRefreshOperator(
        task_id="tableau_refresh_gfb033_merch_kpi_over_time",
        data_source_name="GFB033_MERCH_KPI_OVER_TIME",
    )

    gfb035_merch_future_showroom_products = SnowflakeProcedureOperator(
        procedure="gfb.gfb035_merch_future_showroom_products.sql",
        database="reporting_prod",
        autocommit=False,
    )

    tableau_refresh_gfb035_merch_future_showroom_products = TableauRefreshOperator(
        task_id="tableau_refresh_gfb035_merch_future_showroom_products",
        data_source_name="GFB035_MERCH_FUTURE_SHOWROOM_PRODUCTS",
    )

    gfb036_loyalty_points_outstanding = SnowflakeProcedureOperator(
        procedure="gfb.gfb036_loyalty_points_outstanding.sql",
        database="reporting_prod",
        autocommit=False,
    )

    tableau_refresh_gfb036_loyalty_points_outstanding = TableauRefreshOperator(
        task_id="tableau_refresh_gfb036_loyalty_points_outstanding",
        data_source_name="GFB036_LOYALTY_POINTS_OUTANDING",
    )

    gfb037_loyalty_points_redemption = SnowflakeProcedureOperator(
        procedure="gfb.gfb037_loyalty_points_redemption.sql",
        database="reporting_prod",
        autocommit=False,
    )

    tableau_refresh_gfb037_loyalty_points_redemption = TableauRefreshOperator(
        task_id="tableau_refresh_gfb037_loyalty_points_redemption",
        data_source_name="GFB037_LOYALTY_POINTS_REDEMPTION",
    )

    gfb054_credit_billing_deep_dive = SnowflakeProcedureOperator(
        procedure="gfb.gfb054_credit_billing_deep_dive.sql",
        database="reporting_prod",
        autocommit=False,
    )

    tableau_refresh_gfb054_01_credit_billing_deep_dive = TableauRefreshOperator(
        task_id="tableau_refresh_gfb054_01_credit_billing_deep_dive",
        data_source_name="GFB054_01_CREDIT_BILLING_DEEP_DIVE",
    )

    tableau_refresh_gfb054_02_v1_credit_billing_deep_dive = TableauRefreshOperator(
        task_id="tableau_refresh_gfb054_02_v1_credit_billing_deep_dive",
        data_source_name="GFB054_02_V1_CREDIT_BILLING_DEEP_DIVE",
    )

    tableau_refresh_gfb054_02_v2_credit_billing_deep_dive = TableauRefreshOperator(
        task_id="tableau_refresh_gfb054_02_v2_credit_billing_deep_dive",
        data_source_name="GFB054_02_V2_CREDIT_BILLING_DEEP_DIVE",
    )

    tableau_refresh_gfb054_03_credit_billing_deep_dive = TableauRefreshOperator(
        task_id="tableau_refresh_gfb054_03_credit_billing_deep_dive",
        data_source_name="GFB054_03_CREDIT_BILLING_DEEP_DIVE",
    )

    tableau_refresh_gfb054_04_credit_billing_deep_dive = TableauRefreshOperator(
        task_id="tableau_refresh_gfb054_04_credit_billing_deep_dive",
        data_source_name="GFB054_04_CREDIT_BILLING_DEEP_DIVE",
    )

    tableau_refresh_gfb054_05_credit_billing_deep_dive = TableauRefreshOperator(
        task_id="tableau_refresh_gfb054_05_credit_billing_deep_dive",
        data_source_name="GFB054_05_CREDIT_BILLING_DEEP_DIVE",
    )

    gfb039_passive_cancel = SnowflakeProcedureOperator(
        procedure="gfb.gfb039_passive_cancel.sql",
        database="reporting_prod",
        autocommit=False,
    )

    tableau_refresh_gfb039_passive_cancel = TableauRefreshOperator(
        task_id="tableau_refresh_gfb039_passive_cancel",
        data_source_name="GFB039_PASSIVE_CANCEL",
    )

    gfb039_01_activating_customer_detail = SnowflakeProcedureOperator(
        procedure="gfb.gfb039_01_activating_customer_detail.sql",
        database="reporting_prod",
        autocommit=False,
    )

    tableau_refresh_gfb039_01_activating_customer_detail = TableauRefreshOperator(
        task_id="tableau_refresh_gfb039_01_activating_customer_detail",
        data_source_name="GFB039_01_ACTIVATING_CUSTOMER_DETAIL",
    )

    gfb045_cohort_data_set = SnowflakeProcedureOperator(
        procedure="gfb.gfb045_cohort_data_set.sql",
        database="reporting_prod",
        autocommit=False,
    )

    tableau_refresh_gfb045_cohort_data_set = TableauRefreshOperator(
        task_id="tableau_refresh_gfb045_cohort_data_set",
        data_source_name="GFB045_COHORT_DATA_SET",
    )

    gfb065_sd_one_click_tracking = SnowflakeProcedureOperator(
        procedure="gfb.gfb065_sd_one_click_tracking.sql",
        database="reporting_prod",
        autocommit=False,
    )

    gfb065_sd_one_click_tracking_qa = SnowflakeProcedureOperator(
        procedure="gfb.gfb065_sd_one_click_tracking_qa.sql",
        database="reporting_prod",
        autocommit=False,
    )

    tableau_refresh_gfb065_sd_one_click_tracking = TableauRefreshOperator(
        task_id="tableau_refresh_gfb065_sd_one_click_tracking",
        data_source_name="GFB065_SD_ONE_CLICK_TRACKING",
    )

    gfb068_credit_billing_pause_tracking = SnowflakeProcedureOperator(
        procedure="gfb.gfb068_credit_billing_pause_tracking.sql",
        database="reporting_prod",
        autocommit=False,
    )

    tableau_refresh_gfb068_credit_billing_pause_tracking = TableauRefreshOperator(
        task_id="tableau_refresh_gfb068_credit_billing_pause_tracking",
        data_source_name="GFB068_CREDIT_BILLING_PAUSE_TRACKING",
    )

    tableau_refresh_gfb001_customer_seg_count_all_time = TableauRefreshOperator(
        task_id="tableau_refresh_gfb001_customer_seg_count_all_time",
        data_source_name="GFB001_CUSTOMER_SEG_COUNT_ALL_TIME",
    )

    tableau_refresh_gfb037_01_loyalty_item_sold = TableauRefreshOperator(
        task_id="tableau_refresh_gfb037_01_loyalty_item_sold",
        data_source_name="GFB037_01_LOYALTY_ITEM_SOLD",
    )

    jf_sd_downgraded_members_monthly = SnowflakeProcedureOperator(
        procedure="gfb.jf_sd_downgraded_members_monthly.sql",
        database="reporting_prod",
        autocommit=False,
    )

    jf_sd_dual_members_monthly = SnowflakeProcedureOperator(
        procedure="gfb.jf_sd_dual_members_monthly.sql",
        database="reporting_prod",
        autocommit=False,
    )

    jf_fk_downgraded_members_monthly = SnowflakeProcedureOperator(
        procedure="gfb.jf_fk_downgraded_members_monthly.sql",
        database="reporting_prod",
        autocommit=False,
    )

    jf_fk_dual_members_monthly = SnowflakeProcedureOperator(
        procedure="gfb.jf_fk_dual_members_monthly.sql",
        database="reporting_prod",
        autocommit=False,
    )

    join_branch_0 = DummyOperator(task_id="join_branch_0", trigger_rule="none_failed")

    join_branch_layer2_03 = DummyOperator(
        task_id="join_branch_layer2_03", trigger_rule="none_failed"
    )

    [
        jf_sd_downgraded_members_monthly,
        jf_sd_dual_members_monthly,
        jf_fk_downgraded_members_monthly,
        jf_fk_dual_members_monthly,
    ] >> join_branch_0

    join_branch_0 >> [
        gfb012_01_failed_payment_method,
        gfb033_merch_kpi_over_time,
        gfb035_merch_future_showroom_products,
        gfb036_loyalty_points_outstanding,
        gfb039_passive_cancel,
        gfb039_01_activating_customer_detail,
        gfb045_cohort_data_set,
        gfb065_sd_one_click_tracking,
        gfb065_sd_one_click_tracking_qa,
        gfb068_credit_billing_pause_tracking,
        gfb054_credit_billing_deep_dive,
    ]

    gfb012_01_failed_payment_method >> join_branch_layer2_03

    join_branch_layer2_03 >> tableau_refresh_gfb012_01_failed_payment_method

    (
        gfb001_02_customer_lifetime_value_no_cust_seg
        >> tableau_refresh_gfb001_02_customer_lifetime_value_no_cust_seg
    )

    gfb_vip_lifetime_value_monthly >> tableau_refresh_gfb_vip_lifetime_value_monthly

    gfb032_customer_promo_data_set >> tableau_refresh_gfb032_customer_promo_data_set

    gfb033_merch_kpi_over_time >> tableau_refresh_gfb033_merch_kpi_over_time

    (
        gfb035_merch_future_showroom_products
        >> tableau_refresh_gfb035_merch_future_showroom_products
    )

    (
        gfb036_loyalty_points_outstanding
        >> tableau_refresh_gfb036_loyalty_points_outstanding
    )

    gfb037_loyalty_points_redemption >> tableau_refresh_gfb037_loyalty_points_redemption

    gfb045_cohort_data_set >> tableau_refresh_gfb045_cohort_data_set

    gfb039_passive_cancel >> tableau_refresh_gfb039_passive_cancel

    (
        gfb039_01_activating_customer_detail
        >> tableau_refresh_gfb039_01_activating_customer_detail
    )

    [
        gfb065_sd_one_click_tracking,
        gfb065_sd_one_click_tracking_qa,
    ] >> tableau_refresh_gfb065_sd_one_click_tracking

    (
        gfb068_credit_billing_pause_tracking
        >> tableau_refresh_gfb068_credit_billing_pause_tracking
    )

    gfb054_credit_billing_deep_dive >> [
        tableau_refresh_gfb054_01_credit_billing_deep_dive,
        tableau_refresh_gfb054_02_v1_credit_billing_deep_dive,
        tableau_refresh_gfb054_02_v2_credit_billing_deep_dive,
        tableau_refresh_gfb054_03_credit_billing_deep_dive,
        tableau_refresh_gfb054_04_credit_billing_deep_dive,
        tableau_refresh_gfb054_05_credit_billing_deep_dive,
    ]
