import pendulum
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from include.config import owners
from include.config.email_lists import analytics_support
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.airflow.operators.tableau import TableauRefreshOperator

default_args = {
    'depends_on_past': False,
    'start_date': pendulum.datetime(2019, 1, 1, 7, tz='America/Los_Angeles'),
    'retries': 1,
    'owner': owners.jfb_analytics,
    'email': analytics_support,
}

dag = DAG(
    dag_id="analytics_reporting_gfb_data_center_daily_5am",
    default_args=default_args,
    schedule_interval="0 0 * * *",
    max_active_tasks=15,
    catchup=False,
)

with dag:
    gfb_order_line_data_set = SnowflakeProcedureOperator(
        procedure='gfb.gfb_order_line_data_set.sql',
        database='reporting_prod',
        # warehouse='DA_WH_ETL_HEAVY',
        autocommit=False,
    )

    gfb_inventory_data_set = SnowflakeProcedureOperator(
        procedure='gfb.gfb_inventory_data_set.sql',
        database='reporting_prod',
        autocommit=False,
    )

    gfb_po_data_set = SnowflakeProcedureOperator(
        procedure='gfb.gfb_po_data_set.sql',
        database='reporting_prod',
        autocommit=False,
    )

    merch_dim_product = SnowflakeProcedureOperator(
        procedure='gfb.merch_dim_product.sql',
        database='reporting_prod',
        autocommit=False,
    )

    dim_promo = SnowflakeProcedureOperator(
        procedure='gfb.dim_promo.sql',
        database='reporting_prod',
        autocommit=False,
    )

    gfb_crossover_customers_behaviour = SnowflakeProcedureOperator(
        procedure='gfb.gfb_crossover_customers_behaviour.sql',
        database='reporting_prod',
        autocommit=False,
    )

    sd_merge_acquisition_reporting = SnowflakeProcedureOperator(
        procedure='gfb.sd_merge_acquisition_reporting.sql',
        database='reporting_prod',
        autocommit=False,
    )

    gfb_segment_pageviews = SnowflakeProcedureOperator(
        procedure='gfb.gfb_segment_pageviews.sql',
        database='reporting_prod',
        autocommit=False,
    )

    gfb_product_review_data_set = SnowflakeProcedureOperator(
        procedure='gfb.gfb_product_review_data_set.sql',
        database='reporting_prod',
        autocommit=False,
    )

    dos_107_merch_data_set = SnowflakeProcedureOperator(
        procedure='gfb.dos_107_merch_data_set.sql',
        database='reporting_prod',
        autocommit=False,
    )

    # tableau_refresh_dos_107_merch_data_set_placed_date = TableauRefreshOperator(
    #     task_id='tableau_refresh_dos_107_merch_data_set_placed_date',
    #     data_source_name='DOS_107_MERCH_DATA_SET_BY_PLACE_DATE_EXTRACT',
    # )
    #
    # tableau_refresh_dos_107_merch_data_set_shipped_date = TableauRefreshOperator(
    #     task_id='tableau_refresh_dos_107_merch_data_set_shipped_date',
    #     data_source_name='DOS_107_MERCH_DATA_SET_BY_SHIP_DATE_EXTRACT',
    # )
    #
    # tableau_refresh_dos_107_merch_dataset_daily_sales = TableauRefreshOperator(
    #     task_id='tableau_refresh_dos_107_merch_dataset_daily_sales',
    #     data_source_name='DOS_107_MERCH_DATASET_DAILY_SALES',
    # )
    #
    # tableau_refresh_dos_107_merch_data_set_by_place_date_return_by_order_date = (
    #     TableauRefreshOperator(
    #         task_id='tableau_refresh_dos_107_merch_data_set_by_place_date_return_by_order_date',
    #         data_source_name='DOS_107_MERCH_DATA_SET_BY_PLACE_DATE_RETURN_BY_ORDER_DATE',
    #     )
    # )

    gfb011_promo_data_set = SnowflakeProcedureOperator(
        procedure='gfb.gfb011_promo_data_set.sql',
        database='reporting_prod',
        autocommit=False,
    )

    gfb011_promo_data_set_exchanges_reships = SnowflakeProcedureOperator(
        procedure='gfb.gfb011_promo_data_set_exchanges_reships.sql',
        database='reporting_prod',
        autocommit=False,
    )

    # tableau_refresh_gfb011_promo_data_set = TableauRefreshOperator(
    #     task_id='tableau_refresh_gfb011_promo_data_set',
    #     data_source_name='GFB011_PROMO_DATA_SET',
    # )
    #
    # tableau_refresh_gfb011_promo_data_set_exchanges_reships = TableauRefreshOperator(
    #     task_id='tableau_refresh_gfb011_promo_data_set_exchanges_reships',
    #     data_source_name='GFB011_PROMO_DATA_SET_EXCHANGES_RESHIPS',
    # )

    dos_104_merch_basket_analysis = SnowflakeProcedureOperator(
        procedure='gfb.dos_104_merch_basket_analysis.sql',
        database='reporting_prod',
        autocommit=False,
    )

    gfb006_vips_to_watch = SnowflakeProcedureOperator(
        procedure='gfb.gfb006_vips_to_watch.sql',
        database='reporting_prod',
        autocommit=False,
    )

    gfb014_01_bop_vip = SnowflakeProcedureOperator(
        procedure='gfb.gfb014_01_bop_vip.sql',
        database='reporting_prod',
        autocommit=False,
    )

    gfb057_passive_cancel_with_future_potential = SnowflakeProcedureOperator(
        procedure='gfb.gfb057_passive_cancel_with_future_potential.sql',
        database='reporting_prod',
        autocommit=False,
    )

    gfb014_attrition_data_set = SnowflakeProcedureOperator(
        procedure='gfb.gfb014_attrition_data_set.sql',
        database='reporting_prod',
        autocommit=False,
    )

    # tableau_refresh_gfb014_01_bop_vip = TableauRefreshOperator(
    #     task_id='tableau_refresh_gfb014_01_bop_vip',
    #     data_source_name='GFB014_01_BOP_VIP',
    # )
    #
    # tableau_refresh_gfb057_passive_cancel_with_future_potential = TableauRefreshOperator(
    #     task_id='tableau_refresh_gfb057_passive_cancel_with_future_potential',
    #     data_source_name='GFB057_PASSIVE_CANCEL_WITH_FUTURE_POTENTIAL',
    # )
    #
    # tableau_refresh_gfb014_attrition_data_set = TableauRefreshOperator(
    #     task_id='tableau_refresh_gfb014_attrition_data_set',
    #     data_source_name='GFB014_ATTRITION_DATA_SET',
    # )

    dos_083_clearance_reporting = SnowflakeProcedureOperator(
        procedure='gfb.dos_083_clearance_reporting.sql',
        database='reporting_prod',
        autocommit=False,
    )

    # tableau_refresh_dos_083_clearance_reporting = TableauRefreshOperator(
    #     task_id='tableau_refresh_dos_083_clearance_reporting',
    #     data_source_name='DOS_083_CLEARANCE_REPORTING',
    # )

    dos_083_01_clearance_reporting_shipped_date = SnowflakeProcedureOperator(
        procedure='gfb.dos_083_01_clearance_reporting_shipped_date.sql',
        database='reporting_prod',
        autocommit=False,
    )

    dos_114_ladder_plan = SnowflakeProcedureOperator(
        procedure='gfb.dos_114_ladder_plan.sql',
        database='reporting_prod',
        autocommit=False,
    )

    # tableau_refresh_dos_114_ladder_plan = TableauRefreshOperator(
    #     task_id='tableau_refresh_dos_114_ladder_plan',
    #     data_source_name='DOS_114_LADDER_PLAN',
    # )

    gfb009_chargeback_report = SnowflakeProcedureOperator(
        procedure='gfb.gfb009_chargeback_report.sql',
        database='reporting_prod',
        autocommit=False,
    )

    # tableau_refresh_gfb009_chargeback_report = TableauRefreshOperator(
    #     task_id='tableau_refresh_gfb009_chargeback_report',
    #     data_source_name='GFB009_CHARGEBACK_REPORT',
    # )

    dos_096_selling_by_size = SnowflakeProcedureOperator(
        procedure='gfb.dos_096_selling_by_size.sql',
        database='reporting_prod',
        autocommit=False,
    )

    # tableau_refresh_dos_096_selling_by_size = TableauRefreshOperator(
    #     task_id='tableau_refresh_dos_096_selling_by_size',
    #     data_source_name='DOS_096_SELLING_BY_SIZE',
    # )

    gfb015_out_of_stock_by_size = SnowflakeProcedureOperator(
        procedure='gfb.gfb015_out_of_stock_by_size.sql',
        database='reporting_prod',
        autocommit=False,
    )

    # tableau_refresh_gfb015_out_of_stock_by_size = TableauRefreshOperator(
    #     task_id='tableau_refresh_gfb015_out_of_stock_by_size',
    #     data_source_name='GFB015_OUT_OF_STOCK_BY_SIZE',
    # )

    gfb016_vip_universe = SnowflakeProcedureOperator(
        procedure='gfb.gfb016_vip_universe.sql',
        database='reporting_prod',
        autocommit=False,
    )

    gfb017_lead_universe = SnowflakeProcedureOperator(
        procedure='gfb.gfb017_lead_universe.sql',
        database='reporting_prod',
        autocommit=False,
    )

    # tableau_refresh_gfb016_vip_universe = TableauRefreshOperator(
    #     task_id='tableau_refresh_gfb016_vip_universe',
    #     data_source_name='GFB016_VIP_UNIVERSE',
    # )
    #
    # tableau_refresh_gfb017_lead_universe = TableauRefreshOperator(
    #     task_id='tableau_refresh_gfb017_lead_universe',
    #     data_source_name='GFB017_LEAD_UNIVERSE',
    # )

    gfb018_waitlist_data_set = SnowflakeProcedureOperator(
        procedure='gfb.gfb018_waitlist_data_set.sql',
        database='reporting_prod',
        autocommit=False,
    )

    # tableau_refresh_gfb018_waitlist_data_set = TableauRefreshOperator(
    #     task_id='tableau_refresh_gfb018_waitlist_data_set',
    #     data_source_name='GFB018_WAITLIST_DATA_SET',
    # )

    dos_080_open_to_buy = SnowflakeProcedureOperator(
        procedure='gfb.dos_080_open_to_buy.sql',
        database='reporting_prod',
        autocommit=False,
    )

    dos_080_01_open_to_buy_pending_inventory = SnowflakeProcedureOperator(
        procedure='gfb.dos_080_01_open_to_buy_pending_inventory.sql',
        database='reporting_prod',
        autocommit=False,
    )

    # tableau_refresh_dos_080_open_to_buy = TableauRefreshOperator(
    #     task_id='tableau_refresh_dos_080_open_to_buy',
    #     data_source_name='DOS_080_OPEN_TO_BUY',
    # )

    gfb001_customer_seg = SnowflakeProcedureOperator(
        procedure='gfb.gfb001_customer_seg.sql',
        database='reporting_prod',
        # warehouse='DA_WH_ETL_HEAVY',
        autocommit=False,
    )

    gfb001_customer_lifetime_value = SnowflakeProcedureOperator(
        procedure='gfb.gfb001_customer_lifetime_value.sql',
        database='reporting_prod',
        # warehouse='DA_WH_ETL_HEAVY',
        autocommit=False,
    )

    gfb001_01_basket_analysis_with_customer_seg = SnowflakeProcedureOperator(
        procedure='gfb.gfb001_01_basket_analysis_with_customer_seg.sql',
        database='reporting_prod',
        autocommit=False,
    )

    # tableau_refresh_gfb001_customer_lifetime_value = TableauRefreshOperator(
    #     task_id='tableau_refresh_gfb001_customer_lifetime_value',
    #     data_source_name='GFB001_CUSTOMER_LIFETIME_VALUE',
    # )
    #
    # tableau_refresh_gfb001_01_basket_analysis_with_customer_seg = TableauRefreshOperator(
    #     task_id='tableau_refresh_gfb001_01_basket_analysis_with_customer_seg',
    #     data_source_name='GFB001_01_BASKET_ANALYSIS_WITH_CUSTOMER_SEG',
    # )

    gfb019_product_review = SnowflakeProcedureOperator(
        procedure='gfb.gfb019_product_review.sql',
        database='reporting_prod',
        autocommit=False,
    )

    # tableau_refresh_gfb019_product_review = TableauRefreshOperator(
    #     task_id='tableau_refresh_gfb019_product_review',
    #     data_source_name='GFB019_PRODUCT_REVIEW',
    # )
    #
    # tableau_refresh_gfb019_01_product_review_summary = TableauRefreshOperator(
    #     task_id='tableau_refresh_gfb019_01_product_review_summary',
    #     data_source_name='GFB019_01_PRODUCT_REVIEW_SUMMARY',
    # )
    #
    # tableau_refresh_gfb019_02_product_return_reason = TableauRefreshOperator(
    #     task_id='tableau_refresh_gfb019_02_product_return_reason',
    #     data_source_name='GFB019_02_PRODUCT_RETURN_REASON',
    # )

    gfb074_token_breakdown = SnowflakeProcedureOperator(
        procedure='gfb.gfb074_token_breakdown.sql',
        database='reporting_prod',
        autocommit=False,
    )

    # tableau_refresh_gfb074_token_breakdown = TableauRefreshOperator(
    #     task_id='tableau_refresh_gfb074_token_breakdown',
    #     data_source_name='GFB074_TOKEN_BREAKDOWN',
    # )

    gfb012_payment_method = SnowflakeProcedureOperator(
        procedure='gfb.gfb012_payment_method.sql',
        database='reporting_prod',
        autocommit=False,
    )

    gfb012_01_failed_payment_method = SnowflakeProcedureOperator(
        procedure='gfb.gfb012_01_failed_payment_method.sql',
        database='reporting_prod',
        autocommit=False,
    )

    # tableau_refresh_gfb012_payment_method = TableauRefreshOperator(
    #     task_id='tableau_refresh_gfb012_payment_method',
    #     data_source_name='GFB012_PAYMENT_METHOD',
    # )
    #
    # tableau_refresh_gfb09_03_product_review_words = TableauRefreshOperator(
    #     task_id='tableau_refresh_gfb09_03_product_review_words',
    #     data_source_name='GFB019_03_PRODUCT_REVIEW_WORDS',
    # )

    gfb021_po_status_data_set = SnowflakeProcedureOperator(
        procedure='gfb.gfb021_po_status_data_set.sql',
        database='reporting_prod',
        autocommit=False,
    )

    # tableau_refresh_gfb021_po_status_data_set = TableauRefreshOperator(
    #     task_id='tableau_refresh_gfb021_po_status_data_set',
    #     data_source_name='GFB021_PO_STATUS_DATA_SET',
    # )

    gfb_asknicely_response_data_set = SnowflakeProcedureOperator(
        procedure='gfb.gfb_asknicely_response_data_set.sql',
        database='reporting_prod',
        autocommit=False,
    )

    gfb024_activating_promo_ltv = SnowflakeProcedureOperator(
        procedure='gfb.gfb024_activating_promo_ltv.sql',
        database='reporting_prod',
        autocommit=False,
    )

    # tableau_refresh_gfb024_activating_promo_ltv = TableauRefreshOperator(
    #     task_id='tableau_refresh_gfb024_activating_promo_ltv',
    #     data_source_name='GFB024_ACTIVATING_PROMO_LTV',
    # )

    dos_090_credit_billing_retries = SnowflakeProcedureOperator(
        procedure='gfb.dos_090_credit_billing_retries.sql',
        database='reporting_prod',
        autocommit=False,
    )

    # tableau_refresh_dos_090_credit_billing_retries = TableauRefreshOperator(
    #     task_id='tableau_refresh_dos_090_credit_billing_retries',
    #     data_source_name='VIEW_DOS_090_CREDIT_BILLING_RETRIES',
    # )

    dos_097_unredeemed_credits = SnowflakeProcedureOperator(
        procedure='gfb.dos_097_unredeemed_credits.sql',
        database='reporting_prod',
        autocommit=False,
    )

    # tableau_refresh_dos_097_unredeemed_credits = TableauRefreshOperator(
    #     task_id='tableau_refresh_dos_097_unredeemed_credits',
    #     data_source_name='VIEW_DOS_097_UNREDEEMED_CREDITS',
    # )

    gfb007_billing_circle_outcome = SnowflakeProcedureOperator(
        procedure='gfb.gfb007_billing_circle_outcome.sql',
        database='reporting_prod',
        autocommit=False,
    )

    gfb_dim_vip = SnowflakeProcedureOperator(
        procedure='gfb.gfb_dim_vip.sql',
        database='reporting_prod',
        # warehouse='DA_WH_ETL_HEAVY',
        autocommit=False,
    )

    gfb011_01_promo_code_order_data_set = SnowflakeProcedureOperator(
        procedure='gfb.gfb011_01_promo_code_order_data_set.sql',
        database='reporting_prod',
        autocommit=False,
    )

    gfb026_product_bundle_data_set = SnowflakeProcedureOperator(
        procedure='gfb.gfb026_product_bundle_data_set.sql',
        database='reporting_prod',
        autocommit=False,
    )

    # tableau_refresh_gfb026_product_bundle_data_set_place_date = TableauRefreshOperator(
    #     task_id='tableau_refresh_gfb026_product_bundle_data_set_place_date',
    #     data_source_name='GFB026_PRODUCT_BUNDLE_DATA_SET_PLACE_DATE',
    # )
    #
    # tableau_refresh_gfb026_product_bundle_data_set_shipped_date = TableauRefreshOperator(
    #     task_id='tableau_refresh_gfb026_product_bundle_data_set_shipped_date',
    #     data_source_name='GFB026_PRODUCT_BUNDLE_DATA_SET_SHIPPED_DATE',
    # )
    #
    # tableau_refresh_gfb026_01_fk_bundle_order_info = TableauRefreshOperator(
    #     task_id='tableau_refresh_gfb026_01_fk_bundle_order_info',
    #     data_source_name='GFB026_01_FK_BUNDLE_ORDER_INFO',
    # )

    gfb025_merch_master_style_log_alert = SnowflakeProcedureOperator(
        procedure='gfb.gfb025_merch_master_style_log_alert.sql',
        database='reporting_prod',
        autocommit=False,
    )

    # tableau_refresh_gfb011_01_promo_code_order_data_set = TableauRefreshOperator(
    #     task_id='tableau_refresh_gfb011_01_promo_code_order_data_set',
    #     data_source_name='GFB011_01_PROMO_CODE_ORDER_DATA_SET',
    # )

    gfb028_merch_basket_analysis = SnowflakeProcedureOperator(
        procedure='gfb.gfb028_merch_basket_analysis.sql',
        database='reporting_prod',
        autocommit=False,
    )

    # tableau_refresh_gfb028_merch_basket_analysis = TableauRefreshOperator(
    #     task_id='tableau_refresh_gfb028_merch_basket_analysis',
    #     data_source_name='GFB028_MERCH_BASKET_ANALYSIS',
    # )

    gfb029_customer_skip_analysis = SnowflakeProcedureOperator(
        procedure='gfb.gfb029_customer_skip_analysis.sql',
        database='reporting_prod',
        autocommit=False,
    )

    # tableau_refresh_gfb029_customer_skip_analysis = TableauRefreshOperator(
    #     task_id='tableau_refresh_gfb029_customer_skip_analysis',
    #     data_source_name='GFB029_CUSTOMER_SKIP_ANALYSIS',
    # )

    gfb030_orders_in_bucket_analysis = SnowflakeProcedureOperator(
        procedure='gfb.gfb030_orders_in_bucket_analysis.sql',
        database='reporting_prod',
        autocommit=False,
    )

    # tableau_refresh_gfb030_orders_in_bucket_analysis = TableauRefreshOperator(
    #     task_id='tableau_refresh_gfb030_orders_in_bucket_analysis',
    #     data_source_name='GFB030_ORDERS_IN_BUCKET_ANALYSIS',
    # )

    dim_web_merch_product = SnowflakeProcedureOperator(
        procedure='gfb.dim_web_merch_product.sql',
        database='reporting_prod',
        autocommit=False,
    )

    gfb031_product_promo_data_set = SnowflakeProcedureOperator(
        procedure='gfb.gfb031_product_promo_data_set.sql',
        database='reporting_prod',
        autocommit=False,
    )

    gfb031_02_bundle_promo_data_set = SnowflakeProcedureOperator(
        procedure='gfb.gfb031_02_bundle_promo_data_set.sql',
        database='reporting_prod',
        autocommit=False,
    )

    gfb031_01_product_promo_summary = SnowflakeProcedureOperator(
        procedure='gfb.gfb031_01_product_promo_summary.sql',
        database='reporting_prod',
        autocommit=False,
    )

    # tableau_refresh_gfb031_product_promo_data_set = TableauRefreshOperator(
    #     task_id='tableau_refresh_gfb031_product_promo_data_set',
    #     data_source_name='GFB031_PRODUCT_PROMO_DATA_SET',
    # )
    #
    # tableau_refresh_gfb031_01_product_promo_summary = TableauRefreshOperator(
    #     task_id='tableau_refresh_gfb031_01_product_promo_summary',
    #     data_source_name='GFB031_01_PRODUCT_PROMO_SUMMARY',
    # )
    #
    # tableau_refresh_gfb031_02_bundle_promo_data_set = TableauRefreshOperator(
    #     task_id='tableau_refresh_gfb031_02_bundle_promo_data_set',
    #     data_source_name='GFB031_02_BUNDLE_PROMO_DATA_SET',
    # )
    #
    # tableau_refresh_gfb031_02_jf_sd_bundle_promo_data_set = TableauRefreshOperator(
    #     task_id='tableau_refresh_gfb031_02_jf_sd_bundle_promo_data_set',
    #     data_source_name='GFB031_02_JF_SD_BUNDLE_PROMO_DATA_SET',
    # )

    gfb_promo_campaign_product_meta = SnowflakeProcedureOperator(
        procedure='gfb.gfb_promo_campaign_product_meta.sql',
        database='reporting_prod',
        autocommit=False,
    )

    gfb_loyalty_points_activity = SnowflakeProcedureOperator(
        procedure='gfb.gfb_loyalty_points_activity.sql',
        database='reporting_prod',
        autocommit=False,
    )

    # tableau_refresh_gfb037_02_loyalty_promo_redemption = TableauRefreshOperator(
    #     task_id='tableau_refresh_gfb037_02_loyalty_promo_redemption',
    #     data_source_name='GFB037_02_LOYALTY_PROMO_REDEMPTION',
    # )

    gfb034_jf_sd_bundle_data_set = SnowflakeProcedureOperator(
        procedure='gfb.gfb034_jf_sd_bundle_data_set.sql',
        database='reporting_prod',
        autocommit=False,
    )

    # tableau_refresh_gfb034_jf_sd_bundle_data_set = TableauRefreshOperator(
    #     task_id='tableau_refresh_gfb034_jf_sd_bundle_data_set',
    #     data_source_name='GFB034_JF_SD_BUNDLE_DATA_SET',
    # )
    #
    # tableau_refresh_gfb034_01_jf_sd_bundle_order_info = TableauRefreshOperator(
    #     task_id='tableau_refresh_gfb034_01_jf_sd_bundle_order_info',
    #     data_source_name='GFB034_01_JF_SD_BUNDLE_ORDER_INFO',
    # )

    gfb038_vip_reactivation = SnowflakeProcedureOperator(
        procedure='gfb.gfb038_vip_reactivation.sql',
        database='reporting_prod',
        autocommit=False,
    )

    # tableau_refresh_gfb038_vip_reactivation = TableauRefreshOperator(
    #     task_id='tableau_refresh_gfb038_vip_reactivation',
    #     data_source_name='GFB038_VIP_REACTIVATION',
    # )

    gfb_dim_bundle = SnowflakeProcedureOperator(
        procedure='gfb.gfb_dim_bundle.sql',
        database='reporting_prod',
        autocommit=False,
    )

    gfb023_active_vip_credit_data = SnowflakeProcedureOperator(
        procedure='gfb.gfb023_active_vip_credit_data.sql',
        database='reporting_prod',
        autocommit=False,
    )

    # tableau_refresh_gfb023_active_vip_credit_data = TableauRefreshOperator(
    #     task_id='tableau_refresh_gfb023_active_vip_credit_data',
    #     data_source_name='GFB023_ACTIVE_VIP_CREDIT_DATA',
    # )

    gfb044_holiday_data_set_hist_snow = SnowflakeProcedureOperator(
        procedure='gfb.gfb044_holiday_data_set_hist_snow.sql',
        database='reporting_prod',
        autocommit=False,
    )

    gfb015_01_out_of_stock_by_size_hist = SnowflakeProcedureOperator(
        procedure='gfb.gfb015_01_out_of_stock_by_size_hist.sql',
        database='reporting_prod',
        autocommit=False,
    )

    # tableau_refresh_gfb015_01_out_of_stock_by_size_hist = TableauRefreshOperator(
    #     task_id='tableau_refresh_gfb015_01_out_of_stock_by_size_hist',
    #     data_source_name='GFB015_01_OUT_OF_STOCK_BY_SIZE_HIST',
    # )

    gfb047_product_return_data_set = SnowflakeProcedureOperator(
        procedure='gfb.gfb047_product_return_data_set.sql',
        database='reporting_prod',
        autocommit=False,
    )

    # tableau_refresh_gfb047_product_return_data_set = TableauRefreshOperator(
    #     task_id='tableau_refresh_gfb047_product_return_data_set',
    #     data_source_name='GFB047_PRODUCT_RETURN_DATA_SET',
    # )

    gfb042_product_attribute_analysis = SnowflakeProcedureOperator(
        procedure='gfb.gfb042_product_attribute_analysis.sql',
        database='reporting_prod',
        autocommit=False,
    )

    # tableau_refresh_gfb042_product_attribute_analysis = TableauRefreshOperator(
    #     task_id='tableau_refresh_gfb042_product_attribute_analysis',
    #     data_source_name='GFB042_PRODUCT_ATTRIBUTE_ANALYSIS',
    # )

    gfb049_fk_site_jabber = SnowflakeProcedureOperator(
        procedure='gfb.gfb049_fk_site_jabber.sql',
        database='reporting_prod',
        autocommit=False,
    )

    gfb_membership_credit_activity = SnowflakeProcedureOperator(
        procedure='gfb.gfb_membership_credit_activity.sql',
        database='reporting_prod',
        autocommit=False,
    )

    gfb_sku_product_status = SnowflakeProcedureOperator(
        procedure='gfb.gfb_sku_product_status.sql',
        database='reporting_prod',
        autocommit=False,
    )

    gfb051_02_finance_forecast_credit_redemption = SnowflakeProcedureOperator(
        procedure='gfb.gfb051_02_finance_forecast_credit_redemption.sql',
        database='reporting_prod',
        autocommit=False,
    )

    gfb051_03_finance_forecast_credit_cancellation = SnowflakeProcedureOperator(
        procedure='gfb.gfb051_03_finance_forecast_credit_cancellation.sql',
        database='reporting_prod',
        autocommit=False,
    )

    gfb051_04_finance_forecast_daily_rate = SnowflakeProcedureOperator(
        procedure='gfb.gfb051_04_finance_forecast_daily_rate.sql',
        database='reporting_prod',
        autocommit=False,
    )

    gfb053_merch_purchase_by_tenure = SnowflakeProcedureOperator(
        procedure='gfb.gfb053_merch_purchase_by_tenure.sql',
        database='reporting_prod',
        autocommit=False,
    )

    # tableau_refresh_gfb053_merch_purchase_by_tenure = TableauRefreshOperator(
    #     task_id='tableau_refresh_gfb053_merch_purchase_by_tenure',
    #     data_source_name='GFB053_MERCH_PURCHASE_BY_TENURE',
    # )

    gfb056_showroom_scorecard = SnowflakeProcedureOperator(
        procedure='gfb.gfb056_showroom_scorecard.sql',
        database='reporting_prod',
        autocommit=False,
    )

    # tableau_refresh_gfb056_showroom_scorecard = TableauRefreshOperator(
    #     task_id='tableau_refresh_gfb056_showroom_scorecard',
    #     data_source_name='GFB056_SHOWROOM_SCORECARD',
    # )

    gfb001_03_tenure_block_lifetime_value = SnowflakeProcedureOperator(
        procedure='gfb.gfb001_03_tenure_block_lifetime_value.sql',
        database='reporting_prod',
        autocommit=False,
    )

    gfb001_03_tenure_block_lifetime_value_qa = SnowflakeProcedureOperator(
        procedure='gfb.gfb001_03_tenure_block_lifetime_value_qa.sql',
        database='reporting_prod',
        autocommit=False,
    )

    gfb048_ghosting_proposal_data_set = SnowflakeProcedureOperator(
        procedure='gfb.gfb048_ghosting_proposal_data_set.sql',
        database='reporting_prod',
        autocommit=False,
    )

    # tableau_refresh_gfb048_ghosting_proposal_data_set = TableauRefreshOperator(
    #     task_id='tableau_refresh_gfb048_ghosting_proposal_data_set',
    #     data_source_name='GFB048_GHOSTING_PROPOSAL_DATA_SET',
    # )
    #
    # tableau_refresh_gfb001_03_tenure_block_lifetime_value = TableauRefreshOperator(
    #     task_id='tableau_refresh_gfb001_03_tenure_block_lifetime_value',
    #     data_source_name='GFB001_03_TENURE_BLOCK_LIFETIME_VALUE',
    # )

    gfb060_jfb_quarterly_tracker = SnowflakeProcedureOperator(
        procedure='gfb.gfb060_jfb_quarterly_tracker.sql',
        database='reporting_prod',
        autocommit=False,
    )

    # tableau_refresh_gfb060_jfb_quarterly_tracker = TableauRefreshOperator(
    #     task_id='tableau_refresh_gfb060_jfb_quarterly_tracker',
    #     data_source_name='GFB060_JFB_QUARTERLY_TRACKER',
    # )

    gfb001_customer_seg_all_time = SnowflakeProcedureOperator(
        procedure='gfb.gfb001_customer_seg_all_time.sql',
        database='reporting_prod',
        autocommit=False,
    )

    gfb062_fk_showroom_recap = SnowflakeProcedureOperator(
        procedure='gfb.gfb062_fk_showroom_recap.sql',
        database='reporting_prod',
        autocommit=False,
    )

    # tableau_refresh_gfb062_fk_showroom_recap = TableauRefreshOperator(
    #     task_id='tableau_refresh_gfb062_fk_showroom_recap',
    #     data_source_name='GFB062_FK_SHOWROOM_RECAP',
    # )

    gfb070_daily_cash = SnowflakeProcedureOperator(
        procedure='gfb.gfb070_daily_cash.sql',
        database='reporting_prod',
        autocommit=False,
    )

    # tableau_refresh_gfb070_daily_cash = TableauRefreshOperator(
    #     task_id='tableau_refresh_gfb070_daily_cash',
    #     data_source_name='GFB070_DAILY_CASH',
    # )

    gfb069_fk_social_report = SnowflakeProcedureOperator(
        procedure='gfb.gfb069_fk_social_report.sql',
        database='reporting_prod',
        autocommit=False,
    )

    # tableau_refresh_gfb069_fk_social_report = TableauRefreshOperator(
    #     task_id='tableau_refresh_gfb069_fk_social_report',
    #     data_source_name='GFB069_FK_SOCIAL_REPORT',
    # )

    gift_with_purchase_data_set = SnowflakeProcedureOperator(
        procedure='gfb.gift_with_purchase_data_set.sql',
        database='reporting_prod',
        autocommit=False,
    )

    gwp_sub_brand_detail = SnowflakeProcedureOperator(
        procedure='gfb.gwp_sub_brand_detail.sql',
        database='reporting_prod',
        autocommit=False,
    )

    gfb_quiz_activity = SnowflakeProcedureOperator(
        procedure='gfb.gfb_quiz_activity.sql',
        database='reporting_prod',
        autocommit=False,
    )

    # tableau_refresh_gift_with_purchase_data_set = TableauRefreshOperator(
    #     task_id='tableau_refresh_gift_with_purchase_data_set',
    #     data_source_name='GIFT_WITH_PURCHASE_DATA_SET',
    # )
    #
    # tableau_refresh_gwp_sub_brand_detail = TableauRefreshOperator(
    #     task_id='tableau_refresh_gwp_sub_brand_detail',
    #     data_source_name='GWP_SUB_BRAND_DETAIL',
    # )

    pcvr_session_historical = SnowflakeProcedureOperator(
        procedure='gfb.pcvr_session_historical.sql',
        database='reporting_prod',
        autocommit=False,
    )

    pcvr_product_psource = SnowflakeProcedureOperator(
        procedure='gfb.pcvr_product_psource.sql',
        database='reporting_prod',
        autocommit=False,
    )

    gfb072_sales_by_credit = SnowflakeProcedureOperator(
        procedure='gfb.gfb072_sales_by_credit.sql',
        database='reporting_prod',
        autocommit=False,
    )

    # tableau_refresh_gfb072_sales_by_credit = TableauRefreshOperator(
    #     task_id='tableau_refresh_gfb072_sales_by_credit',
    #     data_source_name='GFB072_SALES_BY_CREDIT',
    # )

    gfb077_bundle_sales = SnowflakeProcedureOperator(
        procedure='gfb.gfb077_bundle_sales.sql',
        database='reporting_prod',
        autocommit=False,
    )

    gfb075_jfb_online_cancellations = SnowflakeProcedureOperator(
        procedure='gfb.gfb075_jfb_online_cancellations.sql',
        database='reporting_prod',
        autocommit=False,
    )

    # tableau_refresh_gfb075_jfb_online_cancellations = TableauRefreshOperator(
    #     task_id='tableau_refresh_gfb075_jfb_online_cancellations',
    #     data_source_name='GFB075_JFB_ONLINE_CANCELLATIONS',
    # )

    gfb078_shipping_return_cost_details = SnowflakeProcedureOperator(
        procedure='gfb.gfb078_shipping_return_cost_details.sql',
        database='reporting_prod',
        autocommit=False,
    )

    # tableau_refresh_gfb078_shipping_return_cost_details = TableauRefreshOperator(
    #     task_id='tableau_refresh_gfb078_shipping_return_cost_details',
    #     data_source_name='GFB078_SHIPPING_RETURN_COST_DETAILS',
    # )

    gfb081_active_skus = SnowflakeProcedureOperator(
        procedure='gfb.gfb081_active_skus.sql',
        database='reporting_prod',
        autocommit=False,
    )

    # tableau_refresh_gfb081_active_skus = TableauRefreshOperator(
    #     task_id='tableau_refresh_gfb081_active_skus',
    #     data_source_name='GFB081_ACTIVE_SKUS',
    # )

    gfb079_dp_order_combinations = SnowflakeProcedureOperator(
        procedure='gfb.gfb079_dp_order_combinations.sql',
        database='reporting_prod',
        autocommit=False,
    )

    # tableau_refresh_gfb079_dp_order_combinations = TableauRefreshOperator(
    #     task_id='tableau_refresh_gfb079_dp_order_combinations',
    #     data_source_name='GFB079_DP_ORDER_COMBINATIONS',
    # )

    gfb082_mm_returns_data = SnowflakeProcedureOperator(
        procedure='gfb.gfb082_mm_returns_data.sql',
        database='reporting_prod',
        autocommit=False,
    )

    gfb082_01_mm_return_rate = SnowflakeProcedureOperator(
        procedure='gfb.gfb082_01_mm_return_rate.sql',
        database='reporting_prod',
        autocommit=False,
    )

    # tableau_refresh_gfb082_mm_returns_data = TableauRefreshOperator(
    #     task_id='tableau_refresh_gfb082_mm_returns_data',
    #     data_source_name='GFB082_MM_RETURNS_DATA',
    # )
    #
    # tableau_refresh_gfb082_01_mm_return_rate = TableauRefreshOperator(
    #     task_id='tableau_refresh_gfb082_01_mm_return_rate',
    #     data_source_name='GFB082_01_MM_RETURN_RATE',
    # )

    gfb083_mm_invoice_reconciliation_dataset = SnowflakeProcedureOperator(
        procedure='gfb.gfb083_mm_invoice_reconciliation_dataset.sql',
        database='reporting_prod',
        autocommit=False,
    )

    # tableau_refresh_gfb083_mm_invoice_reconciliation_dataset = TableauRefreshOperator(
    #     task_id='tableau_refresh_gfb083_mm_invoice_reconciliation_dataset',
    #     data_source_name='GFB083_MM_INVOICE_RECONCILIATION_DATASET',
    # )

    gfb084_sales_by_credit_type_dataset = SnowflakeProcedureOperator(
        procedure='gfb.gfb084_sales_by_credit_type_dataset.sql',
        database='reporting_prod',
        autocommit=False,
    )

    tableau_refresh_gfb084_sales_by_credit_type_dataset = TableauRefreshOperator(
        task_id='tableau_refresh_gfb084_sales_by_credit_type_dataset',
        data_source_name='GFB084_SALES_BY_CREDIT_TYPE_DATASET',
    )

    third_party_brands_sales = SnowflakeProcedureOperator(
        procedure='gfb.third_party_brands_sales.sql',
        database='reporting_prod',
        autocommit=False,
    )

    # tableau_refresh_third_party_brands_sales = TableauRefreshOperator(
    #     task_id='tableau_refresh_third_party_brands_sales',
    #     data_source_name='third_party_brands_sales',
    # )

    # layer 1 joins
    join_branch_0 = EmptyOperator(task_id="join_branch_0", trigger_rule="none_failed")

    # layer 2 joins
    join_branch_layer2_01 = EmptyOperator(
        task_id="join_branch_layer2_01", trigger_rule="none_failed"
    )

    join_branch_layer2_02 = EmptyOperator(
        task_id="join_branch_layer2_02", trigger_rule="none_failed"
    )

    join_branch_layer2_03 = EmptyOperator(
        task_id="join_branch_layer2_03", trigger_rule="none_failed"
    )

    join_branch_layer2_04 = EmptyOperator(
        task_id="join_branch_layer2_04", trigger_rule="none_failed"
    )

    join_branch_layer2_05 = EmptyOperator(
        task_id="join_branch_layer2_05", trigger_rule="none_failed"
    )

    join_branch_layer2_06 = EmptyOperator(
        task_id="join_branch_layer2_06", trigger_rule="none_failed"
    )

    # layer 3 joins
    join_branch_layer3_01 = EmptyOperator(
        task_id="join_branch_layer3_01", trigger_rule="none_failed"
    )

    join_branch_layer3_02 = EmptyOperator(
        task_id="join_branch_layer3_02", trigger_rule="none_failed"
    )

    # layer 4 joins
    join_branch_layer4_01 = EmptyOperator(
        task_id="join_branch_layer4_01", trigger_rule="none_failed"
    )

    # layer 1
    [
        gfb_order_line_data_set,
        gfb_inventory_data_set >> merch_dim_product >> gfb_dim_bundle,
        gfb_po_data_set,
        dim_promo,
        gfb_crossover_customers_behaviour,
        sd_merge_acquisition_reporting,
        gfb_segment_pageviews,
        gfb_product_review_data_set,
        gfb_asknicely_response_data_set,
        gfb_dim_vip >> gfb_quiz_activity,
        dim_web_merch_product,
        pcvr_session_historical >> pcvr_product_psource,
        gfb_promo_campaign_product_meta,
        gfb_loyalty_points_activity,
        gfb_membership_credit_activity,
        gfb_sku_product_status,
    ] >> join_branch_0

    # layer 2
    join_branch_0 >> [
        gfb015_out_of_stock_by_size,
        gfb011_promo_data_set,
        dos_104_merch_basket_analysis,
        gfb006_vips_to_watch,
        gfb014_01_bop_vip,
        gfb057_passive_cancel_with_future_potential,
        gfb014_attrition_data_set,
        dos_083_clearance_reporting,
        dos_083_01_clearance_reporting_shipped_date,
        gfb009_chargeback_report,
        dos_096_selling_by_size,
        gfb016_vip_universe,
        gfb017_lead_universe,
        gfb018_waitlist_data_set,
        gfb019_product_review,
        gfb012_payment_method,
        gfb012_01_failed_payment_method,
        gfb021_po_status_data_set,
        dos_090_credit_billing_retries,
        dos_097_unredeemed_credits,
        gfb007_billing_circle_outcome,
        gfb026_product_bundle_data_set,
        gfb001_customer_seg,
        gfb025_merch_master_style_log_alert,
        gfb028_merch_basket_analysis,
        gfb029_customer_skip_analysis,
        gfb038_vip_reactivation,
        gfb023_active_vip_credit_data,
        gfb044_holiday_data_set_hist_snow,
        gfb015_01_out_of_stock_by_size_hist,
        gfb047_product_return_data_set,
        gfb048_ghosting_proposal_data_set,
        gfb049_fk_site_jabber,
        gfb051_04_finance_forecast_daily_rate,
        gfb053_merch_purchase_by_tenure,
        gfb001_03_tenure_block_lifetime_value,
        gfb001_03_tenure_block_lifetime_value_qa,
        gfb060_jfb_quarterly_tracker,
        gfb001_customer_seg_all_time,
        gfb070_daily_cash,
        gfb072_sales_by_credit,
        gift_with_purchase_data_set,
        gwp_sub_brand_detail,
        gfb077_bundle_sales,
        gfb083_mm_invoice_reconciliation_dataset,
        # tableau_refresh_gfb083_mm_invoice_reconciliation_dataset,
        gfb084_sales_by_credit_type_dataset,
        tableau_refresh_gfb084_sales_by_credit_type_dataset,
        third_party_brands_sales,
    ]

    # layer 3
    (
        [
            gfb015_out_of_stock_by_size,
            gfb048_ghosting_proposal_data_set,
        ]
        >> join_branch_layer2_06
        >> dos_107_merch_data_set
    )

    # (join_branch_layer2_06 >> gfb069_fk_social_report >> tableau_refresh_gfb069_fk_social_report)

    (
        gfb011_promo_data_set
        >> [
            gfb011_01_promo_code_order_data_set,
            gfb024_activating_promo_ltv,
        ]
        >> join_branch_layer2_04
    )

    [
        gfb014_01_bop_vip,
        gfb057_passive_cancel_with_future_potential,
        gfb014_attrition_data_set,
    ] >> join_branch_layer2_01

    # dos_083_clearance_reporting >> tableau_refresh_dos_083_clearance_reporting
    #
    # dos_096_selling_by_size >> tableau_refresh_dos_096_selling_by_size
    #
    # gfb015_out_of_stock_by_size >> tableau_refresh_gfb015_out_of_stock_by_size

    [
        gfb016_vip_universe,
        gfb017_lead_universe,
    ] >> join_branch_layer2_02

    # gfb018_waitlist_data_set >> tableau_refresh_gfb018_waitlist_data_set
    #
    # gfb019_product_review >> [
    #     tableau_refresh_gfb019_product_review,
    #     tableau_refresh_gfb019_01_product_review_summary,
    #     tableau_refresh_gfb019_02_product_return_reason,
    #     tableau_refresh_gfb09_03_product_review_words,
    # ]

    [
        gfb012_payment_method,
    ] >> join_branch_layer2_03

    # gfb009_chargeback_report >> tableau_refresh_gfb009_chargeback_report
    #
    # gfb021_po_status_data_set >> tableau_refresh_gfb021_po_status_data_set
    #
    # dos_090_credit_billing_retries >> tableau_refresh_dos_090_credit_billing_retries
    #
    # dos_097_unredeemed_credits >> tableau_refresh_dos_097_unredeemed_credits

    # gfb026_product_bundle_data_set >> [
    #     tableau_refresh_gfb026_product_bundle_data_set_place_date,
    #     tableau_refresh_gfb026_product_bundle_data_set_shipped_date,
    #     tableau_refresh_gfb026_01_fk_bundle_order_info,
    # ]

    gfb001_customer_seg >> [
        gfb001_customer_lifetime_value,
        gfb001_01_basket_analysis_with_customer_seg,
    ]

    # gfb028_merch_basket_analysis >> tableau_refresh_gfb028_merch_basket_analysis
    #
    # gfb029_customer_skip_analysis >> tableau_refresh_gfb029_customer_skip_analysis

    gfb011_promo_data_set >> gfb030_orders_in_bucket_analysis

    gfb011_promo_data_set >> [
        gfb031_product_promo_data_set,
        gfb031_01_product_promo_summary,
        gfb031_02_bundle_promo_data_set,
    ]

    gfb015_out_of_stock_by_size >> gfb034_jf_sd_bundle_data_set

    # gfb038_vip_reactivation >> tableau_refresh_gfb038_vip_reactivation
    #
    # gfb023_active_vip_credit_data >> tableau_refresh_gfb023_active_vip_credit_data

    # utlise gfb011_promo_data_set here if necessary

    # gfb015_01_out_of_stock_by_size_hist >> tableau_refresh_gfb015_01_out_of_stock_by_size_hist
    #
    # gfb047_product_return_data_set >> tableau_refresh_gfb047_product_return_data_set

    (
        gfb051_04_finance_forecast_daily_rate
        >> [
            gfb051_02_finance_forecast_credit_redemption,
            gfb051_03_finance_forecast_credit_cancellation,
        ]
        >> join_branch_layer2_05
    )

    # gfb048_ghosting_proposal_data_set >> tableau_refresh_gfb048_ghosting_proposal_data_set
    #
    # gfb053_merch_purchase_by_tenure >> tableau_refresh_gfb053_merch_purchase_by_tenure

    # [
    #     gfb001_03_tenure_block_lifetime_value,
    #     gfb001_03_tenure_block_lifetime_value_qa,
    # ] >> tableau_refresh_gfb001_03_tenure_block_lifetime_value
    #
    # gfb060_jfb_quarterly_tracker >> tableau_refresh_gfb060_jfb_quarterly_tracker
    #
    # gfb070_daily_cash >> tableau_refresh_gfb070_daily_cash
    #
    # gfb072_sales_by_credit >> tableau_refresh_gfb072_sales_by_credit
    # gift_with_purchase_data_set >> tableau_refresh_gift_with_purchase_data_set
    # gwp_sub_brand_detail >> tableau_refresh_gwp_sub_brand_detail
    # third_party_brands_sales >> tableau_refresh_third_party_brands_sales

    # layer 4
    dos_107_merch_data_set >> [
        # tableau_refresh_dos_107_merch_data_set_placed_date,
        # tableau_refresh_dos_107_merch_data_set_shipped_date,
        # tableau_refresh_dos_107_merch_dataset_daily_sales,
        # tableau_refresh_dos_107_merch_data_set_by_place_date_return_by_order_date,
        dos_114_ladder_plan,
        dos_080_open_to_buy,
        dos_080_01_open_to_buy_pending_inventory,
        gfb056_showroom_scorecard,
        gfb062_fk_showroom_recap,
        gfb074_token_breakdown,
    ]

    [
        gfb015_01_out_of_stock_by_size_hist,
        dos_107_merch_data_set,
    ] >> join_branch_layer3_02

    # join_branch_layer2_01 >> [
    #     tableau_refresh_gfb014_01_bop_vip,
    #     tableau_refresh_gfb057_passive_cancel_with_future_potential,
    #     tableau_refresh_gfb014_attrition_data_set,
    # ]
    #
    # join_branch_layer2_02 >> [
    #     tableau_refresh_gfb016_vip_universe,
    #     tableau_refresh_gfb017_lead_universe,
    # ]
    #
    # join_branch_layer2_03 >> [
    #     tableau_refresh_gfb012_payment_method,
    # ]
    #
    # join_branch_layer2_04 >> [
    #     tableau_refresh_gfb011_promo_data_set,
    #     tableau_refresh_gfb011_01_promo_code_order_data_set,
    #     tableau_refresh_gfb024_activating_promo_ltv,
    # ]

    [
        gfb001_customer_lifetime_value,
        gfb001_01_basket_analysis_with_customer_seg,
    ] >> join_branch_layer3_01

    # gfb030_orders_in_bucket_analysis >> tableau_refresh_gfb030_orders_in_bucket_analysis
    #
    # gfb031_product_promo_data_set >> tableau_refresh_gfb031_product_promo_data_set
    #
    # gfb031_01_product_promo_summary >> tableau_refresh_gfb031_01_product_promo_summary
    #
    # gfb031_02_bundle_promo_data_set >> [
    #     tableau_refresh_gfb031_02_bundle_promo_data_set,
    #     tableau_refresh_gfb031_02_jf_sd_bundle_promo_data_set,
    # ]
    #
    # gfb034_jf_sd_bundle_data_set >> [
    #     tableau_refresh_gfb034_jf_sd_bundle_data_set,
    #     tableau_refresh_gfb034_01_jf_sd_bundle_order_info,
    # ]

    # gfb042_product_attribute_analysis >> tableau_refresh_gfb042_product_attribute_analysis

    (join_branch_layer2_05)

    # layer 5
    join_branch_layer3_02 >> gfb042_product_attribute_analysis

    [
        dos_080_open_to_buy,
        dos_080_01_open_to_buy_pending_inventory,
    ] >> join_branch_layer4_01

    # join_branch_layer3_01 >> [
    #     tableau_refresh_gfb001_customer_lifetime_value,
    #     tableau_refresh_gfb001_01_basket_analysis_with_customer_seg,
    # ]

    # dos_114_ladder_plan >> tableau_refresh_dos_114_ladder_plan
    #
    # gfb056_showroom_scorecard >> tableau_refresh_gfb056_showroom_scorecard
    #
    # gfb062_fk_showroom_recap >> tableau_refresh_gfb062_fk_showroom_recap
    #
    # gfb074_token_breakdown >> tableau_refresh_gfb074_token_breakdown
    #
    # gfb075_jfb_online_cancellations >> tableau_refresh_gfb075_jfb_online_cancellations
    #
    # gfb078_shipping_return_cost_details >> tableau_refresh_gfb078_shipping_return_cost_details
    #
    # gfb079_dp_order_combinations >> tableau_refresh_gfb079_dp_order_combinations
    #
    # gfb081_active_skus >> tableau_refresh_gfb081_active_skus

    # (
    #     gfb011_promo_data_set_exchanges_reships
    #     >> tableau_refresh_gfb011_promo_data_set_exchanges_reships
    # )

    # (
    #     gfb082_mm_returns_data
    #     >> tableau_refresh_gfb082_mm_returns_data
    #     >> gfb082_01_mm_return_rate
    #     >> tableau_refresh_gfb082_01_mm_return_rate
    # )

    # layer 6
    # join_branch_layer4_01 >> tableau_refresh_dos_080_open_to_buy
