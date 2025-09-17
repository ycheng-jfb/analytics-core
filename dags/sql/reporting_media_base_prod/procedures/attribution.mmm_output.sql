-- USE ROLE tfg_media_admin;

CREATE OR REPLACE TRANSIENT TABLE reporting_media_base_prod.attribution.top_down_monitoring AS
(SELECT date, vip_conversions_exlc_aff_and_infl, predicted_vips, store_brand_name
 FROM reporting_media_base_prod.attribution.mmm_monitoring_fl_us
 UNION ALL
 (SELECT date, vip_conversions_exlc_aff_and_infl, predicted_vips, store_brand_name
  FROM reporting_media_base_prod.attribution.mmm_monitoring_flm_us)
 UNION ALL
 (SELECT date, vip_conversions_exlc_aff_and_infl, predicted_vips, store_brand_name
  FROM reporting_media_base_prod.attribution.top_down_monitoring_sx_us));

CREATE OR REPLACE TRANSIENT TABLE reporting_media_base_prod.attribution.top_down_monitoring_refreshed AS
(SELECT date, vip_conversions_exlc_aff_and_infl, predicted_vips, store_brand_name
 FROM reporting_media_base_prod.attribution.mmm_monitoring_refreshed_fl_us
 UNION ALL
 (SELECT date, vip_conversions_exlc_aff_and_infl, predicted_vips, store_brand_name
  FROM reporting_media_base_prod.attribution.mmm_monitoring_refreshed_flm_us)
 UNION ALL
 (SELECT date, vip_conversions_exlc_aff_and_infl, predicted_vips, store_brand_name
  FROM reporting_media_base_prod.attribution.top_down_monitoring_refreshed_sx_us));

CREATE OR REPLACE TRANSIENT TABLE reporting_media_base_prod.attribution.top_down_diminishing_returns AS
(SELECT date,
        feature,
        value,
        "attributed vips",
        "spend date vips",
        "carryover value",
        calculated_spend,
        spend,
        impression_threshold,
        cpm,
        last_updated,
        store_brand_name
 FROM reporting_media_base_prod.attribution.mmm_diminishing_returns_fl_us
 UNION ALL
 (SELECT date,
         feature,
         value,
         "attributed vips",
         "spend date vips",
         "carryover value",
         calculated_spend,
         spend,
         impression_threshold,
         cpm,
         last_updated,
         store_brand_name
  FROM reporting_media_base_prod.attribution.mmm_diminishing_returns_flm_us)
 UNION ALL
 (SELECT date,
         feature,
         value,
         "attributed vips",
         "spend date vips",
         "carryover value",
         calculated_spend,
         spend,
         impression_threshold,
         cpm,
         last_updated,
         store_brand_name
  FROM reporting_media_base_prod.attribution.top_down_diminishing_returns_sx_us));

CREATE OR REPLACE TRANSIENT TABLE reporting_media_base_prod.attribution.top_down_adstock AS
(SELECT "Day", "Adstock", "Feature", "Adstock_Alpha", last_updated, store_brand_name
 FROM reporting_media_base_prod.attribution.mmm_adstock_fl_us
 UNION ALL
 (SELECT  "Day", "Adstock", "Feature", "Adstock_Alpha", last_updated, store_brand_name
  FROM reporting_media_base_prod.attribution.mmm_adstock_flm_us)
 UNION ALL
 (SELECT  "Day", "Adstock", "Feature", "Adstock_Alpha", last_updated, store_brand_name
  FROM reporting_media_base_prod.attribution.top_down_adstock_sx_us));

CREATE OR REPLACE TRANSIENT TABLE reporting_media_base_prod.attribution.top_down_output_base AS
(SELECT date,
        vips,
        channel,
        feature_name,
        feature,
        value,
        feature_type,
        "carryover value",
        "spend date vips",
        spend,
        last_updated,
        store_brand_name
 FROM reporting_media_base_prod.attribution.top_down_output_base_fl_us
 UNION ALL
 (SELECT date,
         vips,
         channel,
         feature_name,
         feature,
         value,
         feature_type,
         "carryover value",
         "spend date vips",
         spend,
         last_updated,
         store_brand_name
  FROM reporting_media_base_prod.attribution.top_down_output_base_flm_us)
 UNION ALL
 (SELECT date,
         vips,
         channel,
         feature_name,
         feature,
         value,
         feature_type,
         "carryover value",
         "spend date vips",
         spend,
         last_updated,
         store_brand_name
  FROM reporting_media_base_prod.attribution.top_down_output_base_sx_us));

CREATE OR REPLACE TRANSIENT TABLE reporting_media_base_prod.attribution.top_down_output AS
(SELECT date, vips, channel, spend, feature, feature_name, value, last_updated, store_brand_name
 FROM reporting_media_base_prod.attribution.top_down_output_fl_us
 UNION ALL
 (SELECT date, vips, channel, spend, feature, feature_name, value, last_updated, store_brand_name
  FROM reporting_media_base_prod.attribution.top_down_output_flm_us)
 UNION ALL
 (SELECT date, vips, channel, spend, feature, feature_name, value, last_updated, store_brand_name
  FROM reporting_media_base_prod.attribution.top_down_output_sx_us));

CREATE OR REPLACE TRANSIENT TABLE reporting_media_base_prod.attribution.top_down_vip_dash AS
(SELECT channel,
        feature,
        feature_value,
        spend,
        vips,
        optimal_spend,
        optimal_vips,
        cps,
        cps_transform,
        spend_change,
        date,
        store_brand_name
 FROM reporting_media_base_prod.attribution.mmm_daily_planner_fl_us
 UNION ALL
 (SELECT channel,
         feature,
         feature_value,
         spend,
         vips,
         optimal_spend,
         optimal_vips,
         cps,
         cps_transform,
         spend_change,
         date,
         store_brand_name
  FROM reporting_media_base_prod.attribution.mmm_daily_planner_flm_us)
 UNION ALL
 (SELECT channel,
         feature,
         feature_value,
         spend,
         vips,
         optimal_spend,
         optimal_vips,
         cps,
         cps_transform,
         spend_change,
         date,
         store_brand_name
  FROM reporting_media_base_prod.attribution.top_down_vip_dash_sx_us));

CREATE OR REPLACE TRANSIENT TABLE reporting_media_base_prod.attribution.top_down_planner_monitoring AS
(SELECT date, store_brand_name, channel, spend, vips, optimal_spend, optimal_vips, spend_change
 FROM reporting_media_base_prod.attribution.mmm_planner_monitoring_fl_us
 UNION ALL
 (SELECT date, store_brand_name, channel, spend, vips, optimal_spend, optimal_vips, spend_change
  FROM reporting_media_base_prod.attribution.mmm_planner_monitoring_flm_us)
 UNION ALL
 (SELECT date, store_brand_name, channel, spend, vips, optimal_spend, optimal_vips, spend_change
  FROM reporting_media_base_prod.attribution.top_down_planner_monitoring_sx_us));


CREATE OR REPLACE TRANSIENT TABLE reporting_media_base_prod.attribution.mmm_outputs AS
(SELECT brand,
        date,
        final_grouping,
        clicks,
        credits,
        "impact score",
        impressions,
        opens,
        "pixel vips",
        sends,
        signal,
        spend,
        "spend incl credits",
        last_updated,
        feature_type,
        feature,
        modeled_metric,
        modeled_signal,
        "vip conversions exlc affiliate",
        modeled_signal_scaled,
        predicted_vips,
        total_daily_predicted_vips,
        total_daily_predicted_marketing_vips,
        channel_pct_of_predicted_credit,
        channel_pct_of_marketing_predicted_credit,
        attributed_vips_with_base,
        attributed_vips_without_base,
        vip_conversions
 FROM reporting_media_base_prod.attribution.mmm_outputs_fl_us
 UNION ALL
 (SELECT brand,
         date,
         final_grouping,
         clicks,
         credits,
         "impact score",
         impressions,
         opens,
         "pixel vips",
         sends,
         signal,
         spend,
         "spend incl credits",
         last_updated,
         feature_type,
         feature,
         modeled_metric,
         modeled_signal,
         "vip conversions exlc affiliate",
         modeled_signal_scaled,
         predicted_vips,
         total_daily_predicted_vips,
         total_daily_predicted_marketing_vips,
         channel_pct_of_predicted_credit,
         channel_pct_of_marketing_predicted_credit,
         attributed_vips_with_base,
         attributed_vips_without_base,
         vip_conversions
  FROM reporting_media_base_prod.attribution.mmm_outputs_flm_us)
 UNION ALL
 (SELECT brand,
         date,
         final_grouping,
         clicks,
         credits,
         "impact score",
         impressions,
         opens,
         "pixel vips",
         sends,
         signal,
         spend,
         "spend incl credits",
         last_updated,
         feature_type,
         feature,
         modeled_metric,
         modeled_signal,
         "vip conversions exlc affiliate",
         modeled_signal_scaled,
         predicted_vips,
         total_daily_predicted_vips,
         total_daily_predicted_marketing_vips,
         channel_pct_of_predicted_credit,
         channel_pct_of_marketing_predicted_credit,
         attributed_vips_with_base,
         attributed_vips_without_base,
         vip_conversions
  FROM reporting_media_base_prod.attribution.mmm_outputs_sx_us));

CREATE OR REPLACE TRANSIENT TABLE reporting_media_base_prod.attribution.mmm_correlations AS
(SELECT "Row",
        "Column",
        "Value",
        brand,
        correlation_start_date,
        correlation_end_date,
        "Row_sort",
        "Column_sort",
        "Row_type",
        "Column_type"
 FROM reporting_media_base_prod.attribution.mmm_correlations_fl_us
 UNION ALL
 (SELECT "Row",
         "Column",
         "Value",
         brand,
         correlation_start_date,
         correlation_end_date,
         "Row_sort",
         "Column_sort",
         "Row_type",
         "Column_type"
  FROM reporting_media_base_prod.attribution.mmm_correlations_flm_us)
 UNION ALL
 (SELECT "Row",
         "Column",
         "Value",
         brand,
         correlation_start_date,
         correlation_end_date,
         "Row_sort",
         "Column_sort",
         "Row_type",
         "Column_type"
  FROM reporting_media_base_prod.attribution.mmm_correlations_sx_us));
