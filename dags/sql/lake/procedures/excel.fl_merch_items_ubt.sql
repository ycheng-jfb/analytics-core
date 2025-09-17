BEGIN;

DELETE FROM lake_stg.excel.fl_merch_items_ubt_stg;


COPY INTO lake_stg.excel.fl_merch_items_ubt_stg (product_segment, style_sku, sku, gender, category, class, subclass, original_showroom_month, current_showroom, marketing_story, go_live_date, item_rank, design_style_number, current_name, color, color_family, buy_timing, item_status, style_status, sku_status, inseams_construction, end_use, eco_system, fit_block, color_application, eco_style, fabric, fit_style, lined_unlined, style_parent, size_scale, size_range, factory, editorial_features, us_msrp_dollar, us_vip_dollar, us_ttl_buy_units, us_ttl_blended_ldp_dollar, us_blended_vip_imu_percent, us_blended_ldp_dollar, us_ttl_msrp_dollar, us_ttl_vip_dollar, mx_msrp_dollar, mx_vip_dollar, mx_ttl_buy_units, mx_ttl_blended_ldp_dollar, mx_blended_vip_imu_percent, mx_blended_ldp_dollar, mx_ttl_msrp_dollar, mx_ttl_vip_dollar, eu_msrp_dollar, eu_vip_dollar, eu_ttl_buy_units, eu_ttl_blended_ldp_dollar, eu_blended_vip_imu_percent, eu_blended_ldp_dollar, eu_ttl_msrp_dollar, eu_ttl_vip_dollar, uk_msrp_dollar, uk_vip_dollar, uk_ttl_buy_units, uk_ttl_blended_ldp_dollar, uk_blended_vip_imu_percent, uk_blended_ldp_dollar, uk_ttl_msrp_dollar, uk_ttl_vip_dollar, rtl_msrp_dollar, rtl_vip_dollar, rtl_ttl_buy_units, rtl_ttl_blended_ldp_dollar, rtl_blended_vip_imu_percent, rtl_blended_ldp_dollar, rtl_ttl_msrp_dollar, rtl_ttl_vip_dollar, channel, global_total_units, na_total_units, na_blended_ldp, eu_plus_uk_total_units, eu_plus_uk_blended_ldp, sub_brand)
FROM (SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,$41,$42,$43,$44,$45,$46,$47,$48,$49,$50,$51,$52,$53,$54,$55,$56,$57,$58,$59,$60,$61,$62,$63,$64,$65,$66,$67,$68,$69,$70,$71,$72,$73,$74,$75,$76,$77,$78,$79,$80,'Fabletics'
      FROM '@lake_stg.public.tsos_da_int_inbound/lake/excel.fl_merch_items_ubt/v2/mens ubt revamp 2.2024-items_mens.csv.gz')
FILE_FORMAT=(
    TYPE = CSV,
    FIELD_DELIMITER = '|',
    RECORD_DELIMITER = '\n',
    FIELD_OPTIONALLY_ENCLOSED_BY = '"',
    SKIP_HEADER = 1,
    ESCAPE_UNENCLOSED_FIELD = NONE,
    NULL_IF = ('')
);

COPY INTO lake_stg.excel.fl_merch_items_ubt_stg (product_segment, style_sku, sku, gender, category, class, subclass, original_showroom_month, current_showroom, marketing_story, go_live_date, item_rank, design_style_number, current_name, color, color_family, buy_timing, item_status, style_status, sku_status, inseams_construction, end_use, eco_system, fit_block, color_application, eco_style, fabric, fit_style, lined_unlined, style_parent, size_scale, size_range, factory, editorial_features, us_msrp_dollar, us_vip_dollar, us_ttl_buy_units, us_ttl_blended_ldp_dollar, us_blended_vip_imu_percent, us_blended_ldp_dollar, us_ttl_msrp_dollar, us_ttl_vip_dollar, mx_msrp_dollar, mx_vip_dollar, mx_ttl_buy_units, mx_ttl_blended_ldp_dollar, mx_blended_vip_imu_percent, mx_blended_ldp_dollar, mx_ttl_msrp_dollar, mx_ttl_vip_dollar, eu_msrp_dollar, eu_vip_dollar, eu_ttl_buy_units, eu_ttl_blended_ldp_dollar, eu_blended_vip_imu_percent, eu_blended_ldp_dollar, eu_ttl_msrp_dollar, eu_ttl_vip_dollar, uk_msrp_dollar, uk_vip_dollar, uk_ttl_buy_units, uk_ttl_blended_ldp_dollar, uk_blended_vip_imu_percent, uk_blended_ldp_dollar, uk_ttl_msrp_dollar, uk_ttl_vip_dollar, rtl_msrp_dollar, rtl_vip_dollar, rtl_ttl_buy_units, rtl_ttl_blended_ldp_dollar, rtl_blended_vip_imu_percent, rtl_blended_ldp_dollar, rtl_ttl_msrp_dollar, rtl_ttl_vip_dollar, channel, global_total_units, na_total_units, na_blended_ldp, eu_plus_uk_total_units, eu_plus_uk_blended_ldp, style_color, tall_inseam, short_inseam, sub_brand)
FROM (SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,$41,$42,$43,$44,$45,$46,$47,$48,$49,$50,$51,$52,$53,$54,$55,$56,$57,$58,$59,$60,$61,$62,$63,$64,$65,$66,$67,$68,$69,$70,$71,$72,$73,$74,$75,$76,$77,$78,$79,$80,$81,$82,$83,'Fabletics'
      FROM '@lake_stg.public.tsos_da_int_inbound/lake/excel.fl_merch_items_ubt/v2/womens ubt revamp 2.2024-items_womens.csv.gz')
FILE_FORMAT=(
    TYPE = CSV,
    FIELD_DELIMITER = '|',
    RECORD_DELIMITER = '\n',
    FIELD_OPTIONALLY_ENCLOSED_BY = '"',
    SKIP_HEADER = 1,
    ESCAPE_UNENCLOSED_FIELD = NONE,
    NULL_IF = ('')
);

COPY INTO lake_stg.excel.fl_merch_items_ubt_stg (product_segment, style_sku, sku, gender, category, class, subclass, original_showroom_month, current_showroom, marketing_story, go_live_date, item_rank, design_style_number, current_name, color, color_family, buy_timing, item_status, style_status, sku_status, inseams_construction, end_use, eco_system, fit_block, color_application, eco_style, fabric, fit_style, lined_unlined, style_parent, size_scale, size_range, factory, editorial_features, us_msrp_dollar, us_vip_dollar, us_ttl_buy_units, us_ttl_blended_ldp_dollar, us_blended_vip_imu_percent, us_blended_ldp_dollar, us_ttl_msrp_dollar, us_ttl_vip_dollar, mx_msrp_dollar, mx_vip_dollar, mx_ttl_buy_units, mx_ttl_blended_ldp_dollar, mx_blended_vip_imu_percent, mx_blended_ldp_dollar, mx_ttl_msrp_dollar, mx_ttl_vip_dollar, eu_msrp_dollar, eu_vip_dollar, eu_ttl_buy_units, eu_ttl_blended_ldp_dollar, eu_blended_vip_imu_percent, eu_blended_ldp_dollar, eu_ttl_msrp_dollar, eu_ttl_vip_dollar, uk_msrp_dollar, uk_vip_dollar, uk_ttl_buy_units, uk_ttl_blended_ldp_dollar, uk_blended_vip_imu_percent, uk_blended_ldp_dollar, uk_ttl_msrp_dollar, uk_ttl_vip_dollar, rtl_msrp_dollar, rtl_vip_dollar, rtl_ttl_buy_units, rtl_ttl_blended_ldp_dollar, rtl_blended_vip_imu_percent, rtl_blended_ldp_dollar, rtl_ttl_msrp_dollar, rtl_ttl_vip_dollar, channel, global_total_units, na_total_units, na_blended_ldp, eu_plus_uk_total_units, eu_plus_uk_blended_ldp, sub_brand)
FROM (SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,$41,$42,$43,$44,$45,$46,$47,$48,$49,$50,$51,$52,$53,$54,$55,$56,$57,$58,$59,$60,$61,$62,$63,$64,$65,$66,$67,$68,$69,$70,$71,$72,$73,$74,$75,$76,$77,$78,$79,$80,'Yitty'
      FROM '@lake_stg.public.tsos_da_int_inbound/lake/excel.fl_merch_items_ubt/v2/shapewear ubt revamp 2.2024-items_shapewear.csv.gz')
FILE_FORMAT=(
    TYPE = CSV,
    FIELD_DELIMITER = '|',
    RECORD_DELIMITER = '\n',
    FIELD_OPTIONALLY_ENCLOSED_BY = '"',
    SKIP_HEADER = 1,
    ESCAPE_UNENCLOSED_FIELD = NONE,
    NULL_IF = ('')
);

COPY INTO lake_stg.excel.fl_merch_items_ubt_stg (product_segment, style_sku, sku, gender, category, class, subclass, original_showroom_month, current_showroom, marketing_story, go_live_date, item_rank, design_style_number, current_name, color, color_family, buy_timing, item_status, style_status, sku_status, inseams_construction, end_use, eco_system, fit_block, color_application, eco_style, fabric, fit_style, lined_unlined, style_parent, size_scale, size_range, factory, editorial_features, us_msrp_dollar, us_vip_dollar, us_ttl_buy_units, us_ttl_blended_ldp_dollar, us_blended_vip_imu_percent, us_blended_ldp_dollar, us_ttl_msrp_dollar, us_ttl_vip_dollar, mx_msrp_dollar, mx_vip_dollar, mx_ttl_buy_units, mx_ttl_blended_ldp_dollar, mx_blended_vip_imu_percent, mx_blended_ldp_dollar, mx_ttl_msrp_dollar, mx_ttl_vip_dollar, eu_msrp_dollar, eu_vip_dollar, eu_ttl_buy_units, eu_ttl_blended_ldp_dollar, eu_blended_vip_imu_percent, eu_blended_ldp_dollar, eu_ttl_msrp_dollar, eu_ttl_vip_dollar, uk_msrp_dollar, uk_vip_dollar, uk_ttl_buy_units, uk_ttl_blended_ldp_dollar, uk_blended_vip_imu_percent, uk_blended_ldp_dollar, uk_ttl_msrp_dollar, uk_ttl_vip_dollar, rtl_msrp_dollar, rtl_vip_dollar, rtl_ttl_buy_units, rtl_ttl_blended_ldp_dollar, rtl_blended_vip_imu_percent, rtl_blended_ldp_dollar, rtl_ttl_msrp_dollar, rtl_ttl_vip_dollar, channel, global_total_units, na_total_units, na_blended_ldp, eu_plus_uk_total_units, eu_plus_uk_blended_ldp, sub_brand)
FROM (SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,$41,$42,$43,$44,$45,$46,$47,$48,$49,$50,$51,$52,$53,$54,$55,$56,$57,$58,$59,$60,$61,$62,$63,$64,$65,$66,$67,$68,$69,$70,$71,$72,$73,$74,$75,$76,$77,$78,$79,$80,'Scrubs'
      FROM '@lake_stg.public.tsos_da_int_inbound/lake/excel.fl_merch_items_ubt/v2/scrubs ubt revamp 2.2024-items_scrubs.csv.gz')
FILE_FORMAT=(
    TYPE = CSV,
    FIELD_DELIMITER = '|',
    RECORD_DELIMITER = '\n',
    FIELD_OPTIONALLY_ENCLOSED_BY = '"',
    SKIP_HEADER = 1,
    ESCAPE_UNENCLOSED_FIELD = NONE,
    NULL_IF = ('')
);

DELETE FROM lake_stg.excel.fl_merch_items_ubt_stg
WHERE (COALESCE(TRIM(PRODUCT_SEGMENT),'') = '')
AND (COALESCE(TRIM(STYLE_SKU),'') = '')
AND (COALESCE(TRIM(SKU),'') = '')
AND (COALESCE(TRIM(GENDER),'') = '')
AND (COALESCE(TRIM(CATEGORY),'') = '')
AND (COALESCE(TRIM(CLASS),'') = '')
AND (COALESCE(TRIM(SUBCLASS),'') = '')
AND (COALESCE(TRIM(ORIGINAL_SHOWROOM_MONTH),'') = '')
AND (COALESCE(TRIM(CURRENT_SHOWROOM),'') = '')
AND (COALESCE(TRIM(MARKETING_STORY),'') = '')
AND (COALESCE(TRIM(GO_LIVE_DATE),'') = '')
AND (COALESCE(TRIM(ITEM_RANK),'') = '')
AND (COALESCE(TRIM(DESIGN_STYLE_NUMBER),'') = '')
AND (COALESCE(TRIM(CURRENT_NAME),'') = '')
AND (COALESCE(TRIM(COLOR),'') = '')
AND (COALESCE(TRIM(COLOR_FAMILY),'') = '')
AND (COALESCE(TRIM(BUY_TIMING),'') = '')
AND (COALESCE(TRIM(ITEM_STATUS),'') = '')
AND (COALESCE(TRIM(STYLE_STATUS),'') = '')
AND (COALESCE(TRIM(SKU_STATUS),'') = '')
AND (COALESCE(TRIM(INSEAMS_CONSTRUCTION),'') = '')
AND (COALESCE(TRIM(END_USE),'') = '')
AND (COALESCE(TRIM(ECO_SYSTEM),'') = '')
AND (COALESCE(TRIM(FIT_BLOCK),'') = '')
AND (COALESCE(TRIM(COLOR_APPLICATION),'') = '')
AND (COALESCE(TRIM(ECO_STYLE),'') = '')
AND (COALESCE(TRIM(FABRIC),'') = '')
AND (COALESCE(TRIM(FIT_STYLE),'') = '')
AND (COALESCE(TRIM(LINED_UNLINED),'') = '')
AND (COALESCE(TRIM(STYLE_PARENT),'') = '')
AND (COALESCE(TRIM(SIZE_SCALE),'') = '')
AND (COALESCE(TRIM(SIZE_RANGE),'') = '')
AND (COALESCE(TRIM(FACTORY),'') = '')
AND (COALESCE(TRIM(EDITORIAL_FEATURES),'') = '')
AND (COALESCE(TRIM(US_MSRP_DOLLAR),'') = '')
AND (COALESCE(TRIM(US_VIP_DOLLAR),'') = '')
AND (COALESCE(TRIM(US_TTL_BUY_UNITS),'') = '')
AND (COALESCE(TRIM(US_TTL_BLENDED_LDP_DOLLAR),'') = '')
AND (COALESCE(TRIM(US_BLENDED_VIP_IMU_PERCENT),'') = '')
AND (COALESCE(TRIM(US_BLENDED_LDP_DOLLAR),'') = '')
AND (COALESCE(TRIM(US_TTL_MSRP_DOLLAR),'') = '')
AND (COALESCE(TRIM(US_TTL_VIP_DOLLAR),'') = '')
AND (COALESCE(TRIM(MX_MSRP_DOLLAR),'') = '')
AND (COALESCE(TRIM(MX_VIP_DOLLAR),'') = '')
AND (COALESCE(TRIM(MX_TTL_BUY_UNITS),'') = '')
AND (COALESCE(TRIM(MX_TTL_BLENDED_LDP_DOLLAR),'') = '')
AND (COALESCE(TRIM(MX_BLENDED_VIP_IMU_PERCENT),'') = '')
AND (COALESCE(TRIM(MX_BLENDED_LDP_DOLLAR),'') = '')
AND (COALESCE(TRIM(MX_TTL_MSRP_DOLLAR),'') = '')
AND (COALESCE(TRIM(MX_TTL_VIP_DOLLAR),'') = '')
AND (COALESCE(TRIM(EU_MSRP_DOLLAR),'') = '')
AND (COALESCE(TRIM(EU_VIP_DOLLAR),'') = '')
AND (COALESCE(TRIM(EU_TTL_BUY_UNITS),'') = '')
AND (COALESCE(TRIM(EU_TTL_BLENDED_LDP_DOLLAR),'') = '')
AND (COALESCE(TRIM(EU_BLENDED_VIP_IMU_PERCENT),'') = '')
AND (COALESCE(TRIM(EU_BLENDED_LDP_DOLLAR),'') = '')
AND (COALESCE(TRIM(EU_TTL_MSRP_DOLLAR),'') = '')
AND (COALESCE(TRIM(EU_TTL_VIP_DOLLAR),'') = '')
AND (COALESCE(TRIM(UK_MSRP_DOLLAR),'') = '')
AND (COALESCE(TRIM(UK_VIP_DOLLAR),'') = '')
AND (COALESCE(TRIM(UK_TTL_BUY_UNITS),'') = '')
AND (COALESCE(TRIM(UK_TTL_BLENDED_LDP_DOLLAR),'') = '')
AND (COALESCE(TRIM(UK_BLENDED_VIP_IMU_PERCENT),'') = '')
AND (COALESCE(TRIM(UK_BLENDED_LDP_DOLLAR),'') = '')
AND (COALESCE(TRIM(UK_TTL_MSRP_DOLLAR),'') = '')
AND (COALESCE(TRIM(UK_TTL_VIP_DOLLAR),'') = '')
AND (COALESCE(TRIM(RTL_MSRP_DOLLAR),'') = '')
AND (COALESCE(TRIM(RTL_VIP_DOLLAR),'') = '')
AND (COALESCE(TRIM(RTL_TTL_BUY_UNITS),'') = '')
AND (COALESCE(TRIM(RTL_TTL_BLENDED_LDP_DOLLAR),'') = '')
AND (COALESCE(TRIM(RTL_BLENDED_VIP_IMU_PERCENT),'') = '')
AND (COALESCE(TRIM(RTL_BLENDED_LDP_DOLLAR),'') = '')
AND (COALESCE(TRIM(RTL_TTL_MSRP_DOLLAR),'') = '')
AND (COALESCE(TRIM(RTL_TTL_VIP_DOLLAR),'') = '')
AND (COALESCE(TRIM(CHANNEL),'') = '')
AND (COALESCE(TRIM(GLOBAL_TOTAL_UNITS),'') = '')
AND (COALESCE(TRIM(NA_TOTAL_UNITS),'') = '')
AND (COALESCE(TRIM(NA_BLENDED_LDP),'') = '')
AND (COALESCE(TRIM(EU_PLUS_UK_TOTAL_UNITS),'') = '')
AND (COALESCE(TRIM(EU_PLUS_UK_BLENDED_LDP),'') = '')
AND (COALESCE(TRIM(STYLE_COLOR),'') = '')
AND (COALESCE(TRIM(TALL_INSEAM),'') = '')
AND (COALESCE(TRIM(SHORT_INSEAM),'') = '');

MERGE INTO lake.excel.fl_merch_items_ubt t
USING (
    SELECT
        a.*
    FROM (
        SELECT
            product_segment,
            style_sku,
            sku,
            gender,
            category,
            class,
            subclass,
            original_showroom_month,
            current_showroom,
            marketing_story,
            go_live_date,
            item_rank,
            design_style_number,
            current_name,
            color,
            color_family,
            buy_timing,
            item_status,
            style_status,
            sku_status,
            inseams_construction,
            end_use,
            eco_system,
            fit_block,
            color_application,
            eco_style,
            fabric,
            fit_style,
            lined_unlined,
            style_parent,
            size_scale,
            size_range,
            factory,
            editorial_features,
            us_msrp_dollar,
            us_vip_dollar,
            us_ttl_buy_units,
            us_ttl_blended_ldp_dollar,
            us_blended_vip_imu_percent * 100 AS us_blended_vip_imu_percent,
            us_blended_ldp_dollar,
            us_ttl_msrp_dollar,
            us_ttl_vip_dollar,
            mx_msrp_dollar,
            mx_vip_dollar,
            mx_ttl_buy_units,
            mx_ttl_blended_ldp_dollar,
            mx_blended_vip_imu_percent * 100 AS mx_blended_vip_imu_percent,
            mx_blended_ldp_dollar,
            mx_ttl_msrp_dollar,
            mx_ttl_vip_dollar,
            eu_msrp_dollar,
            eu_vip_dollar,
            eu_ttl_buy_units,
            eu_ttl_blended_ldp_dollar,
            eu_blended_vip_imu_percent * 100 AS eu_blended_vip_imu_percent,
            eu_blended_ldp_dollar,
            eu_ttl_msrp_dollar,
            eu_ttl_vip_dollar,
            uk_msrp_dollar,
            uk_vip_dollar,
            uk_ttl_buy_units,
            uk_ttl_blended_ldp_dollar,
            uk_blended_vip_imu_percent * 100 AS uk_blended_vip_imu_percent,
            uk_blended_ldp_dollar,
            uk_ttl_msrp_dollar,
            uk_ttl_vip_dollar,
            rtl_msrp_dollar,
            rtl_vip_dollar,
            rtl_ttl_buy_units,
            rtl_ttl_blended_ldp_dollar,
            rtl_blended_vip_imu_percent * 100 AS rtl_blended_vip_imu_percent,
            rtl_blended_ldp_dollar,
            rtl_ttl_msrp_dollar,
            rtl_ttl_vip_dollar,
            channel,
            global_total_units,
            na_total_units,
            na_blended_ldp,
            eu_plus_uk_total_units,
            eu_plus_uk_blended_ldp,
            style_color,
            tall_inseam,
            short_inseam,
            CONCAT('https://fabletics-us-cdn.justfab.com/media/images/products/' || ubt.sku || '/' || ubt.sku ||'-1_271x407.jpg') AS image_url,
            sub_brand AS sub_brand,
            hash(*) AS meta_row_hash,
            current_timestamp AS meta_create_datetime,
            current_timestamp AS meta_update_datetime,
            row_number() OVER ( PARTITION BY sku, current_showroom ORDER BY coalesce(current_showroom, '1900-01-01') DESC ) AS rn
        FROM lake_stg.excel.fl_merch_items_ubt_stg ubt
     ) a
    WHERE a.rn = 1
) s ON equal_null(t.sku, s.sku)
    AND equal_null(t.current_showroom, s.current_showroom)
WHEN NOT MATCHED THEN INSERT (
    product_segment, style_sku, sku, gender, category, class, subclass, original_showroom_month, current_showroom, marketing_story, go_live_date, item_rank, design_style_number, current_name, color, color_family, buy_timing, item_status, style_status, sku_status, inseams_construction, end_use, eco_system, fit_block, color_application, eco_style, fabric, fit_style, lined_unlined, style_parent, size_scale, size_range, factory, editorial_features, us_msrp_dollar, us_vip_dollar, us_ttl_buy_units, us_ttl_blended_ldp_dollar, us_blended_vip_imu_percent, us_blended_ldp_dollar, us_ttl_msrp_dollar, us_ttl_vip_dollar, mx_msrp_dollar, mx_vip_dollar, mx_ttl_buy_units, mx_ttl_blended_ldp_dollar, mx_blended_vip_imu_percent, mx_blended_ldp_dollar, mx_ttl_msrp_dollar, mx_ttl_vip_dollar, eu_msrp_dollar, eu_vip_dollar, eu_ttl_buy_units, eu_ttl_blended_ldp_dollar, eu_blended_vip_imu_percent, eu_blended_ldp_dollar, eu_ttl_msrp_dollar, eu_ttl_vip_dollar, uk_msrp_dollar, uk_vip_dollar, uk_ttl_buy_units, uk_ttl_blended_ldp_dollar, uk_blended_vip_imu_percent, uk_blended_ldp_dollar, uk_ttl_msrp_dollar, uk_ttl_vip_dollar, rtl_msrp_dollar, rtl_vip_dollar, rtl_ttl_buy_units, rtl_ttl_blended_ldp_dollar, rtl_blended_vip_imu_percent, rtl_blended_ldp_dollar, rtl_ttl_msrp_dollar, rtl_ttl_vip_dollar, channel, global_total_units, na_total_units, na_blended_ldp, eu_plus_uk_total_units, eu_plus_uk_blended_ldp, style_color, tall_inseam, short_inseam, image_url, sub_brand, meta_row_hash, meta_create_datetime, meta_update_datetime
)
VALUES (
    product_segment, style_sku, sku, gender, category, class, subclass, original_showroom_month, current_showroom, marketing_story, go_live_date, item_rank, design_style_number, current_name, color, color_family, buy_timing, item_status, style_status, sku_status, inseams_construction, end_use, eco_system, fit_block, color_application, eco_style, fabric, fit_style, lined_unlined, style_parent, size_scale, size_range, factory, editorial_features, us_msrp_dollar, us_vip_dollar, us_ttl_buy_units, us_ttl_blended_ldp_dollar, us_blended_vip_imu_percent, us_blended_ldp_dollar, us_ttl_msrp_dollar, us_ttl_vip_dollar, mx_msrp_dollar, mx_vip_dollar, mx_ttl_buy_units, mx_ttl_blended_ldp_dollar, mx_blended_vip_imu_percent, mx_blended_ldp_dollar, mx_ttl_msrp_dollar, mx_ttl_vip_dollar, eu_msrp_dollar, eu_vip_dollar, eu_ttl_buy_units, eu_ttl_blended_ldp_dollar, eu_blended_vip_imu_percent, eu_blended_ldp_dollar, eu_ttl_msrp_dollar, eu_ttl_vip_dollar, uk_msrp_dollar, uk_vip_dollar, uk_ttl_buy_units, uk_ttl_blended_ldp_dollar, uk_blended_vip_imu_percent, uk_blended_ldp_dollar, uk_ttl_msrp_dollar, uk_ttl_vip_dollar, rtl_msrp_dollar, rtl_vip_dollar, rtl_ttl_buy_units, rtl_ttl_blended_ldp_dollar, rtl_blended_vip_imu_percent, rtl_blended_ldp_dollar, rtl_ttl_msrp_dollar, rtl_ttl_vip_dollar, channel, global_total_units, na_total_units, na_blended_ldp, eu_plus_uk_total_units, eu_plus_uk_blended_ldp, style_color, tall_inseam, short_inseam, image_url, sub_brand, meta_row_hash, meta_create_datetime, meta_update_datetime
)
WHEN MATCHED AND t.meta_row_hash != s.meta_row_hash
 THEN UPDATE
SET t.product_segment = s.product_segment,
    t.style_sku = s.style_sku,
    t.sku = s.sku,
    t.gender = s.gender,
    t.category = s.category,
    t.class = s.class,
    t.subclass = s.subclass,
    t.original_showroom_month = s.original_showroom_month,
    t.current_showroom = s.current_showroom,
    t.marketing_story = s.marketing_story,
    t.go_live_date = s.go_live_date,
    t.item_rank = s.item_rank,
    t.design_style_number = s.design_style_number,
    t.current_name = s.current_name,
    t.color = s.color,
    t.color_family = s.color_family,
    t.buy_timing = s.buy_timing,
    t.item_status = s.item_status,
    t.style_status = s.style_status,
    t.sku_status = s.sku_status,
    t.inseams_construction = s.inseams_construction,
    t.end_use = s.end_use,
    t.eco_system = s.eco_system,
    t.fit_block = s.fit_block,
    t.color_application = s.color_application,
    t.eco_style = s.eco_style,
    t.fabric = s.fabric,
    t.fit_style = s.fit_style,
    t.lined_unlined = s.lined_unlined,
    t.style_parent = s.style_parent,
    t.size_scale = s.size_scale,
    t.size_range = s.size_range,
    t.factory = s.factory,
    t.editorial_features = s.editorial_features,
    t.us_msrp_dollar = s.us_msrp_dollar,
    t.us_vip_dollar = s.us_vip_dollar,
    t.us_ttl_buy_units = s.us_ttl_buy_units,
    t.us_ttl_blended_ldp_dollar = s.us_ttl_blended_ldp_dollar,
    t.us_blended_vip_imu_percent = s.us_blended_vip_imu_percent,
    t.us_blended_ldp_dollar = s.us_blended_ldp_dollar,
    t.us_ttl_msrp_dollar = s.us_ttl_msrp_dollar,
    t.us_ttl_vip_dollar = s.us_ttl_vip_dollar,
    t.mx_msrp_dollar = s.mx_msrp_dollar,
    t.mx_vip_dollar = s.mx_vip_dollar,
    t.mx_ttl_buy_units = s.mx_ttl_buy_units,
    t.mx_ttl_blended_ldp_dollar = s.mx_ttl_blended_ldp_dollar,
    t.mx_blended_vip_imu_percent = s.mx_blended_vip_imu_percent,
    t.mx_blended_ldp_dollar = s.mx_blended_ldp_dollar,
    t.mx_ttl_msrp_dollar = s.mx_ttl_msrp_dollar,
    t.mx_ttl_vip_dollar = s.mx_ttl_vip_dollar,
    t.eu_msrp_dollar = s.eu_msrp_dollar,
    t.eu_vip_dollar = s.eu_vip_dollar,
    t.eu_ttl_buy_units = s.eu_ttl_buy_units,
    t.eu_ttl_blended_ldp_dollar = s.eu_ttl_blended_ldp_dollar,
    t.eu_blended_vip_imu_percent = s.eu_blended_vip_imu_percent,
    t.eu_blended_ldp_dollar = s.eu_blended_ldp_dollar,
    t.eu_ttl_msrp_dollar = s.eu_ttl_msrp_dollar,
    t.eu_ttl_vip_dollar = s.eu_ttl_vip_dollar,
    t.uk_msrp_dollar = s.uk_msrp_dollar,
    t.uk_vip_dollar = s.uk_vip_dollar,
    t.uk_ttl_buy_units = s.uk_ttl_buy_units,
    t.uk_ttl_blended_ldp_dollar = s.uk_ttl_blended_ldp_dollar,
    t.uk_blended_vip_imu_percent = s.uk_blended_vip_imu_percent,
    t.uk_blended_ldp_dollar = s.uk_blended_ldp_dollar,
    t.uk_ttl_msrp_dollar = s.uk_ttl_msrp_dollar,
    t.uk_ttl_vip_dollar = s.uk_ttl_vip_dollar,
    t.rtl_msrp_dollar = s.rtl_msrp_dollar,
    t.rtl_vip_dollar = s.rtl_vip_dollar,
    t.rtl_ttl_buy_units = s.rtl_ttl_buy_units,
    t.rtl_ttl_blended_ldp_dollar = s.rtl_ttl_blended_ldp_dollar,
    t.rtl_blended_vip_imu_percent = s.rtl_blended_vip_imu_percent,
    t.rtl_blended_ldp_dollar = s.rtl_blended_ldp_dollar,
    t.rtl_ttl_msrp_dollar = s.rtl_ttl_msrp_dollar,
    t.rtl_ttl_vip_dollar = s.rtl_ttl_vip_dollar,
    t.channel = s.channel,
    t.global_total_units = s.global_total_units,
    t.na_total_units = s.na_total_units,
    t.na_blended_ldp = s.na_blended_ldp,
    t.eu_plus_uk_total_units = s.eu_plus_uk_total_units,
    t.eu_plus_uk_blended_ldp = s.eu_plus_uk_blended_ldp,
    t.style_color = s.style_color,
    t.tall_inseam = s.tall_inseam,
    t.short_inseam = s.short_inseam,
    t.image_url = s.image_url,
    t.sub_brand = s.sub_brand,
    t.meta_row_hash = s.meta_row_hash,
    t.meta_update_datetime = s.meta_update_datetime;

DELETE FROM lake.excel.fl_merch_items_ubt t
WHERE NOT EXISTS (
    SELECT 1
    FROM lake_stg.excel.fl_merch_items_ubt_stg s
    WHERE equal_null(t.sku, s.sku)
        AND equal_null(t.current_showroom, s.current_showroom)
);

COMMIT;
