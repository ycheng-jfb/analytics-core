BEGIN;

DELETE FROM lake_stg.excel.fl_merch_outfits_ubt_hierarchy_stg;


COPY INTO lake_stg.excel.fl_merch_outfits_ubt_hierarchy_stg (product_segment, gender, outfit_number, showroom_month, go_live_date, number_of_outfit_components, us_outfit_vip_dollar, eu_uk_vipdollar, initial_launch, additional_information, outfit_complexion, sku_number_1, description_1_current_name, style_color_1, sku_number_2, description_2_current_name, style_color_2, sku_number_3_reg_inseam, sku_number_3_short_inseam, sku_number_3_tall_inseam, inseam, description_3_current_name, style_color_3, sku_number_4, description_4_current_name, style_color_4, sku_number_5, description_5_current_name, style_color_5, sku_number_6, description_6_current_name, style_color_6, sku_number_7, description_7_current_name, style_color_7, sku_number_8, description_8_current_name, style_color_8, outfit_msrp_dollar, comp_number_1_msrp, comp_number_2_msrp, comp_number_3_msrp, comp_number_4_msrp, comp_number_5_msrp, comp_number_6_msrp, comp_number_7_msrp, comp_number_8_msrp, us_imu, us_outfit_cost_dollar, us_top_cost_dollar, us_bra_jacket_cost_dollar, us_bottom_cost_dollar, us_cost_comp_number_4, us_cost_comp_number_5, us_cost_comp_number_6, us_cost_comp_number_7, us_cost_comp_number_8, mx_imu, mx_outfit_cost_dollar, mx_top_cost_dollar, mx_bra_jacket_cost_dollar, mx_bottom_cost_dollar, mx_cost_comp_number_4, mx_cost_comp_number_5, mx_cost_comp_number_6, mx_cost_comp_number_7, mx_cost_comp_number_8, eu_imu, eu_outfit_cost_dollar, eu_top_cost_dollar, eu_bra_jacket_cost_dollar, eu_bottom_cost_dollar, eu_cost_comp_number_4, eu_cost_comp_number_5, eu_cost_comp_number_6, eu_cost_comp_number_7, eu_cost_comp_number_8, uk_imu, uk_outfit_cost_dollar, uk_top_cost_dollar, uk_bra_jacket_cost_dollar, uk_bottom_cost_dollar, uk_cost_comp_number_4, uk_cost_comp_number_5, uk_cost_comp_number_6, uk_cost_comp_number_7, uk_cost_comp_number_8, rtl_imu, rtl_outfit_cost_dollar, rtl_top_cost_dollar, rtl_bra_jacket_cost_dollar, rtl_bottom_cost_dollar, rtl_cost_comp_number_4, rtl_cost_comp_number_5, rtl_cost_comp_number_6, rtl_cost_comp_number_7, rtl_cost_comp_number_8, sub_brand)
FROM (SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,$41,$42,$43,$44,$45,$46,$47,$48,$49,$50,$51,$52,$53,$54,$55,$56,$57,$58,$59,$60,$61,$62,$63,$64,$65,$66,$67,$68,$69,$70,$71,$72,$73,$74,$75,$76,$77,$78,$79,$80,$81,$82,$83,$84,$85,$86,$87,$88,$89,$90,$91,$92,$93,$94,$95,$96,$97,'Fabletics'
      FROM '@lake_stg.public.tsos_da_int_inbound/lake/excel.fl_merch_outfits_ubt_hierarchy/v2/mens ubt hierarchy revamp nov 24-outfits_mens.csv.gz')
FILE_FORMAT=(
    TYPE = CSV,
    FIELD_DELIMITER = '|',
    RECORD_DELIMITER = '\n',
    FIELD_OPTIONALLY_ENCLOSED_BY = '"',
    SKIP_HEADER = 1,
    ESCAPE_UNENCLOSED_FIELD = NONE,
    NULL_IF = ('')
);


COPY INTO lake_stg.excel.fl_merch_outfits_ubt_hierarchy_stg (product_segment, gender, outfit_number, showroom_month, go_live_date, number_of_outfit_components, us_outfit_vip_dollar, eu_uk_vipdollar, initial_launch, additional_information, outfit_complexion, sku_number_1, description_1_current_name, style_color_1, sku_number_2, description_2_current_name, style_color_2, sku_number_3_reg_inseam, sku_number_3_short_inseam, sku_number_3_tall_inseam, inseam, description_3_current_name, style_color_3, sku_number_4, description_4_current_name, style_color_4, sku_number_5, description_5_current_name, style_color_5, sku_number_6, description_6_current_name, style_color_6, sku_number_7, description_7_current_name, style_color_7, sku_number_8, description_8_current_name, style_color_8, outfit_msrp_dollar, comp_number_1_msrp, comp_number_2_msrp, comp_number_3_msrp, comp_number_4_msrp, comp_number_5_msrp, comp_number_6_msrp, comp_number_7_msrp, comp_number_8_msrp, us_imu, us_outfit_cost_dollar, us_top_cost_dollar, us_bra_jacket_cost_dollar, us_bottom_cost_dollar, us_cost_comp_number_4, us_cost_comp_number_5, us_cost_comp_number_6, us_cost_comp_number_7, us_cost_comp_number_8, mx_imu, mx_outfit_cost_dollar, mx_top_cost_dollar, mx_bra_jacket_cost_dollar, mx_bottom_cost_dollar, mx_cost_comp_number_4, mx_cost_comp_number_5, mx_cost_comp_number_6, mx_cost_comp_number_7, mx_cost_comp_number_8, eu_imu, eu_outfit_cost_dollar, eu_top_cost_dollar, eu_bra_jacket_cost_dollar, eu_bottom_cost_dollar, eu_cost_comp_number_4, eu_cost_comp_number_5, eu_cost_comp_number_6, eu_cost_comp_number_7, eu_cost_comp_number_8, uk_imu, uk_outfit_cost_dollar, uk_top_cost_dollar, uk_bra_jacket_cost_dollar, uk_bottom_cost_dollar, uk_cost_comp_number_4, uk_cost_comp_number_5, uk_cost_comp_number_6, uk_cost_comp_number_7, uk_cost_comp_number_8, rtl_imu, rtl_outfit_cost_dollar, rtl_top_cost_dollar, rtl_bra_jacket_cost_dollar, rtl_bottom_cost_dollar, rtl_cost_comp_number_4, rtl_cost_comp_number_5, rtl_cost_comp_number_6, rtl_cost_comp_number_7, rtl_cost_comp_number_8, sub_brand)
FROM (SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,$41,$42,$43,$44,$45,$46,$47,$48,$49,$50,$51,$52,$53,$54,$55,$56,$57,$58,$59,$60,$61,$62,$63,$64,$65,$66,$67,$68,$69,$70,$71,$72,$73,$74,$75,$76,$77,$78,$79,$80,$81,$82,$83,$84,$85,$86,$87,$88,$89,$90,$91,$92,$93,$94,$95,$96,$97,'Fabletics'
      FROM '@lake_stg.public.tsos_da_int_inbound/lake/excel.fl_merch_outfits_ubt_hierarchy/v2/womens ubt hierarchy revamp nov 2024-outfits_womens.csv.gz')
FILE_FORMAT=(
    TYPE = CSV,
    FIELD_DELIMITER = '|',
    RECORD_DELIMITER = '\n',
    FIELD_OPTIONALLY_ENCLOSED_BY = '"',
    SKIP_HEADER = 1,
    ESCAPE_UNENCLOSED_FIELD = NONE,
    NULL_IF = ('')
);


COPY INTO lake_stg.excel.fl_merch_outfits_ubt_hierarchy_stg (product_segment, gender, outfit_number, showroom_month, go_live_date, number_of_outfit_components, us_outfit_vip_dollar, eu_uk_vipdollar, initial_launch, additional_information, outfit_complexion, sku_number_1, description_1_current_name, style_color_1, sku_number_2, description_2_current_name, style_color_2, sku_number_3_reg_inseam, sku_number_3_short_inseam, sku_number_3_tall_inseam, inseam, description_3_current_name, style_color_3, sku_number_4, description_4_current_name, style_color_4, sku_number_5, description_5_current_name, style_color_5, sku_number_6, description_6_current_name, style_color_6, sku_number_7, description_7_current_name, style_color_7, sku_number_8, description_8_current_name, style_color_8, outfit_msrp_dollar, comp_number_1_msrp, comp_number_2_msrp, comp_number_3_msrp, comp_number_4_msrp, comp_number_5_msrp, comp_number_6_msrp, comp_number_7_msrp, comp_number_8_msrp, us_imu, us_outfit_cost_dollar, us_top_cost_dollar, us_bra_jacket_cost_dollar, us_bottom_cost_dollar, us_cost_comp_number_4, us_cost_comp_number_5, us_cost_comp_number_6, us_cost_comp_number_7, us_cost_comp_number_8, mx_imu, mx_outfit_cost_dollar, mx_top_cost_dollar, mx_bra_jacket_cost_dollar, mx_bottom_cost_dollar, mx_cost_comp_number_4, mx_cost_comp_number_5, mx_cost_comp_number_6, mx_cost_comp_number_7, mx_cost_comp_number_8, eu_imu, eu_outfit_cost_dollar, eu_top_cost_dollar, eu_bra_jacket_cost_dollar, eu_bottom_cost_dollar, eu_cost_comp_number_4, eu_cost_comp_number_5, eu_cost_comp_number_6, eu_cost_comp_number_7, eu_cost_comp_number_8, uk_imu, uk_outfit_cost_dollar, uk_top_cost_dollar, uk_bra_jacket_cost_dollar, uk_bottom_cost_dollar, uk_cost_comp_number_4, uk_cost_comp_number_5, uk_cost_comp_number_6, uk_cost_comp_number_7, uk_cost_comp_number_8, rtl_imu, rtl_outfit_cost_dollar, rtl_top_cost_dollar, rtl_bra_jacket_cost_dollar, rtl_bottom_cost_dollar, rtl_cost_comp_number_4, rtl_cost_comp_number_5, rtl_cost_comp_number_6, rtl_cost_comp_number_7, rtl_cost_comp_number_8, sub_brand)
FROM (SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,$41,$42,$43,$44,$45,$46,$47,$48,$49,$50,$51,$52,$53,$54,$55,$56,$57,$58,$59,$60,$61,$62,$63,$64,$65,$66,$67,$68,$69,$70,$71,$72,$73,$74,$75,$76,$77,$78,$79,$80,$81,$82,$83,$84,$85,$86,$87,$88,$89,$90,$91,$92,$93,$94,$95,$96,$97,'Yitty'
      FROM '@lake_stg.public.tsos_da_int_inbound/lake/excel.fl_merch_outfits_ubt_hierarchy/v2/shapewear ubt hierarchy revamp nov 24-outfits_shapewear.csv.gz')
FILE_FORMAT=(
    TYPE = CSV,
    FIELD_DELIMITER = '|',
    RECORD_DELIMITER = '\n',
    FIELD_OPTIONALLY_ENCLOSED_BY = '"',
    SKIP_HEADER = 1,
    ESCAPE_UNENCLOSED_FIELD = NONE,
    NULL_IF = ('')
);

COPY INTO lake_stg.excel.fl_merch_outfits_ubt_hierarchy_stg (product_segment, gender, outfit_number, showroom_month, go_live_date, number_of_outfit_components, us_outfit_vip_dollar, eu_uk_vipdollar, initial_launch, additional_information, outfit_complexion, sku_number_1, description_1_current_name, style_color_1, sku_number_2, description_2_current_name, style_color_2, sku_number_3_reg_inseam, sku_number_3_short_inseam, sku_number_3_tall_inseam, inseam, description_3_current_name, style_color_3, sku_number_4, description_4_current_name, style_color_4, sku_number_5, description_5_current_name, style_color_5, sku_number_6, description_6_current_name, style_color_6, sku_number_7, description_7_current_name, style_color_7, sku_number_8, description_8_current_name, style_color_8, outfit_msrp_dollar, comp_number_1_msrp, comp_number_2_msrp, comp_number_3_msrp, comp_number_4_msrp, comp_number_5_msrp, comp_number_6_msrp, comp_number_7_msrp, comp_number_8_msrp, us_imu, us_outfit_cost_dollar, us_top_cost_dollar, us_bra_jacket_cost_dollar, us_bottom_cost_dollar, us_cost_comp_number_4, us_cost_comp_number_5, us_cost_comp_number_6, us_cost_comp_number_7, us_cost_comp_number_8, mx_imu, mx_outfit_cost_dollar, mx_top_cost_dollar, mx_bra_jacket_cost_dollar, mx_bottom_cost_dollar, mx_cost_comp_number_4, mx_cost_comp_number_5, mx_cost_comp_number_6, mx_cost_comp_number_7, mx_cost_comp_number_8, eu_imu, eu_outfit_cost_dollar, eu_top_cost_dollar, eu_bra_jacket_cost_dollar, eu_bottom_cost_dollar, eu_cost_comp_number_4, eu_cost_comp_number_5, eu_cost_comp_number_6, eu_cost_comp_number_7, eu_cost_comp_number_8, uk_imu, uk_outfit_cost_dollar, uk_top_cost_dollar, uk_bra_jacket_cost_dollar, uk_bottom_cost_dollar, uk_cost_comp_number_4, uk_cost_comp_number_5, uk_cost_comp_number_6, uk_cost_comp_number_7, uk_cost_comp_number_8, rtl_imu, rtl_outfit_cost_dollar, rtl_top_cost_dollar, rtl_bra_jacket_cost_dollar, rtl_bottom_cost_dollar, rtl_cost_comp_number_4, rtl_cost_comp_number_5, rtl_cost_comp_number_6, rtl_cost_comp_number_7, rtl_cost_comp_number_8, sub_brand)
FROM (SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,$41,$42,$43,$44,$45,$46,$47,$48,$49,$50,$51,$52,$53,$54,$55,$56,$57,$58,$59,$60,$61,$62,$63,$64,$65,$66,$67,$68,$69,$70,$71,$72,$73,$74,$75,$76,$77,$78,$79,$80,$81,$82,$83,$84,$85,$86,$87,$88,$89,$90,$91,$92,$93,$94,$95,$96,$97,'Scrubs'
      FROM '@lake_stg.public.tsos_da_int_inbound/lake/excel.fl_merch_outfits_ubt_hierarchy/v2/scrubs ubt hierarchy revamp nov 24-outfits_scrubs.csv.gz')
FILE_FORMAT=(
    TYPE = CSV,
    FIELD_DELIMITER = '|',
    RECORD_DELIMITER = '\n',
    FIELD_OPTIONALLY_ENCLOSED_BY = '"',
    SKIP_HEADER = 1,
    ESCAPE_UNENCLOSED_FIELD = NONE,
    NULL_IF = ('')
);

DELETE FROM lake_stg.excel.fl_merch_outfits_ubt_hierarchy_stg
WHERE (COALESCE(TRIM(PRODUCT_SEGMENT),'') = '')
AND (COALESCE(TRIM(GENDER),'') = '')
AND (COALESCE(TRIM(OUTFIT_NUMBER),'') = '')
AND (COALESCE(TRIM(SHOWROOM_MONTH),'') = '')
AND (COALESCE(TRIM(GO_LIVE_DATE),'') = '')
AND (COALESCE(TRIM(NUMBER_OF_OUTFIT_COMPONENTS),'') = '')
AND (COALESCE(TRIM(US_OUTFIT_VIP_DOLLAR),'') = '')
AND (COALESCE(TRIM(EU_UK_VIPDOLLAR),'') = '')
AND (COALESCE(TRIM(INITIAL_LAUNCH),'') = '')
AND (COALESCE(TRIM(ADDITIONAL_INFORMATION),'') = '')
AND (COALESCE(TRIM(OUTFIT_COMPLEXION),'') = '')
AND (COALESCE(TRIM(SKU_NUMBER_1),'') = '')
AND (COALESCE(TRIM(DESCRIPTION_1_CURRENT_NAME),'') = '')
AND (COALESCE(TRIM(STYLE_COLOR_1),'') = '')
AND (COALESCE(TRIM(SKU_NUMBER_2),'') = '')
AND (COALESCE(TRIM(DESCRIPTION_2_CURRENT_NAME),'') = '')
AND (COALESCE(TRIM(STYLE_COLOR_2),'') = '')
AND (COALESCE(TRIM(SKU_NUMBER_3_REG_INSEAM),'') = '')
AND (COALESCE(TRIM(SKU_NUMBER_3_SHORT_INSEAM),'') = '')
AND (COALESCE(TRIM(SKU_NUMBER_3_TALL_INSEAM),'') = '')
AND (COALESCE(TRIM(INSEAM),'') = '')
AND (COALESCE(TRIM(DESCRIPTION_3_CURRENT_NAME),'') = '')
AND (COALESCE(TRIM(STYLE_COLOR_3),'') = '')
AND (COALESCE(TRIM(SKU_NUMBER_4),'') = '')
AND (COALESCE(TRIM(DESCRIPTION_4_CURRENT_NAME),'') = '')
AND (COALESCE(TRIM(STYLE_COLOR_4),'') = '')
AND (COALESCE(TRIM(SKU_NUMBER_5),'') = '')
AND (COALESCE(TRIM(DESCRIPTION_5_CURRENT_NAME),'') = '')
AND (COALESCE(TRIM(STYLE_COLOR_5),'') = '')
AND (COALESCE(TRIM(SKU_NUMBER_6),'') = '')
AND (COALESCE(TRIM(DESCRIPTION_6_CURRENT_NAME),'') = '')
AND (COALESCE(TRIM(STYLE_COLOR_6),'') = '')
AND (COALESCE(TRIM(SKU_NUMBER_7),'') = '')
AND (COALESCE(TRIM(DESCRIPTION_7_CURRENT_NAME),'') = '')
AND (COALESCE(TRIM(STYLE_COLOR_7),'') = '')
AND (COALESCE(TRIM(SKU_NUMBER_8),'') = '')
AND (COALESCE(TRIM(DESCRIPTION_8_CURRENT_NAME),'') = '')
AND (COALESCE(TRIM(STYLE_COLOR_8),'') = '')
AND (COALESCE(TRIM(OUTFIT_MSRP_DOLLAR),'') = '')
AND (COALESCE(TRIM(COMP_NUMBER_1_MSRP),'') = '')
AND (COALESCE(TRIM(COMP_NUMBER_2_MSRP),'') = '')
AND (COALESCE(TRIM(COMP_NUMBER_3_MSRP),'') = '')
AND (COALESCE(TRIM(COMP_NUMBER_4_MSRP),'') = '')
AND (COALESCE(TRIM(COMP_NUMBER_5_MSRP),'') = '')
AND (COALESCE(TRIM(COMP_NUMBER_6_MSRP),'') = '')
AND (COALESCE(TRIM(COMP_NUMBER_7_MSRP),'') = '')
AND (COALESCE(TRIM(COMP_NUMBER_8_MSRP),'') = '')
AND (COALESCE(TRIM(US_IMU),'') = '')
AND (COALESCE(TRIM(US_OUTFIT_COST_DOLLAR),'') = '')
AND (COALESCE(TRIM(US_TOP_COST_DOLLAR),'') = '')
AND (COALESCE(TRIM(US_BRA_JACKET_COST_DOLLAR),'') = '')
AND (COALESCE(TRIM(US_BOTTOM_COST_DOLLAR),'') = '')
AND (COALESCE(TRIM(US_COST_COMP_NUMBER_4),'') = '')
AND (COALESCE(TRIM(US_COST_COMP_NUMBER_5),'') = '')
AND (COALESCE(TRIM(US_COST_COMP_NUMBER_6),'') = '')
AND (COALESCE(TRIM(US_COST_COMP_NUMBER_7),'') = '')
AND (COALESCE(TRIM(US_COST_COMP_NUMBER_8),'') = '')
AND (COALESCE(TRIM(MX_IMU),'') = '')
AND (COALESCE(TRIM(MX_OUTFIT_COST_DOLLAR),'') = '')
AND (COALESCE(TRIM(MX_TOP_COST_DOLLAR),'') = '')
AND (COALESCE(TRIM(MX_BRA_JACKET_COST_DOLLAR),'') = '')
AND (COALESCE(TRIM(MX_BOTTOM_COST_DOLLAR),'') = '')
AND (COALESCE(TRIM(MX_COST_COMP_NUMBER_4),'') = '')
AND (COALESCE(TRIM(MX_COST_COMP_NUMBER_5),'') = '')
AND (COALESCE(TRIM(MX_COST_COMP_NUMBER_6),'') = '')
AND (COALESCE(TRIM(MX_COST_COMP_NUMBER_7),'') = '')
AND (COALESCE(TRIM(MX_COST_COMP_NUMBER_8),'') = '')
AND (COALESCE(TRIM(EU_IMU),'') = '')
AND (COALESCE(TRIM(EU_OUTFIT_COST_DOLLAR),'') = '')
AND (COALESCE(TRIM(EU_TOP_COST_DOLLAR),'') = '')
AND (COALESCE(TRIM(EU_BRA_JACKET_COST_DOLLAR),'') = '')
AND (COALESCE(TRIM(EU_BOTTOM_COST_DOLLAR),'') = '')
AND (COALESCE(TRIM(EU_COST_COMP_NUMBER_4),'') = '')
AND (COALESCE(TRIM(EU_COST_COMP_NUMBER_5),'') = '')
AND (COALESCE(TRIM(EU_COST_COMP_NUMBER_6),'') = '')
AND (COALESCE(TRIM(EU_COST_COMP_NUMBER_7),'') = '')
AND (COALESCE(TRIM(EU_COST_COMP_NUMBER_8),'') = '')
AND (COALESCE(TRIM(UK_IMU),'') = '')
AND (COALESCE(TRIM(UK_OUTFIT_COST_DOLLAR),'') = '')
AND (COALESCE(TRIM(UK_TOP_COST_DOLLAR),'') = '')
AND (COALESCE(TRIM(UK_BRA_JACKET_COST_DOLLAR),'') = '')
AND (COALESCE(TRIM(UK_BOTTOM_COST_DOLLAR),'') = '')
AND (COALESCE(TRIM(UK_COST_COMP_NUMBER_4),'') = '')
AND (COALESCE(TRIM(UK_COST_COMP_NUMBER_5),'') = '')
AND (COALESCE(TRIM(UK_COST_COMP_NUMBER_6),'') = '')
AND (COALESCE(TRIM(UK_COST_COMP_NUMBER_7),'') = '')
AND (COALESCE(TRIM(UK_COST_COMP_NUMBER_8),'') = '')
AND (COALESCE(TRIM(RTL_IMU),'') = '')
AND (COALESCE(TRIM(RTL_OUTFIT_COST_DOLLAR),'') = '')
AND (COALESCE(TRIM(RTL_TOP_COST_DOLLAR),'') = '')
AND (COALESCE(TRIM(RTL_BRA_JACKET_COST_DOLLAR),'') = '')
AND (COALESCE(TRIM(RTL_BOTTOM_COST_DOLLAR),'') = '')
AND (COALESCE(TRIM(RTL_COST_COMP_NUMBER_4),'') = '')
AND (COALESCE(TRIM(RTL_COST_COMP_NUMBER_5),'') = '')
AND (COALESCE(TRIM(RTL_COST_COMP_NUMBER_6),'') = '')
AND (COALESCE(TRIM(RTL_COST_COMP_NUMBER_7),'') = '')
AND (COALESCE(TRIM(RTL_COST_COMP_NUMBER_8),'') = '')
;

MERGE INTO lake.excel.fl_merch_outfits_ubt_hierarchy t
USING (
    SELECT
        a.*
    FROM (
        SELECT
            product_segment,
            gender,
            outfit_number,
            showroom_month,
            go_live_date,
            number_of_outfit_components,
            us_outfit_vip_dollar,
            eu_uk_vipdollar,
            initial_launch,
            additional_information,
            outfit_complexion,
            sku_number_1,
            description_1_current_name,
            style_color_1,
            sku_number_2,
            description_2_current_name,
            style_color_2 ,
            sku_number_3_reg_inseam,
            sku_number_3_short_inseam,
            sku_number_3_tall_inseam,
            inseam,
            description_3_current_name,
            style_color_3,
            sku_number_4,
            description_4_current_name,
            style_color_4,
            sku_number_5,
            description_5_current_name,
            style_color_5,
            sku_number_6,
            description_6_current_name,
            style_color_6,
            sku_number_7,
            description_7_current_name,
            style_color_7,
            sku_number_8,
            description_8_current_name,
            style_color_8,
            outfit_msrp_dollar,
            comp_number_1_msrp,
            comp_number_2_msrp,
            comp_number_3_msrp,
            comp_number_4_msrp,
            comp_number_5_msrp,
            comp_number_6_msrp,
            comp_number_7_msrp,
            comp_number_8_msrp,
            us_imu * 100 AS us_imu,
            us_outfit_cost_dollar,
            us_top_cost_dollar,
            us_bra_jacket_cost_dollar,
            us_bottom_cost_dollar,
            us_cost_comp_number_4,
            us_cost_comp_number_5,
            us_cost_comp_number_6,
            us_cost_comp_number_7,
            us_cost_comp_number_8,
            mx_imu * 100 AS mx_imu,
            mx_outfit_cost_dollar,
            mx_top_cost_dollar,
            mx_bra_jacket_cost_dollar,
            mx_bottom_cost_dollar,
            mx_cost_comp_number_4,
            mx_cost_comp_number_5,
            mx_cost_comp_number_6,
            mx_cost_comp_number_7,
            mx_cost_comp_number_8,
            eu_imu * 100 AS eu_imu,
            eu_outfit_cost_dollar,
            eu_top_cost_dollar,
            eu_bra_jacket_cost_dollar,
            eu_bottom_cost_dollar,
            eu_cost_comp_number_4,
            eu_cost_comp_number_5,
            eu_cost_comp_number_6,
            eu_cost_comp_number_7,
            eu_cost_comp_number_8,
            uk_imu * 100 AS uk_imu,
            uk_outfit_cost_dollar,
            uk_top_cost_dollar,
            uk_bra_jacket_cost_dollar,
            uk_bottom_cost_dollar,
            uk_cost_comp_number_4,
            uk_cost_comp_number_5,
            uk_cost_comp_number_6,
            uk_cost_comp_number_7,
            uk_cost_comp_number_8,
            rtl_imu * 100 AS rtl_imu,
            rtl_outfit_cost_dollar,
            rtl_top_cost_dollar,
            rtl_bra_jacket_cost_dollar,
            rtl_bottom_cost_dollar,
            rtl_cost_comp_number_4,
            rtl_cost_comp_number_5,
            rtl_cost_comp_number_6,
            rtl_cost_comp_number_7,
            rtl_cost_comp_number_8,
            CONCAT('https://fabletics-us-cdn.justfab.com/media/images/products/' || ubt.outfit_number ||'/' || ubt.outfit_number || '-1_271x407.jpg') AS outfit_image,
            sub_brand,
            hash(*) AS meta_row_hash,
            current_timestamp AS meta_create_datetime,
            current_timestamp AS meta_update_datetime,
            row_number() OVER ( PARTITION BY outfit_number, showroom_month ORDER BY coalesce(go_live_date, '0') DESC ) AS rn
        FROM lake_stg.excel.fl_merch_outfits_ubt_hierarchy_stg ubt
     ) a
    WHERE a.rn = 1
) s ON equal_null(t.outfit_number, s.outfit_number)
    AND equal_null(t.showroom_month, s.showroom_month)
WHEN NOT MATCHED THEN INSERT (
    product_segment, gender, outfit_number, showroom_month, go_live_date, number_of_outfit_components, us_outfit_vip_dollar, eu_uk_vipdollar, initial_launch, additional_information, outfit_complexion, sku_number_1, description_1_current_name, style_color_1, sku_number_2, description_2_current_name, style_color_2, sku_number_3_reg_inseam, sku_number_3_short_inseam, sku_number_3_tall_inseam, inseam, description_3_current_name, style_color_3, sku_number_4, description_4_current_name, style_color_4, sku_number_5, description_5_current_name, style_color_5, sku_number_6, description_6_current_name, style_color_6, sku_number_7, description_7_current_name, style_color_7, sku_number_8, description_8_current_name, style_color_8, outfit_msrp_dollar, comp_number_1_msrp, comp_number_2_msrp, comp_number_3_msrp, comp_number_4_msrp, comp_number_5_msrp, comp_number_6_msrp, comp_number_7_msrp, comp_number_8_msrp, us_imu, us_outfit_cost_dollar, us_top_cost_dollar, us_bra_jacket_cost_dollar, us_bottom_cost_dollar, us_cost_comp_number_4, us_cost_comp_number_5, us_cost_comp_number_6, us_cost_comp_number_7, us_cost_comp_number_8, mx_imu, mx_outfit_cost_dollar, mx_top_cost_dollar, mx_bra_jacket_cost_dollar, mx_bottom_cost_dollar, mx_cost_comp_number_4, mx_cost_comp_number_5, mx_cost_comp_number_6, mx_cost_comp_number_7, mx_cost_comp_number_8, eu_imu, eu_outfit_cost_dollar, eu_top_cost_dollar, eu_bra_jacket_cost_dollar, eu_bottom_cost_dollar, eu_cost_comp_number_4, eu_cost_comp_number_5, eu_cost_comp_number_6, eu_cost_comp_number_7, eu_cost_comp_number_8, uk_imu, uk_outfit_cost_dollar, uk_top_cost_dollar, uk_bra_jacket_cost_dollar, uk_bottom_cost_dollar, uk_cost_comp_number_4, uk_cost_comp_number_5, uk_cost_comp_number_6, uk_cost_comp_number_7, uk_cost_comp_number_8, rtl_imu, rtl_outfit_cost_dollar, rtl_top_cost_dollar, rtl_bra_jacket_cost_dollar, rtl_bottom_cost_dollar, rtl_cost_comp_number_4, rtl_cost_comp_number_5, rtl_cost_comp_number_6, rtl_cost_comp_number_7, rtl_cost_comp_number_8, outfit_image, sub_brand, meta_row_hash, meta_create_datetime, meta_update_datetime
)
VALUES (
    product_segment, gender, outfit_number, showroom_month, go_live_date, number_of_outfit_components, us_outfit_vip_dollar, eu_uk_vipdollar, initial_launch, additional_information, outfit_complexion, sku_number_1, description_1_current_name, style_color_1, sku_number_2, description_2_current_name, style_color_2, sku_number_3_reg_inseam, sku_number_3_short_inseam, sku_number_3_tall_inseam, inseam, description_3_current_name, style_color_3, sku_number_4, description_4_current_name, style_color_4, sku_number_5, description_5_current_name, style_color_5, sku_number_6, description_6_current_name, style_color_6, sku_number_7, description_7_current_name, style_color_7, sku_number_8, description_8_current_name, style_color_8, outfit_msrp_dollar, comp_number_1_msrp, comp_number_2_msrp, comp_number_3_msrp, comp_number_4_msrp, comp_number_5_msrp, comp_number_6_msrp, comp_number_7_msrp, comp_number_8_msrp, us_imu, us_outfit_cost_dollar, us_top_cost_dollar, us_bra_jacket_cost_dollar, us_bottom_cost_dollar, us_cost_comp_number_4, us_cost_comp_number_5, us_cost_comp_number_6, us_cost_comp_number_7, us_cost_comp_number_8, mx_imu, mx_outfit_cost_dollar, mx_top_cost_dollar, mx_bra_jacket_cost_dollar, mx_bottom_cost_dollar, mx_cost_comp_number_4, mx_cost_comp_number_5, mx_cost_comp_number_6, mx_cost_comp_number_7, mx_cost_comp_number_8, eu_imu, eu_outfit_cost_dollar, eu_top_cost_dollar, eu_bra_jacket_cost_dollar, eu_bottom_cost_dollar, eu_cost_comp_number_4, eu_cost_comp_number_5, eu_cost_comp_number_6, eu_cost_comp_number_7, eu_cost_comp_number_8, uk_imu, uk_outfit_cost_dollar, uk_top_cost_dollar, uk_bra_jacket_cost_dollar, uk_bottom_cost_dollar, uk_cost_comp_number_4, uk_cost_comp_number_5, uk_cost_comp_number_6, uk_cost_comp_number_7, uk_cost_comp_number_8, rtl_imu, rtl_outfit_cost_dollar, rtl_top_cost_dollar, rtl_bra_jacket_cost_dollar, rtl_bottom_cost_dollar, rtl_cost_comp_number_4, rtl_cost_comp_number_5, rtl_cost_comp_number_6, rtl_cost_comp_number_7, rtl_cost_comp_number_8, outfit_image,sub_brand, meta_row_hash, meta_create_datetime, meta_update_datetime
)
WHEN MATCHED AND t.meta_row_hash != s.meta_row_hash
THEN UPDATE
SET t.product_segment = s.product_segment,
    t.gender = s.gender,
    t.outfit_number = s.outfit_number,
    t.showroom_month = s.showroom_month,
    t.go_live_date = s.go_live_date,
    t.number_of_outfit_components = s.number_of_outfit_components,
    t.us_outfit_vip_dollar = s.us_outfit_vip_dollar,
    t.eu_uk_vipdollar = s.eu_uk_vipdollar,
    t.initial_launch = s.initial_launch,
    t.additional_information = s.additional_information,
    t.outfit_complexion = s.outfit_complexion,
    t.sku_number_1 = s.sku_number_1,
    t.description_1_current_name = s.description_1_current_name,
    t.style_color_1 = s.style_color_1,
    t.sku_number_2 = s.sku_number_2,
    t.description_2_current_name = s.description_2_current_name,
    t.style_color_2 = s.style_color_2,
    t.sku_number_3_reg_inseam = s.sku_number_3_reg_inseam,
    t.sku_number_3_short_inseam = s.sku_number_3_short_inseam,
    t.sku_number_3_tall_inseam = s.sku_number_3_tall_inseam,
    t.inseam = s.inseam,
    t.description_3_current_name = s.description_3_current_name,
    t.style_color_3 = s.style_color_3,
    t.sku_number_4 = s.sku_number_4,
    t.description_4_current_name = s.description_4_current_name,
    t.style_color_4 = s.style_color_4,
    t.sku_number_5 = s.sku_number_5,
    t.description_5_current_name = s.description_5_current_name,
    t.style_color_5 = s.style_color_5,
    t.sku_number_6 = s.sku_number_6,
    t.description_6_current_name = s.description_6_current_name,
    t.style_color_6 = s.style_color_6,
    t.sku_number_7 = s.sku_number_7,
    t.description_7_current_name = s.description_7_current_name,
    t.style_color_7 = s.style_color_7,
    t.sku_number_8 = s.sku_number_8,
    t.description_8_current_name = s.description_8_current_name,
    t.style_color_8 = s.style_color_8,
    t.outfit_msrp_dollar = s.outfit_msrp_dollar,
    t.comp_number_1_msrp = s.comp_number_1_msrp,
    t.comp_number_2_msrp = s.comp_number_2_msrp,
    t.comp_number_3_msrp = s.comp_number_3_msrp,
    t.comp_number_4_msrp = s.comp_number_4_msrp,
    t.comp_number_5_msrp = s.comp_number_5_msrp,
    t.comp_number_6_msrp = s.comp_number_6_msrp,
    t.comp_number_7_msrp = s.comp_number_7_msrp,
    t.comp_number_8_msrp = s.comp_number_8_msrp,
    t.us_imu = s.us_imu,
    t.us_outfit_cost_dollar = s.us_outfit_cost_dollar,
    t.us_top_cost_dollar = s.us_top_cost_dollar,
    t.us_bra_jacket_cost_dollar = s.us_bra_jacket_cost_dollar,
    t.us_bottom_cost_dollar = s.us_bottom_cost_dollar,
    t.us_cost_comp_number_4 = s.us_cost_comp_number_4,
    t.us_cost_comp_number_5 = s.us_cost_comp_number_5,
    t.us_cost_comp_number_6 = s.us_cost_comp_number_6,
    t.us_cost_comp_number_7 = s.us_cost_comp_number_7,
    t.us_cost_comp_number_8 = s.us_cost_comp_number_8,
    t.mx_imu = s.mx_imu,
    t.mx_outfit_cost_dollar = s.mx_outfit_cost_dollar,
    t.mx_top_cost_dollar = s.mx_top_cost_dollar,
    t.mx_bra_jacket_cost_dollar = s.mx_bra_jacket_cost_dollar,
    t.mx_bottom_cost_dollar = s.mx_bottom_cost_dollar,
    t.mx_cost_comp_number_4 = s.mx_cost_comp_number_4,
    t.mx_cost_comp_number_5 = s.mx_cost_comp_number_5,
    t.mx_cost_comp_number_6 = s.mx_cost_comp_number_6,
    t.mx_cost_comp_number_7 = s.mx_cost_comp_number_7,
    t.mx_cost_comp_number_8 = s.mx_cost_comp_number_8,
    t.eu_imu = s.eu_imu,
    t.eu_outfit_cost_dollar = s.eu_outfit_cost_dollar,
    t.eu_top_cost_dollar = s.eu_top_cost_dollar,
    t.eu_bra_jacket_cost_dollar = s.eu_bra_jacket_cost_dollar,
    t.eu_bottom_cost_dollar = s.eu_bottom_cost_dollar,
    t.eu_cost_comp_number_4 = s.eu_cost_comp_number_4,
    t.eu_cost_comp_number_5 = s.eu_cost_comp_number_5,
    t.eu_cost_comp_number_6 = s.eu_cost_comp_number_6,
    t.eu_cost_comp_number_7 = s.eu_cost_comp_number_7,
    t.eu_cost_comp_number_8 = s.eu_cost_comp_number_8,
    t.uk_imu = s.uk_imu,
    t.uk_outfit_cost_dollar = s.uk_outfit_cost_dollar,
    t.uk_top_cost_dollar = s.uk_top_cost_dollar,
    t.uk_bra_jacket_cost_dollar = s.uk_bra_jacket_cost_dollar,
    t.uk_bottom_cost_dollar = s.uk_bottom_cost_dollar,
    t.uk_cost_comp_number_4 = s.uk_cost_comp_number_4,
    t.uk_cost_comp_number_5 = s.uk_cost_comp_number_5,
    t.uk_cost_comp_number_6 = s.uk_cost_comp_number_6,
    t.uk_cost_comp_number_7 = s.uk_cost_comp_number_7,
    t.uk_cost_comp_number_8 = s.uk_cost_comp_number_8,
    t.rtl_imu = s.rtl_imu,
    t.rtl_outfit_cost_dollar = s.rtl_outfit_cost_dollar,
    t.rtl_top_cost_dollar = s.rtl_top_cost_dollar,
    t.rtl_bra_jacket_cost_dollar = s.rtl_bra_jacket_cost_dollar,
    t.rtl_bottom_cost_dollar = s.rtl_bottom_cost_dollar,
    t.rtl_cost_comp_number_4 = s.rtl_cost_comp_number_4,
    t.rtl_cost_comp_number_5 = s.rtl_cost_comp_number_5,
    t.rtl_cost_comp_number_6 = s.rtl_cost_comp_number_6,
    t.rtl_cost_comp_number_7 = s.rtl_cost_comp_number_7,
    t.rtl_cost_comp_number_8 = s.rtl_cost_comp_number_8,
    t.outfit_image = s.outfit_image,
    t.sub_brand = s.sub_brand,
    t.meta_row_hash = s.meta_row_hash,
    t.meta_update_datetime = s.meta_update_datetime;

DELETE FROM lake.excel.fl_merch_outfits_ubt_hierarchy t
WHERE NOT EXISTS (
    SELECT 1
    FROM lake_stg.excel.fl_merch_outfits_ubt_hierarchy_stg s
    WHERE equal_null(t.outfit_number, s.outfit_number)
        AND equal_null(t.showroom_month, s.showroom_month)
);

COMMIT;
