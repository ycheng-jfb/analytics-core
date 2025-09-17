CREATE OR REPLACE VIEW lake_view.sharepoint.psourcecategorization AS
SELECT level_1_label_
	,level_4_label_
    ,level_2_label_
    ,psource_level_3_
    ,m_w_s
    ,level_3_label_
    ,psource_level_4_
    ,psource_level_5_
    ,level_5_label_
    ,notes
    ,_fivetran_synced::TIMESTAMP_LTZ AS meta_update_datetime
FROM lake_fivetran.fabletics_sharepoint_v1.psource_cleanup_final_tab;
