from include.utils.snowflake import Column

column_list_inspections = [
    Column("date_inspection", "TIMESTAMP_NTZ(3)"),
    Column("inspector", "VARIANT"),
    Column("inspection_location", "VARCHAR"),
    Column("report_type", "VARIANT"),
    Column("date_created", "TIMESTAMP_NTZ(3)"),
    Column("date_modified", "TIMESTAMP_NTZ(3)"),
    Column("recycled", "NUMBER(38)"),
    Column("factory_name", "VARCHAR"),
    Column("contact_name", "VARCHAR"),
    Column("contact_number", "VARCHAR"),
    Column("inspection_company", "VARCHAR"),
    Column("approved_by", "VARCHAR"),
    Column("id", "NUMBER(38)", uniqueness=True),
    Column("assignments_items", "VARIANT"),
    Column("assignment_group", "VARIANT"),
    Column("inspection_method", "VARIANT"),
    Column("inspection_level", "VARIANT"),
    Column("report_url", "VARCHAR"),
    Column("travel_tag_number", "VARCHAR"),
    Column("shift", "VARCHAR"),
    Column("line", "VARCHAR"),
    Column("supervisor", "VARCHAR"),
    Column("comment", "VARCHAR"),
    Column("cutter", "VARCHAR"),
    Column("next_pqa_date", "VARCHAR"),
    Column("staff_id", "VARCHAR"),
    Column("assignments_reviewers", "VARIANT"),
    Column("reviewer_user", "VARCHAR"),
    Column("spudi_lab_address", "VARCHAR"),
    Column("spudi_date", "VARCHAR"),
    Column("updated_at", "TIMESTAMP_NTZ(3)", delta_column=True),
]

column_list_adv_details = [
    Column(
        "INSPECTION_ID", "NUMBER", source_name="assignments_items_id", uniqueness=True
    ),
    Column(
        "ASSIGNMENT_GROUP", "NUMBER", source_name="assignment_group_id", uniqueness=True
    ),
    Column(
        "PO_NUMBER",
        "VARCHAR",
        source_name="assignments_items_po_line_po_id",
        uniqueness=True,
    ),
    Column("DATE_OF_INSPECTION", "TIMESTAMP_NTZ(3)", source_name="date_inspection"),
    Column("INSPECTOR_NAME", "VARCHAR", source_name="inspector_name"),
    Column("INSPECTION_TYPE", "VARCHAR", source_name="report_type_name"),
    Column("FACTORY_NAME", "VARCHAR", source_name="factory_name"),
    Column(
        "INSPECTION_COMPLETED_DATE",
        "TIMESTAMP_NTZ(3)",
        source_name="assignments_items_inspection_completed_date",
    ),
    Column(
        "INSPECTION_STATUS",
        "VARCHAR",
        source_name="assignments_items_inspection_status_text",
    ),
    Column(
        "INSPECTION_RESULT",
        "VARCHAR",
        source_name="assignments_items_inspection_result_text",
    ),
    Column("PLM_STYLE", "VARCHAR", source_name="assignments_items_po_line_style"),
    Column("BUSINESS_UNIT", "VARCHAR", source_name="assignments_items_po_line_banner"),
    Column(
        "VENDOR_NAME",
        "VARCHAR",
        source_name="assignments_items_po_line_po_suppliers_name",
    ),
    Column(
        "ERP_BUSINESS_ID",
        "NUMBER",
        source_name="assignments_items_po_line_po_suppliers_erp_business_id",
    ),
    Column(
        "FACTORY_NAME_ERP",
        "VARCHAR",
        source_name="assignments_items_po_line_po_factory_erp_business_id",
    ),
    Column(
        "FACTORY_ORIGIN_COUNTRY",
        "VARCHAR",
        source_name="assignments_items_po_line_po_factory_country",
    ),
    Column(
        "ITEM_NAME", "VARCHAR", source_name="assignments_items_po_line_sku_item_name"
    ),
    Column(
        "ITEM_CATEGORY",
        "VARCHAR",
        source_name="assignments_items_po_line_sku_product_family",
    ),
    Column(
        "ITEM_DEPARTMENT", "VARCHAR", source_name="assignments_items_po_line_department"
    ),
    Column(
        "DESTINATION_REGION", "VARCHAR", source_name="assignments_items_po_line_region"
    ),
    Column(
        "DEFECT_DESCRIPTION",
        "VARCHAR",
        source_name="assignments_items_inspection_report_sections_defects_label",
    ),
    Column(
        "DEFECT_SEVERITY_LABEL",
        "VARCHAR",
        source_name="assignments_items_inspection_report_sections_defects_level_label",
    ),
    Column(
        "DEFECT_DESCRIPTION_CATEGORY",
        "VARCHAR",
        source_name="assignments_items_inspection_report_sections_defects_subsection",
    ),
    Column(
        "DEFECT_CODE",
        "VARCHAR",
        source_name="assignments_items_inspection_report_sections_defects_code",
        uniqueness=True,
    ),
    Column(
        "GROUP_INSPECTED_QTY", "NUMBER", source_name="assignments_items_qty_inspected"
    ),
    Column(
        "CRITICAL_DEFECTS",
        "NUMBER",
        source_name="assignments_items_inspection_report_total_critical_defects",
    ),
    Column(
        "MAJOR_DEFECTS",
        "NUMBER",
        source_name="assignments_items_inspection_report_total_major_defects",
    ),
    Column(
        "MINOR_DEFECTS",
        "NUMBER",
        source_name="assignments_items_inspection_report_total_minor_defects",
    ),
    Column(
        "DEFECTIVE_PARTS",
        "NUMBER",
        source_name="assignments_items_inspection_report_defective_parts",
    ),
    Column(
        "DEFECTS_PER_DESCRIPTION",
        "NUMBER",
        source_name="assignments_items_inspection_report_sections_defects_level_value",
    ),
    Column("updated_at", "TIMESTAMP_NTZ(3)", delta_column=True),
]
