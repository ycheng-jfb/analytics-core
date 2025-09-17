CREATE VIEW lake_view.excel.cross_border_fee_entry_list AS
SELECT
    'YT' as brand,
    entry,
    customer_reference,
    entry_val,
    release_date,
    gen_desc,
    stmt_no,
    pmt_date,
    duty,
    other,
    total,
    stmt_month,
    meta_row_hash,
    meta_create_datetime,
    meta_update_datetime
FROM lake.excel.entry_list_report_yitty
UNION ALL
SELECT
    'FL' as brand,
    entry,
    customer_reference,
    entry_val,
    release_date,
    gen_desc,
    stmt_no,
    pmt_date,
    duty,
    other,
    total,
    stmt_month,
    meta_row_hash,
    meta_create_datetime,
    meta_update_datetime
FROM lake.excel.entry_list_report_fabletics
UNION ALL
SELECT
    'SXF' as brand,
    entry,
    customer_reference,
    entry_val,
    release_date,
    gen_desc,
    stmt_no,
    pmt_date,
    duty,
    other,
    total,
    stmt_month,
    meta_row_hash,
    meta_create_datetime,
    meta_update_datetime
FROM lake.excel.entry_list_report_lavender
UNION ALL
SELECT
    'JF' AS brand,
    entry,
    customer_reference,
    entry_val,
    release_date,
    gen_desc,
    stmt_no,
    pmt_date,
    duty,
    other,
    total,
    stmt_month,
    meta_row_hash,
    meta_create_datetime,
    meta_update_datetime
FROM lake.excel.entry_list_report_justfab
UNION ALL
SELECT
    'FK' AS brand,
    entry,
    customer_reference,
    entry_val,
    release_date,
    gen_desc,
    stmt_no,
    pmt_date,
    duty,
    other,
    total,
    stmt_month,
    meta_row_hash,
    meta_create_datetime,
    meta_update_datetime
FROM lake.excel.entry_list_report_personalretailing;
