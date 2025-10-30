from dataclasses import dataclass
from typing import Any, Type, Union

import pendulum
from airflow.models import DAG
from airflow.operators.python import BranchPythonOperator

from include.airflow.callbacks.slack import slack_failure_gsc
from include.airflow.operators.email_to_smb import EmailToSMBOperator
from include.airflow.operators.excel_smb import ExcelSMBToS3BatchOperator, SheetConfig
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.airflow.operators.snowflake_load import (
    SnowflakeCopyTableOperator,
    SnowflakeInsertOperator,
)
from include.airflow.operators.snowflake_to_mssql import SnowflakeToMSSqlFastMany
from include.config import conn_ids, email_lists, owners, s3_buckets, stages
from include.utils.snowflake import Column, CopyConfigCsv

sheets = {
    "oocl": SheetConfig(
        sheet_name='REPORT',
        schema='excel',
        table='broker_data_oocl',
        s3_replace=False,
        header_rows=1,
        column_list=[
            Column('pstadv_num', 'varchar', uniqueness=True),
            Column('entry_num', 'VARCHAR'),
            Column('invc_num', 'VARCHAR'),
            Column('pod_eta', 'DATE'),
            Column('fnd_country', 'VARCHAR'),
            Column('business_unit', 'VARCHAR', source_name='Business Unit field'),
            Column('po_num', 'VARCHAR', uniqueness=True),
            Column('sku_num', 'VARCHAR', uniqueness=True),
            Column('hts_num', 'INT'),
            Column('por_country', 'VARCHAR'),
            Column('unit', 'INT'),
            Column('unit_cost', 'NUMBER(38,2)'),
            Column('sku_value', 'NUMBER(38,2)', source_name='SKU Value'),
            Column('est_duty', 'NUMBER(38,6)', source_name='Est Duty'),
            Column('est_duty_rate', 'NUMBER(38,2)', source_name='est_duty_rate'),
            Column('default_to_rate', 'VARCHAR', source_name='Default To Rate'),
            Column('est_broker_fee', 'NUMBER(38,6)', source_name='Est Broker Fee'),
            Column('advance_fee', 'NUMBER(38,6)', source_name='Advance Fee'),
            Column('receive_date', 'DATE'),
            Column('bl_num', 'VARCHAR'),
            Column('fw_bl_num', 'VARCHAR'),
            Column('cntr_num', 'VARCHAR'),
            Column('pol_etd', 'DATE'),
            Column('oll_billing_invoice_num', 'VARCHAR', source_name='OLL Billing Invoice#'),
            # Column('duty_fee', 'NUMBER(38,2)'),
            # column is removed from file but table has data for old records
            Column('line_no', 'NUMBER(38,2)', source_name='Line No.'),
        ],
    ),
    "carmichael": SheetConfig(
        sheet_name='Sheet',
        schema='excel',
        table='broker_data_carmichael',
        s3_replace=False,
        header_rows=1,
        dtype={'Broker Ref#': 'str'},
        column_list=[
            Column('broker_ref_num', 'VARCHAR', source_name='Broker Ref#', uniqueness=True),
            Column('entry_num', 'VARCHAR', source_name='Entry#'),
            Column('carrier', 'VARCHAR'),
            Column('eta', 'DATE'),
            Column('entry_port', 'VARCHAR', source_name='Entry Port'),
            Column('po_num', 'VARCHAR', source_name='PO#', uniqueness=True),
            Column('sku_num', 'VARCHAR', source_name='SKU#', uniqueness=True),
            Column('po_line_number', 'NUMBER(38,0)', source_name='PO_LINE_NUMBER'),
            Column('hts_num', 'NUMBER(38,0)', source_name='HTS#'),
            Column('advalorem_rate', 'NUMBER(38,6)', source_name='Advalorem Rate'),
            Column('origin_country', 'VARCHAR', source_name='Origin Country'),
            Column('pieces', 'NUMBER(38,0)'),
            Column('unit_price', 'NUMBER(38,4)', source_name='Unit Price'),
            Column('po_value', 'NUMBER(38,4)', source_name='P/O Value'),
            Column('duty', 'NUMBER(38,4)', source_name='Duty'),
            Column('us_tariffs', 'NUMBER(38,4)', source_name='US Tariffs'),
            Column('hmf', 'NUMBER(38,4)'),
            Column('mpf', 'NUMBER(38,4)'),
            Column('non_dutiable', 'NUMBER(38,4)', source_name='Non-Dutiable'),
            Column('cotton_fee', 'NUMBER(38,4)', source_name='Cotton Fee'),
            Column('other_fee', 'NUMBER(38,4)', source_name='Other Fee'),
            Column('broker_fee', 'NUMBER(38,4)', source_name='Broker Fee'),
            Column('other_charge', 'NUMBER(38,4)', source_name='Other Charge'),
            Column('invoice_num_7501', 'NUMBER(38,0)', source_name='7501 Invoice#'),
            Column('line_num_po_7501', 'VARCHAR', source_name='7501 Line#(P/O)'),
            Column('statement_num', 'VARCHAR', source_name='Statement#'),
            Column('duty_rate_301', 'NUMBER(38,4)', source_name='301 Duty Rate'),
        ],
    ),
    "cmt_labor_cost": SheetConfig(
        sheet_name='Sheet1',
        schema='excel',
        table='cmt_labor_cost',
        s3_replace=False,
        header_rows=1,
        column_list=[
            Column('operating_unit', 'VARCHAR', source_name='OperatingUnit'),
            Column('legal_entity', 'VARCHAR', source_name='LegalEntity'),
            Column('brand', 'VARCHAR', source_name='Brand'),
            Column('org_code', 'VARCHAR', source_name='OrgCode'),
            Column('org_name', 'VARCHAR', source_name='OrgName'),
            Column('receipt_number', 'NUMBER(38,0)', source_name='ReceiptNumber'),
            Column('shipment_number', 'NUMBER(38,2)', source_name='ShipmentNumber'),
            Column('receipt_date', 'TIMESTAMP_NTZ', source_name='ReceiptDate'),
            Column('transaction_type', 'VARCHAR', source_name='TransactionType'),
            Column('supplier', 'VARCHAR', source_name='Supplier'),
            Column('po_number', 'VARCHAR', source_name='PONumber'),
            Column('item_number', 'VARCHAR', source_name='ItemNumber'),
            Column('item_description', 'VARCHAR', source_name='ItemDescription'),
            Column('quantity_received', 'NUMBER(38,0)', source_name='QuantityReceived'),
            Column('uom', 'VARCHAR', source_name='UOM'),
            Column('currency', 'VARCHAR', source_name='Currency'),
            Column('functional_currency', 'VARCHAR', source_name='FunctionalCurrency'),
            Column('conversion_rate', 'NUMBER(38,6)', source_name='ConversionRate'),
            Column('first_cost', 'NUMBER(38,6)', source_name='FirstCost'),
            Column('exit_first_cost', 'NUMBER(38,6)', source_name='ExitFirstCost'),
            Column('po_quantity', 'NUMBER(38,0)', source_name='POQty'),
            Column('entered_dr', 'NUMBER(38,6)', source_name='EnteredDR'),
            Column('entered_cr', 'NUMBER(38,6)', source_name='EnteredCR'),
            Column('accounted_dr', 'NUMBER(38,6)', source_name='AccountedDR'),
            Column('accounted_cr', 'NUMBER(38,6)', source_name='AccountedCR'),
            Column('inventory_ap_accrual', 'VARCHAR', source_name='InventoryAPAccrual'),
            Column('unit_ts_freight_cost', 'NUMBER(38,6)', source_name='Unit TS Freight Cost'),
            Column('unit_ts_duty_cost', 'NUMBER(38,6)', source_name='Unit TS Duty Cost'),
            Column('unit_ts_cmt', 'NUMBER(38,6)', source_name='Unit TS_CMT'),
            Column('unit_ts_tariff', 'NUMBER(38,6)', source_name='Unit TS_TARIFF'),
            Column('unit_ts_buyers_comm', 'NUMBER(38,6)', source_name='Unit TS_BUYERS_COMM'),
            Column('unit_ts_inspection', 'NUMBER(38,6)', source_name='Unit TS_INSPECTION'),
            Column('total_unit_lcm', 'NUMBER(38,6)', source_name='TotalUnitLCM'),
            Column('total_ts_freight_cost', 'NUMBER(38,6)', source_name='Total TS Freight Cost'),
            Column('total_ts_duty_cost', 'NUMBER(38,6)', source_name='Total TS Duty Cost'),
            Column('total_ts_cmt', 'NUMBER(38,6)', source_name='Total TS_CMT'),
            Column('total_ts_tariff', 'NUMBER(38,6)', source_name='Total TS_TARIFF'),
            Column('total_ts_buyers_comm', 'NUMBER(38,6)', source_name='Total TS_BUYERS_COMM'),
            Column('total_ts_inspection', 'NUMBER(38,6)', source_name='Total TS_INSPECTION'),
            Column('total_lcm', 'NUMBER(38,6)', source_name='TotalLCM'),
            Column('entered_dr_2', 'NUMBER(38,6)', source_name='EnteredDR'),
            Column('entered_cr_2', 'NUMBER(38,6)', source_name='EnteredCR'),
            Column('accounted_dr_2', 'NUMBER(38,6)', source_name='AccountedDR'),
            Column('accounted_cr_2', 'NUMBER(38,6)', source_name='AccountedCR'),
            Column('landed_cost_absorption', 'VARCHAR', source_name='LandedCostAbsorption'),
            Column('unit_landed_cost', 'NUMBER(38,6)', source_name='UnitLandedCost'),
            Column('total_landed_cost', 'NUMBER(38,6)', source_name='TotalLandedCost'),
        ],
    ),
    "shipment_data": SheetConfig(
        sheet_name='Report0',
        schema='excel',
        table='shipment_data_oocl',
        s3_replace=False,
        header_rows=1,
        column_list=[
            Column('business_unit', 'VARCHAR', source_name='Business Unit'),
            Column('jf_traffic_mode', 'VARCHAR', source_name='JF Traffic Mode'),
            Column('po', 'VARCHAR', source_name='PO', uniqueness=True),
            Column('line_num', 'VARCHAR', source_name='Line#'),
            Column('origin', 'VARCHAR', source_name='Origin'),
            Column('por', 'VARCHAR', source_name='POR'),
            Column('origin_country_region', 'VARCHAR', source_name='Origin Country/Region'),
            Column('cargo_fnd', 'VARCHAR', source_name='Cargo FND'),
            Column(
                'final_destination_country_region',
                'VARCHAR',
                source_name='Final Destination Country/Region',
            ),
            Column(
                'post_advice_number', 'VARCHAR', source_name='Post Advice Number', uniqueness=True
            ),
            Column('container', 'VARCHAR', source_name='Container', uniqueness=True),
            Column('bl', 'VARCHAR', source_name='BL', uniqueness=True),
            Column('size_type', 'VARCHAR', source_name='Size/Type'),
            Column('vendor_code', 'VARCHAR', source_name='Vendor Code'),
            Column('start_ship_window', 'DATE', source_name='Start Ship Window'),
            Column('end_ship_window', 'DATE', source_name='End Ship Window'),
            Column('showroom_date', 'DATE', source_name='Showroom Date'),
            Column('trading_terms_po', 'VARCHAR', source_name='Trading terms (PO)'),
            Column('transport_mode', 'VARCHAR', source_name='Transport Mode'),
            Column('business_type', 'VARCHAR', source_name='Business Type'),
            Column('style_color', 'VARCHAR', source_name='Style-Color', uniqueness=True),
            Column('description', 'VARCHAR', source_name='Description'),
            Column('hts_number', 'VARCHAR', source_name='HTS Number'),
            Column('units_ordered', 'NUMBER(38,2)', source_name='Units Ordered'),
            Column('units_booked', 'NUMBER(38,2)', source_name='Units Booked'),
            Column('units_shipped', 'NUMBER(38,2)', source_name='Units Shipped'),
            Column('packs_booked', 'NUMBER(38,2)', source_name='Packs Booked'),
            Column('unit_cost', 'NUMBER(38,2)', source_name='Unit Cost'),
            Column('pol', 'VARCHAR', source_name='POL'),
            Column('pol_country_region', 'VARCHAR', source_name='POL Country/Region'),
            Column('pod', 'VARCHAR', source_name='POD'),
            Column('pod_country_region', 'VARCHAR', source_name='POD Country/Region'),
            Column('total_item_volume_cbm', 'NUMBER(38,6)', source_name='Total Item Volume (CBM)'),
            Column('total_item_weight_kg', 'NUMBER(38,6)', source_name='Total Item Weight (kg)'),
            Column('carrier', 'VARCHAR', source_name='Carrier'),
            Column('traffic_mode', 'VARCHAR', source_name='Traffic Mode'),
            Column('pol_etd', 'DATE', source_name='POL/ETD'),
            Column('pod_eta', 'DATE', source_name='POD/ETA'),
            Column('launch_date', 'DATE', source_name='Launch Date'),
            Column(
                'vessel_voyage_of_all_routes', 'VARCHAR', source_name='Vessel/Voyage of All Routes'
            ),
        ],
    ),
    "dhl_invoice": SheetConfig(
        sheet_name=0,
        schema='excel',
        table='dhl_invoice',
        s3_replace=False,
        header_rows=1,
        column_list=[
            Column('billing_account_number', 'INT', source_name='Billing Account Number'),
            Column('billing_account_name', 'VARCHAR', source_name='Billing Account Name'),
            Column(
                'billing_account_opened_date', 'DATE', source_name='Billing Account Opened Date'
            ),
            Column(
                'billing_account_regional_segment_code',
                'VARCHAR',
                source_name='Billing Account Regional Segment Code',
            ),
            Column(
                'billing_account_address_line_1',
                'VARCHAR',
                source_name='Billing Account Address Line 1',
            ),
            Column(
                'billing_account_address_line_2',
                'VARCHAR',
                source_name='Billing Account Address Line 2',
            ),
            Column(
                'billing_account_address_city_name',
                'VARCHAR',
                source_name='Billing Account Address City Name',
            ),
            Column(
                'billing_account_address_state_code',
                'VARCHAR',
                source_name='Billing Account Address State Code',
            ),
            Column(
                'billing_account_postal_code', 'VARCHAR', source_name='Billing Account Postal Code'
            ),
            Column(
                'billing_account_top_level_code',
                'VARCHAR',
                source_name='Billing Account Top-Level Code',
            ),
            Column(
                'billing_account_top_level_name',
                'VARCHAR',
                source_name='Billing Account Top-Level Name',
            ),
            Column(
                'billing_account_top_level_type_code',
                'VARCHAR',
                source_name='Billing Account Top-Level Type Code',
            ),
            Column(
                'billing_account_top_level_type_name',
                'VARCHAR',
                source_name='Billing Account Top-Level Type Name',
            ),
            Column(
                'billing_account_mac_group_code',
                'VARCHAR',
                source_name='Billing Account MAC Group Code',
            ),
            Column(
                'billing_account_mac_group_name',
                'VARCHAR',
                source_name='Billing Account MAC Group Name',
            ),
            Column('billing_account_mac_code', 'VARCHAR', source_name='Billing Account MAC Code'),
            Column('billing_account_mac_name', 'VARCHAR', source_name='Billing Account MAC Name'),
            Column('billing_account_agc_code', 'VARCHAR', source_name='Billing Account AGC Code'),
            Column('billing_account_agc_name', 'VARCHAR', source_name='Billing Account AGC Name'),
            Column('billing_site_gsfa_id', 'VARCHAR', source_name='Billing Site GSFA ID'),
            Column('billing_site_name', 'VARCHAR', source_name='Billing Site Name'),
            Column(
                'billing_site_l1_sales_position_name',
                'VARCHAR',
                source_name='Billing Site L1 Sales Position Name',
            ),
            Column(
                'billing_site_l1_sales_person_name',
                'VARCHAR',
                source_name='Billing Site L1 Sales Person Name',
            ),
            Column('billing_parent_gsfa_id', 'VARCHAR', source_name='Billing Parent GSFA ID'),
            Column('billing_parent_name', 'VARCHAR', source_name='Billing Parent Name'),
            Column(
                'billing_parent_l4_sales_position_name',
                'VARCHAR',
                source_name='Billing Parent L4 Sales Position Name',
            ),
            Column(
                'billing_parent_l2_sales_position_name',
                'VARCHAR',
                source_name='Billing Parent L2 Sales Position Name',
            ),
            Column(
                'billing_parent_l1_sales_position_name',
                'VARCHAR',
                source_name='Billing Parent L1 Sales Position Name',
            ),
            Column(
                'billing_parent_l1_sales_person_name',
                'VARCHAR',
                source_name='Billing Parent L1 Sales Person Name',
            ),
            Column('shipment_date', 'DATE', source_name='Shipment Date'),
            Column('invoice_date', 'DATE', source_name='Invoice Date'),
            Column('rating_date', 'DATE', source_name='Rating Date'),
            Column('invoice_cycle', 'VARCHAR', source_name='Invoice Cycle'),
            Column('billing_cycle', 'VARCHAR', source_name='Billing Cycle'),
            Column('invoice_number', 'VARCHAR', source_name='Invoice Number'),
            Column('awb_number', 'INT', source_name='AWB Number'),
            Column('shipment_reference', 'VARCHAR', source_name='Shipment Reference'),
            Column('shipper_account_number', 'INT', source_name='Shipper Account Number'),
            Column('shipper_account_name', 'VARCHAR', source_name='Shipper Account Name'),
            Column('consignor_name', 'VARCHAR', source_name='Consignor Name'),
            Column('consignor_contact_name', 'VARCHAR', source_name='Consignor Contact Name'),
            Column('consignor_address_line_1', 'VARCHAR', source_name='Consignor Address Line 1'),
            Column('consignor_address_line_2', 'VARCHAR', source_name='Consignor Address Line 2'),
            Column('consignor_address_line_3', 'VARCHAR', source_name='Consignor Address Line 3'),
            Column('consignor_city_name', 'VARCHAR', source_name='Consignor City Name'),
            Column('consignor_state_code', 'VARCHAR', source_name='Consignor State Code'),
            Column('consignor_zip_code', 'VARCHAR', source_name='Consignor ZIP Code'),
            Column('consignor_country_code', 'VARCHAR', source_name='Consignor Country Code'),
            Column('consignor_country_name', 'VARCHAR', source_name='Consignor Country Name'),
            Column(
                'consignor_telephone_number', 'VARCHAR', source_name='Consignor Telephone Number'
            ),
            Column('consignee_name', 'VARCHAR', source_name='Consignee Name'),
            Column('consignee_contact_name', 'VARCHAR', source_name='Consignee Contact Name'),
            Column('consignee_address_line_1', 'VARCHAR', source_name='Consignee Address Line 1'),
            Column('consignee_address_line_2', 'VARCHAR', source_name='Consignee Address Line 2'),
            Column('consignee_address_line_3', 'VARCHAR', source_name='Consignee Address Line 3'),
            Column('consignee_city_name', 'VARCHAR', source_name='Consignee City Name'),
            Column('consignee_state_code', 'VARCHAR', source_name='Consignee State Code'),
            Column('consignee_zip_code', 'VARCHAR', source_name='Consignee ZIP Code'),
            Column('consignee_country_code', 'VARCHAR', source_name='Consignee Country Code'),
            Column('consignee_country_name', 'VARCHAR', source_name='Consignee Country Name'),
            Column(
                'consignee_telephone_number', 'VARCHAR', source_name='Consignee Telephone Number'
            ),
            Column('gm_area', 'VARCHAR', source_name='GM Area'),
            Column('direction', 'VARCHAR', source_name='Direction'),
            Column('origin_country_code', 'VARCHAR', source_name='Origin Country Code'),
            Column('origin_country_name', 'VARCHAR', source_name='Origin Country Name'),
            Column('origin_service_area_code', 'VARCHAR', source_name='Origin Service Area Code'),
            Column('origin_service_area_name', 'VARCHAR', source_name='Origin Service Area Name'),
            Column('destination_country_code', 'VARCHAR', source_name='Destination Country Code'),
            Column('destination_country_name', 'VARCHAR', source_name='Destination Country Name'),
            Column(
                'destination_service_area_code',
                'VARCHAR',
                source_name='Destination Service Area Code',
            ),
            Column(
                'destination_service_area_name',
                'VARCHAR',
                source_name='Destination Service Area Name',
            ),
            Column('major_product_code', 'VARCHAR', source_name='Major Product Code'),
            Column('super_product_code', 'VARCHAR', source_name='Super Product Code'),
            Column('local_product_code', 'VARCHAR', source_name='Local Product Code'),
            Column('local_product_name', 'VARCHAR', source_name='Local Product Name'),
            Column(
                'billing_account_inbound_tariff_code',
                'VARCHAR',
                source_name='Billing Account Inbound Tariff Code',
            ),
            Column(
                'billing_account_outbound_tariff_code',
                'VARCHAR',
                source_name='Billing Account Outbound Tariff Code',
            ),
            Column('pricing_zone_description', 'VARCHAR', source_name='Pricing Zone Description'),
            Column('weight_flag_code', 'VARCHAR', source_name='Weight Flag Code'),
            Column('weight_flag_name', 'VARCHAR', source_name='Weight Flag Name'),
            Column('currency_code', 'VARCHAR', source_name='Currency Code'),
            Column(
                'billing_account_source_country_code',
                'VARCHAR',
                source_name='Billing Account Source Country Code',
            ),
            Column(
                'first_checkpoint_datetime',
                'TIMESTAMP_NTZ',
                source_name='First Checkpoint Datetime',
            ),
            Column('tt_days', 'INT', source_name='TT Days'),
            Column(
                'network_completion_checkpoint_datetime',
                'TIMESTAMP_NTZ',
                source_name='Network Completion Checkpoint Datetime',
            ),
            Column(
                'network_completion_checkpoint_signatory',
                'VARCHAR',
                source_name='Network Completion Checkpoint Signatory',
            ),
            Column('published_revenue_lcy', 'NUMBER(38,4)', source_name='Published Revenue (LCY)'),
            Column(
                'billed_number_of_shipments',
                'NUMBER(38,4)',
                source_name='Billed Number of Shipments',
            ),
            Column(
                'billed_number_of_pieces', 'NUMBER(38,4)', source_name='Billed Number of Pieces'
            ),
            Column(
                'billed_total_revenue_lcy', 'NUMBER(38,4)', source_name='Billed Total Revenue (LCY)'
            ),
            Column('revenue_less_fuel', 'NUMBER(38,4)', source_name='Revenue Less Fuel'),
            Column(
                'weight_charge_gross_lcy', 'NUMBER(38,4)', source_name='Weight Charge Gross (LCY)'
            ),
            Column(
                'weight_charge_discount_lcy',
                'NUMBER(38,4)',
                source_name='Weight Charge Discount (LCY)',
            ),
            Column('weight_charge_net_lcy', 'NUMBER(38,4)', source_name='Weight Charge Net (LCY)'),
            Column(
                'total_billed_weight_lbs', 'NUMBER(38,4)', source_name='Total Billed Weight (LBS)'
            ),
            Column(
                'total_billed_weight_kg', 'NUMBER(38,4)', source_name='Total Billed Weight (KG)'
            ),
            Column(
                'commercial_customer_declared_weight_lbs',
                'NUMBER(38,4)',
                source_name='Commercial Customer Declared Weight (LBS)',
            ),
            Column(
                'commercial_customer_declared_volumetric_weight_lbs',
                'NUMBER(38,4)',
                source_name='Commercial Customer Declared Volumetric Weight (LBS)',
            ),
            Column(
                'commercial_dhl_reweigh_weight_lbs',
                'NUMBER(38,4)',
                source_name='Commercial DHL Reweigh Weight (LBS)',
            ),
            Column(
                'commercial_dhl_volumetric_weight_lbs',
                'NUMBER(38,4)',
                source_name='Commercial DHL Volumetric Weight (LBS)',
            ),
            Column(
                'surcharge_fuel_surcharge_gross_lcy',
                'NUMBER(38,4)',
                source_name='Surcharge Fuel Surcharge Gross (LCY)',
            ),
            Column(
                'surcharge_fuel_surcharge_net_lcy',
                'NUMBER(38,4)',
                source_name='Surcharge Fuel Surcharge Net (LCY)',
            ),
            Column(
                'product_surcharge_net_lcy',
                'NUMBER(38,4)',
                source_name='Product Surcharge Net (LCY)',
            ),
            Column(
                'surcharge_customs_services_net_lcy',
                'NUMBER(38,4)',
                source_name='Surcharge Customs Services Net (LCY)',
            ),
            Column(
                'surcharge_dangerous_goods_net_lcy',
                'NUMBER(38,4)',
                source_name='Surcharge Dangerous Goods Net (LCY)',
            ),
            Column(
                'surcharge_ddp_admin_net_lcy',
                'NUMBER(38,4)',
                source_name='Surcharge DDP Admin Net (LCY)',
            ),
            Column(
                'surcharge_gogreen_net_lcy',
                'NUMBER(38,4)',
                source_name='Surcharge GoGreen Net (LCY)',
            ),
            Column(
                'surcharge_insurance_net_lcy',
                'NUMBER(38,4)',
                source_name='Surcharge Insurance Net (LCY)',
            ),
            Column(
                'surcharge_license_net_lcy',
                'NUMBER(38,4)',
                source_name='Surcharge License Net (LCY)',
            ),
            Column(
                'surcharge_others_net_lcy', 'NUMBER(38,4)', source_name='Surcharge Others Net (LCY)'
            ),
            Column(
                'surcharge_over_size_piece_net_lcy',
                'NUMBER(38,4)',
                source_name='Surcharge Over Size Piece Net (LCY)',
            ),
            Column(
                'surcharge_over_weight_piece_net_lcy',
                'NUMBER(38,4)',
                source_name='Surcharge Over Weight Piece Net (LCY)',
            ),
            Column(
                'surcharge_periodic_fees_net_lcy',
                'NUMBER(38,4)',
                source_name='Surcharge Periodic Fees Net (LCY)',
            ),
            Column('surcharge_ras_net_lcy', 'NUMBER(38,4)', source_name='Surcharge RAS Net (LCY)'),
            Column(
                'surcharge_saturday_net_lcy',
                'NUMBER(38,4)',
                source_name='Surcharge Saturday Net (LCY)',
            ),
            Column(
                'surcharge_security_net_lcy',
                'NUMBER(38,4)',
                source_name='Surcharge Security Net (LCY)',
            ),
            Column(
                'surcharge_unmapped_net_lcy',
                'NUMBER(38,4)',
                source_name='Surcharge Unmapped Net (LCY)',
            ),
            Column('extra_charge_net_lcy', 'NUMBER(38,4)', source_name='Extra Charge Net (LCY)'),
            Column('insured_value_lcy', 'NUMBER(38,4)', source_name='Insured Value (LCY)'),
            Column('tax_discount_lcy', 'NUMBER(38,4)', source_name='Tax Discount (LCY)'),
            Column('shipment_tax_lcy', 'NUMBER(38,4)', source_name='Shipment Tax (LCY)'),
            Column('tax_others_amount_lcy', 'NUMBER(38,4)', source_name='Tax Others Amount (LCY)'),
            Column(
                'surcharge_duty_net_lcy', 'NUMBER(38,4)', source_name='Surcharge Duty Net (LCY)'
            ),
            Column('surcharge_tax_net_lcy', 'NUMBER(38,4)', source_name='Surcharge Tax Net (LCY)'),
        ],
        dtype={
            'Consignor ZIP Code': 'str',
            'Consignee ZIP Code': 'str',
            'Invoice Number': 'str',
            'Billing Account Number': 'str',
            'AWB Number': 'str',
        },
    ),
    "shipment_data_domestic": SheetConfig(
        sheet_name='Report0',
        schema='excel',
        table='shipment_data_domestic_oocl',
        s3_replace=False,
        header_rows=1,
        column_list=[
            Column('business_unit', 'VARCHAR', source_name='Business Unit'),
            Column('carrier', 'VARCHAR', source_name='Carrier'),
            Column('jf_traffic_mode', 'VARCHAR', source_name='JF Traffic Mode'),
            Column('traffic_mode', 'VARCHAR', source_name='Traffic Mode'),
            Column('po', 'VARCHAR', source_name='PO', uniqueness=True),
            Column('origin', 'VARCHAR', source_name='Origin'),
            Column('por', 'VARCHAR', source_name='POR'),
            Column('origin_country_region', 'VARCHAR', source_name='Origin Country/Region'),
            Column('cargo_fnd', 'VARCHAR', source_name='Cargo FND'),
            Column(
                'final_destination_country_region',
                'VARCHAR',
                source_name='Final Destination Country/Region',
            ),
            Column('origin_container', 'VARCHAR', source_name='Origin Container', uniqueness=True),
            Column('domestic_bl', 'VARCHAR', source_name='Domestic BL', uniqueness=True),
            Column('domestic_trailer', 'VARCHAR', source_name='Domestic Trailer'),
            Column('size_type', 'VARCHAR', source_name='Size/Type'),
            Column('vendor_code', 'VARCHAR', source_name='Vendor Code'),
            Column('start_ship_window', 'DATE', source_name='Start Ship Window'),
            Column('end_ship_window', 'DATE', source_name='End Ship Window'),
            Column('showroom_date', 'DATE', source_name='Showroom Date'),
            Column('trading_terms', 'VARCHAR', source_name='Trading terms'),
            Column('transport_mode', 'VARCHAR', source_name='Transport Mode'),
            Column('business_type', 'VARCHAR', source_name='Business Type'),
            Column('style_color', 'VARCHAR', source_name='Style-Color', uniqueness=True),
            Column('destination', 'VARCHAR', source_name='Destination'),
            Column('units_booked', 'NUMBER(38,2)', source_name='Units Booked'),
            Column('units_ordered', 'NUMBER(38,2)', source_name='Units Ordered'),
            Column('units_shipped', 'NUMBER(38,2)', source_name='Units Shipped'),
            Column('pre_advice_number', 'INTEGER', source_name='Pre Advice Number'),
            Column(
                'post_advice_number', 'INTEGER', source_name='Post Advice Number', uniqueness=True
            ),
            Column('pol', 'VARCHAR', source_name='POL'),
            Column('pol_country_region', 'VARCHAR', source_name='POL Country/Region'),
            Column('pod', 'VARCHAR', source_name='POD'),
            # Column('pod_country_region', 'VARCHAR', source_name='POD Country/Region'),
            Column('total_item_volume_cbm', 'NUMBER(38,6)', source_name='Total Item Volume (CBM)'),
            Column('total_item_weight_kg', 'NUMBER(38,6)', source_name='Total Item Weight (kg)'),
            Column('vol_cbm', 'NUMBER(38,6)', source_name='VOL (CBM)'),
            Column('gwt_pound', 'NUMBER(38,6)', source_name='GWT(Pound)'),
            Column('line_num', 'VARCHAR', source_name='Line#'),
        ],
    ),
}
email_list = email_lists.data_integration_support + [
    'mgarza@techstyle.com',
    'calcorta@techstyle.com',
]

default_args = {
    'start_date': pendulum.datetime(2021, 2, 1, tz='America/Los_Angeles'),
    'retries': 2,
    'owner': owners.data_integrations,
    'email': email_list,
    'on_failure_callback': slack_failure_gsc,
}


dag = DAG(
    dag_id='global_apps_inbound_landed_cost',
    default_args=default_args,
    schedule='1 * * * *',
    catchup=False,
    max_active_tasks=100,
    max_active_runs=1,
)


@dataclass
class ExcelConfig:
    task_id: str
    smb_dir: str
    sheet_config: Any
    default_schema_version: str = "v2"
    load_operator: Type[
        Union[SnowflakeInsertOperator, SnowflakeCopyTableOperator, SnowflakeProcedureOperator]
    ] = SnowflakeInsertOperator

    @property
    def to_s3(self):
        return ExcelSMBToS3BatchOperator(
            task_id=f"{self.task_id}_excel_to_s3",
            smb_dir=self.smb_dir,
            share_name='BI',
            file_pattern_list=['*.xls*', '*.XLS*'],
            bucket=s3_buckets.tsos_da_int_inbound,
            s3_conn_id=conn_ids.S3.tsos_da_int_prod,
            smb_conn_id=conn_ids.SMB.nas01,
            is_archive_file=True,
            sheet_configs=[self.sheet_config],
            remove_header_new_lines=True,
            default_schema_version=self.default_schema_version,
        )

    @property
    def to_snowflake(self):
        if self.load_operator == SnowflakeCopyTableOperator:
            return SnowflakeCopyTableOperator(
                task_id=f"{self.task_id}_s3_to_snowflake",
                database='lake',
                schema=self.sheet_config.schema,
                table=self.sheet_config.table,
                warehouse='DA_WH_ETL_LIGHT',
                snowflake_conn_id=conn_ids.Snowflake.default,
                column_list=self.sheet_config.column_list,
                files_path=f"{stages.tsos_da_int_inbound}/lake/{self.sheet_config.schema}.{self.sheet_config.table}/{self.default_schema_version}/",  # noqa: E501
                copy_config=CopyConfigCsv(header_rows=1, field_delimiter='|'),
            )
        elif self.load_operator == SnowflakeProcedureOperator:
            return SnowflakeProcedureOperator(
                procedure=f"{self.sheet_config.schema}.{self.sheet_config.table}.sql",
                database='lake',
                warehouse='DA_WH_ETL_LIGHT',
            )
        else:
            return SnowflakeInsertOperator(
                task_id=f"{self.task_id}_s3_to_snowflake",
                database='lake',
                schema=self.sheet_config.schema,
                table=self.sheet_config.table,
                staging_database='lake_stg',
                view_database='lake_view',
                snowflake_conn_id=conn_ids.Snowflake.default,
                column_list=self.sheet_config.column_list,
                files_path=f"{stages.tsos_da_int_inbound}/lake/{self.sheet_config.schema}.{self.sheet_config.table}/{self.default_schema_version}",  # noqa: E501
                copy_config=CopyConfigCsv(header_rows=1, field_delimiter='|'),
            )


smb_path = 'Inbound/airflow.landed_cost'

landed_cost_config = [
    ExcelConfig(
        task_id='broker_oocl',
        smb_dir=f"{smb_path}/broker_data/oocl",
        sheet_config=sheets['oocl'],
        default_schema_version="v2",
    ),
    ExcelConfig(
        task_id='broker_carmichael',
        smb_dir=f"{smb_path}/broker_data/carmichael",
        sheet_config=sheets['carmichael'],
        default_schema_version="v2",
    ),
    ExcelConfig(
        task_id='cmt_labor_cost',
        smb_dir=f"{smb_path}/cmt_labor_cost",
        sheet_config=sheets['cmt_labor_cost'],
        load_operator=SnowflakeCopyTableOperator,
    ),
    ExcelConfig(
        task_id='shipment_data_oocl',
        smb_dir=f"{smb_path}/shipment_data/international",
        sheet_config=sheets['shipment_data'],
        default_schema_version="v3",
        load_operator=SnowflakeProcedureOperator,
    ),
    ExcelConfig(
        task_id='shipment_data_domestic_oocl',
        smb_dir=f"{smb_path}/shipment_data/domestic",
        sheet_config=sheets['shipment_data_domestic'],
        default_schema_version="v3",
        load_operator=SnowflakeProcedureOperator,
    ),
    ExcelConfig(
        task_id='dhl_invoice',
        smb_dir=f"{smb_path}/small_parcel/dhl",
        sheet_config=sheets['dhl_invoice'],
    ),
]


def check_execution_time(**kwargs):
    execution_time = kwargs['data_interval_end'].in_timezone('America/Los_Angeles')
    if execution_time.hour == 6:
        return decide_sql_execution.task_id
    else:
        return []


with dag:
    email_to_smb = EmailToSMBOperator(
        task_id='email_to_smb',
        remote_path=f"{smb_path}/small_parcel/dhl",
        smb_conn_id=conn_ids.SMB.nas01,
        from_address='@dhl',
        resource_address='dhl_waybill@techstyle.net',
        share_name="BI",
    )
    decide_sql_execution = BranchPythonOperator(
        python_callable=check_execution_time, task_id='check_time_sql_execution'
    )

    oocl_shipment_to_mssql = SnowflakeToMSSqlFastMany(
        task_id='oocl_shipment_dataset_to_mssql',
        sql_or_path="""SELECT
                JF_TRAFFIC_MODE,
                TRAFFIC_MODE,
                PO_NUMBER,
                ITEM_NUMBER,
                POST_ADVICE_NUMBER,
                CONTAINER,
                BL,
                SIZE,
                POR,
                ORIGIN,
                ORIGIN_COUNTRY,
                POL,
                POD,
                FINAL_DESTINATION_COUNTRY_REGION,
                TRADING_TERMS,
                TRANSPORT_MODE,
                UNITS_ORDERED,
                UNITS_SHIPPED,
                UNIT_COST,
                TOTAL_ITEM_VOLUME_CBM,
                TOTAL_ITEM_WEIGHT_KG,
                CARRIER,
                POL_ETD,
                POD_ETA,
                LAUNCH_DATE,
                VESSEL_VOYAGE
            FROM
                REPORTING_BASE_PROD.GSC.LC_OOCL_SHIPMENT_DATASET""",
        tgt_table='oocl_shipment_dataset',
        tgt_database='ultrawarehouse',
        tgt_schema='rpt',
        mssql_conn_id=conn_ids.MsSql.dbp40_app_airflow_rw,
        if_exists='append',
    )

    for cfg in landed_cost_config:
        to_s3 = cfg.to_s3
        to_snowflake = cfg.to_snowflake
        email_to_smb >> to_s3 >> to_snowflake >> decide_sql_execution >> oocl_shipment_to_mssql
