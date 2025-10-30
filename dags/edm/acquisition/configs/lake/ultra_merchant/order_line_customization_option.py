from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='order_line_customization_option',
    watermark_column='datetime_added',
    column_list=[
        Column('order_line_customization_option_id', 'INT', uniqueness=True),
        Column('order_line_customization_id', 'INT'),
        Column('order_id', 'INT'),
        Column('order_line_id', 'INT'),
        Column('customization_template_id', 'INT'),
        Column('customization_location_id', 'INT'),
        Column('file_name', 'VARCHAR(255)'),
        Column('color_choice', 'VARCHAR(50)'),
        Column('font_choice', 'VARCHAR(50)'),
        Column('icon_choice', 'VARCHAR(50)'),
        Column('text_line_1', 'VARCHAR(255)'),
        Column('text_line_2', 'VARCHAR(255)'),
        Column('text_line_3', 'VARCHAR(255)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)', delta_column=0),
    ],
)
