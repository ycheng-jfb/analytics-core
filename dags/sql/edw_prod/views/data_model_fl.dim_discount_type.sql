CREATE OR REPLACE VIEW data_model_fl.dim_discount_type
            (
             discount_type_id,
             discount_type_label,
             discount_type_description,
             meta_create_datetime,
             meta_update_datetime
                )
AS
SELECT discount_type_id,
       discount_type_label,
       discount_type_description,
       meta_create_datetime,
       meta_update_datetime
FROM stg.dim_discount_type;
