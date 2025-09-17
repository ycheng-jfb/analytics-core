CREATE OR REPLACE VIEW data_model_fl.dim_refund_comment
(
    refund_comment_key,
    refund_comment,
    meta_create_datetime,
    meta_update_datetime
) AS
SELECT
    refund_comment_key,
    refund_comment,
    meta_create_datetime,
    meta_update_datetime
FROM stg.dim_refund_comment;
