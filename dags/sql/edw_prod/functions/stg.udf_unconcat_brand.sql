CREATE OR REPLACE FUNCTION stg.udf_unconcat_brand(concatenated_field INT)
RETURNS INT
AS
  $$
  SELECT CASE WHEN substring(concatenated_field, -2) IN ('10', '20', '30', '40')
  THEN TRY_CAST(regexp_replace(concatenated_field, '..$', '') AS INT)
  ELSE concatenated_field END
  $$
;
