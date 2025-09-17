CREATE OR REPLACE FUNCTION reporting_media_base_prod.dbo.udf_decode_url (
ENCODED_URL  VARCHAR
  )
  RETURNS VARCHAR
  LANGUAGE JAVASCRIPT
AS $$
    try {
       result = decodeURIComponent(ENCODED_URL);
    }
    catch (err) {
      result = ENCODED_URL;
    }
return result;
$$
;
