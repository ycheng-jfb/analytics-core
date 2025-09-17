CREATE OR REPLACE FUNCTION lake.public.udtf_eff_customer(customer_id STRING)
    RETURNS TABLE (
        effective_customer_id STRING
    )
    LANGUAGE JAVASCRIPT
    STRICT
    VOLATILE
AS
'
/*

Within partition, keeps track of the last non-unknown customer id.
Only returns non-null value when customer id is unknown and there is a prior non-unknown
customer id within the partition.
If customer_id = "-2" then return null.  Otherwise, return last non-unknown customer id.
In practice this is used to determine the effective customer id for an anonymous session.
This is accomblished by partitioning by visitor id and ordering by session time descending.

*/
{
    initialize: function() {
        this.customer_id = null;
    },

    processRow: function (row, rowWriter, context) {
        if (row.CUSTOMER_ID != null) {

            this.customer_id = row.CUSTOMER_ID;
            rowWriter.writeRow({EFFECTIVE_CUSTOMER_ID:  null});
        }
        else {
            rowWriter.writeRow({EFFECTIVE_CUSTOMER_ID:  this.customer_id});
        }
    },

   finalize: function (rowWriter, context) {/*...*/},
}
';
