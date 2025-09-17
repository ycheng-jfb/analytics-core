CREATE OR REPLACE VIEW reporting.retail.tableau_retail_transaction_issues AS
(
WITH
_retail_dupes AS (
    SELECT
        ht.status,
        ht.issuerResponseText,
        ht.issuerResponseCode,
        ht.name,
        ht.currency,
        ht.amount,
        ht.tipAmount,
        ht.cardType,
        ht.tenderType,
        ht.paymentScenario,
        ht.merchantName,
        ht.terminalSerialNumber,
        ht.maskedCardNumber,
        ht.timestamp,
        ht.protocol,
        ht.transactionGuid,
        ht.approvalCode,
        CASE WHEN ht.customerReference = 'UPDATE ORDER' THEN NULL ELSE ht.customerReference END AS customerReference ,
        ht.maskedCardToken,
        ht.tokenizationType,
        ht.cardTokenProvider
    FROM lake_view.fabletics.handpoint_transactions ht
    WHERE ht.issuerResponseText = 'APPROVED'
        AND ht.name NOT IN ('EMV Refund', 'MSR Refund', 'Sale Reversal', 'Sale Cancellation')
        AND customerReference IS NOT NULL
        AND NOT EXISTS (
            SELECT 1
            FROM lake_view.fabletics.handpoint_transactions h
            WHERE ht.customerReference = h.customerReference
            AND h.name IN ('Sale Reversal', 'Sale Cancellation')
        )
        AND NOT EXISTS (
            SELECT 1
            FROM lake_view.fabletics.handpoint_transactions h
            WHERE h.name IN ('Sale Reversal', 'Sale Cancellation')
            AND ht.approvalCode = h.approvalCode
            AND ht.maskedCardNumber = h.maskedCardNumber
            AND ht.amount = h.amount
        )
)
SELECT DISTINCT o.order_id,
    o.datetime_added AS order_datetime,
    s.statuscode AS order_processing_statuscode,
    s.label AS order_processing_status,
    (o.subtotal + o.tax + o.shipping - o.discount - o.credit) as order_total,
    c.customer_id,
    a2.administrator_id AS associate_administrator_id,
    a2.firstname AS associate_firstname,
    a2.lastname AS associate_lastname,
    rd.status,
    rd.issuerResponseText,
    rd.issuerResponseCode,
    rd.name,
    rd.currency,
    rd.amount,
    rd.tipAmount,
    rd.cardType,
    rd.tenderType,
    rd.paymentScenario,
    rd.merchantName,
    rd.terminalSerialNumber,
    rd.maskedCardNumber,
    rd.timestamp,
    rd.protocol,
    rd.transactionGuid,
    rd.approvalCode,
    rd.customerReference,
    rd.maskedCardToken,
    rd.tokenizationType,
    rd.cardTokenProvider,
    CURRENT_TIMESTAMP AS meta_update_datetime
FROM _retail_dupes rd
JOIN lake_view.ultra_merchant.payment_transaction_creditcard ptc ON rd.customerReference = ptc.payment_transaction_id
JOIN lake_view.ultra_merchant."ORDER" o ON ptc.order_id = o.order_id
JOIN lake_view.ultra_merchant.customer c ON o.customer_id = c.customer_id
JOIN lake_view.ultra_merchant.statuscode s ON o.processing_statuscode = s.statuscode
LEFT JOIN lake_view.ultra_merchant.order_tracking_detail otd ON o.order_tracking_id = otd.order_tracking_id
    AND otd.object = 'administrator'
LEFT JOIN lake_view.ultra_merchant.administrator a2  ON otd.object_id = a2.administrator_id
LEFT JOIN lake_view.ultra_merchant."ORDER" ord ON ord.capture_payment_transaction_id = rd.customerReference
    AND ord.order_source_id = 5
    AND ord.payment_method = 'creditcard'
    AND ord.capture_payment_transaction_id IS NOT NULL
WHERE ord.capture_payment_transaction_id IS NULL -- filter out approved handpoint transactions that we don't see attached to an order
    AND ptc.transaction_type = 'SALE_REDIRECT' -- filter out what I assume is earlier test transactions with handpoint (AUTH, PRIOR_AUTH_CAPTURE)?
    AND o.datetime_added > '2020-07-15 14:00:00.000'
ORDER BY o.datetime_added
  );
