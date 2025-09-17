TRUNCATE TABLE reference.order_payment_statuscode_rank;

INSERT INTO reference.order_payment_statuscode_rank
(
    statuscode_rank,
    payment_statuscode,
    order_payment_status,
    rank_category
)
VALUES  (1, 2600, 'Paid', 'Success'),
        (2, 2650, 'Refunded', 'Success'),
        (3, 2651, 'Refunded (Partial)', 'Success'),
        (4, 2550, 'Authorized', 'Pending'),
        (5, 2555, 'Capture (In Progress)', 'Pending'),
        (6, 2521, 'Credit Owed', 'Pending'),
        (7, 2520, 'Balance Due', 'Pending'),
        (8, 2564, 'Settlement Failed', 'Failure'),
        (9, 2556, 'Capture Failed', 'Failure'),
        (10, 2510, 'Authorization Failed', 'Failure'),
        (11, 2552, 'Authorization Expired', 'Failure'),
        (12, 2513, 'Prepaid Failed', 'Failure'),
        (13, 2512, 'Gateway Unavailable', 'Failure'),
        (15, 2511, 'AVS Failed', 'Failure'),
        (16, 2500, 'Unpaid', '');
