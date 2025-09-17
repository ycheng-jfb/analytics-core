TRUNCATE TABLE reference.order_processing_statuscode_rank;

INSERT INTO reference.order_processing_statuscode_rank
(
    statuscode_rank,
    processing_statuscode,
    order_processing_status,
    rank_category
)
VALUES
(1, 2100, 'Shipped', 'Success'),
(2, 2110, 'Complete', 'Success'),
(3, 2099, 'Partially Shipped', 'Success'),
(4, 2080, 'Ready For Pickup', 'Pending'),
(5, 2060, 'FulFillment (Batching)', 'Pending'),
(6, 2065, 'FulFillment (In Progress)', 'Pending'),
(7, 2050, 'Placed', 'Pending'),
(8, 2350, 'Hold (Manual Review - Group 1)', 'Pending'),
(9, 2342, 'Hold (Preorder)', 'Pending'),
(10, 2020, 'Authorizing Payment', 'Pending'),
(11, 2000, 'Initializing', 'Pending'),
(12, 2361, 'Hold (Exchange)', 'On Hold'),
(13, 2360, 'Hold (Exchange - Reserve Inventory)', 'On Hold'),
(14, 2362, 'Hold (Exchange - Failed Inventory Check)', 'On Hold'),
(15, 2345, 'Hold (Defer Authorization)', 'On Hold'),
(16, 2310, 'Hold (Payment Problem)', 'On Hold'),
(17, 2305, 'Hold (Problem Resolution)', 'On Hold'),
(18, 2301, 'Hold (Fulfillment Problem)', 'On Hold'),
(19, 2351, 'Hold (Manual Review - SXF YOU)', 'On Hold'),
(20, 2300, 'Hold', 'On Hold'),
(21, 2335, 'Hold (Test Order)', 'On Hold'),
(22, 2030, 'Placement Failed', 'Failure'),
(23, 2200, 'Cancelled', 'Cancelled'),
(24, 2202, 'Cancelled (Incomplete Auth Redirect)', 'Cancelled'),
(25, 2067, 'FulFillment (Failed Inventory Check)', 'Cancelled'),
(26, 2130, 'Split', 'Split'),
(27, 2140, 'Split (For BOPS)', 'Split');
