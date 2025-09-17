CREATE TRANSIENT TABLE IF NOT EXISTS month_end.dates_for_fin008
(
    credit_active_as_of_date DATE,
    credit_issued_max_date   DATE,
    vip_cohort_max_date      DATE,
    member_status_date       DATE,
    num_for_recursion        NUMBER(38, 4)
);
