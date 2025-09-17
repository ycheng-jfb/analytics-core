TRUNCATE TABLE reference.gp_db_currency_mapping;
INSERT INTO reference.gp_db_currency_mapping
(
    db,
    iso_currency_code
)
VALUES
    ('SD', 'USD'),
    ('DTNA', 'USD'),
    ('JFEU', 'EUR'),
    ('JFCA', 'USD'),
    ('SWE', 'SEK'),
    ('JKD', 'USD'),
    ('FBL', 'USD'),
    ('JFFS', 'EUR'),
    ('JFAB', 'USD'),
    ('JFUK', 'GBP'),
    ('DEN', 'DKK');
