CREATE OR REPLACE FUNCTION month_end.udf_correct_state_country(state VARCHAR(75), country VARCHAR(6),
                                                            field_to_update VARCHAR(10))
    RETURNS VARCHAR(75)
AS
$$
    SELECT CASE
               WHEN field_to_update = 'country'
                   THEN
                   CASE
                       WHEN UPPER(country) = 'CA'
                           THEN (CASE
                                     WHEN UPPER(state) IN
                                          (
                                           'AA', 'AE', 'AK', 'AL', 'AP', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL',
                                           'GA', 'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME', 'MI',
                                           'MN', 'MO', 'MS', 'MT', 'NC', 'NE', 'NH', 'NJ', 'NV', 'NY', 'OH', 'OK', 'OR',
                                           'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VA', 'VT', 'WA', 'WI', 'WV', 'WY'
                                              ) THEN 'US'
                                     WHEN UPPER(state) = 'NEW' THEN 'AU'
                                     ELSE UPPER(country) END)
                       WHEN UPPER(country) = 'US' THEN (
                           CASE
                               WHEN UPPER(state) IN
                                    ('AB', 'BC', 'MB', 'NB', 'NL', 'NS', 'NT', 'NU', 'PE', 'QC', 'ON', 'SK', 'YT')
                                   THEN 'CA'
                               WHEN UPPER(state) = 'NSW' THEN 'AU'
                               ELSE UPPER(country)
                               END)
                       WHEN UPPER(country) = 'AU' THEN (CASE
                                                            WHEN UPPER(state) IN
                                                                 ('AA', 'AK', 'AZ', 'CA', 'CO', 'FL', 'HI', 'IN', 'MA',
                                                                  'ME', 'MI', 'MN', 'NC', 'PA', 'NE', 'NJ', 'NV', 'NY',
                                                                  'OR', 'PR', 'TX', 'TX-', 'VA', 'VI', 'WES')
                                                                THEN 'US'
                                                            WHEN UPPER(state) IN ('QUE', 'NOR') THEN 'CA'
                                                            ELSE UPPER(country)
                           END)
                       ELSE UPPER(country) END
               WHEN field_to_update = 'state' THEN
                   CASE
                       WHEN UPPER(country) = 'CA'
                           THEN (
                           CASE
                               WHEN UPPER(state) IN ('--', 'ALB') THEN 'AB'
                               WHEN UPPER(state) = 'BRI' THEN 'BC'
                               WHEN UPPER(state) IN ('LB', 'MAN') THEN 'NB'
                               WHEN UPPER(state) IN ('NF', 'NM') THEN 'NL'
                               WHEN UPPER(state) IN ('NW', 'NSW') THEN 'NT'
                               WHEN UPPER(state) = 'NEW' THEN 'NSW'
                               WHEN UPPER(state) IN ('NOV', 'NOR') THEN 'NS'
                               WHEN UPPER(state) = 'EU' THEN 'NU'
                               WHEN UPPER(state) = 'ONT' THEN 'ON'
                               WHEN UPPER(state) = 'PR' THEN 'PE'
                               WHEN UPPER(state) IN ('QLD', 'QUÃ', 'QUE') THEN 'QC'
                               WHEN UPPER(state) IN ('SA', 'SAS') THEN 'SK'
                               WHEN UPPER(state) = 'YU' THEN 'YT'
                               ELSE UPPER(state)
                               END
                           )
                       WHEN UPPER(country) = 'US'
                           THEN (
                           CASE
                               WHEN UPPER(state) IN ('AU', 'AF', 'GB') THEN 'AE'
                               WHEN UPPER(state) = 'JP' THEN 'AP'
                               WHEN UPPER(state) = 'AS' THEN 'AR'
                               WHEN UPPER(state) IN
                                    ('--', 'BG', 'BN', 'CAL', 'CQ', 'CS', 'EL', 'EU', 'ES', 'US', 'US ', 'U', 'SP',
                                     'ST', 'ST ', 'XB', 'XB ', 'XX', 'XX ', 'N/A', '') THEN 'CA'
                               WHEN UPPER(state) IN ('BA', 'FM', 'FR') THEN 'FL'
                               WHEN UPPER(state) IN ('GE', 'GU', '-- ') THEN 'GA'
                               WHEN UPPER(state) = 'IDA' THEN 'ID'
                               WHEN UPPER(state) IN ('EL', 'EL ') THEN 'IL'
                               WHEN UPPER(state) = 'IO' THEN 'IA'
                               WHEN UPPER(state) = 'OHI' THEN 'OH'
                               WHEN UPPER(state) = 'OKL' THEN 'OK'
                               WHEN UPPER(state) = 'KL' THEN 'OR'
                               WHEN UPPER(state) = 'EL' THEN 'IL'
                               WHEN UPPER(state) = 'PL' THEN 'PA'
                               WHEN UPPER(state) = 'RH' THEN 'RI'
                               WHEN UPPER(state) = 'LS' THEN 'LA'
                               WHEN UPPER(state) IN ('MH', 'MM') THEN 'MI'
                               WHEN UPPER(state) = 'MP' THEN 'MO'
                               WHEN UPPER(state) IN ('N. ', 'NA') THEN 'NC'
                               WHEN UPPER(state) IN ('N', 'N ') THEN 'NY'
                               WHEN UPPER(state) = 'PW' THEN 'PR'
                               WHEN UPPER(state) = 'TG' THEN 'TN'
                               WHEN UPPER(state) IN ('DF', 'TC', 'TC ') THEN 'TX'
                               WHEN UPPER(state) IN ('U.', 'UM', 'US') THEN 'UT'
                               WHEN UPPER(state) IN ('WAS', 'WAS ') THEN 'WA'
                               WHEN UPPER(state) IN
                                    ('AA ', 'AE ', 'AZ ', 'CA ', 'CO ', 'DC ', 'DE ', 'FL ', 'LA ', 'MI ', 'MD ', 'ND ',
                                     'NJ ', 'OH ', 'PA ', 'SC ', 'TN ', 'TX ', 'UT ', 'VA ', 'VI ', 'VT ', 'WA ', 'WV ',
                                     'WY ', 'WI ')
                                   THEN REPLACE(UPPER(state), ' ', '')
                               ELSE UPPER(state)
                               END
                           )
                       WHEN UPPER(country) = 'AU'
                           THEN (
                           CASE
                               WHEN UPPER(state) = 'NEW' THEN 'NSW'
                               WHEN UPPER(state) = 'QUE' THEN 'QC'
                               WHEN UPPER(state) = 'SOU' THEN 'SA'
                               WHEN UPPER(state) = 'TX-' THEN 'TX'
                               WHEN UPPER(state) IN ('CX', 'WES') THEN 'WA'
                               WHEN UPPER(state) IN ('-', '--', 'AUS') THEN 'ACT'
                               WHEN UPPER(state) = 'VT' THEN 'VIC'
                               WHEN UPPER(state) = 'NOR' THEN 'NF'
                               ELSE UPPER(state)
                               END)
                       ELSE UPPER(state)
                       END
               END
$$
;
