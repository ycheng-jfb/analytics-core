CREATE TABLE if NOT EXISTSreporting_media_base_prod.dbo.important_channels_and_accounts
(
    level VARCHAR,
    channel VARCHAR,
    id VARCHAR
);


INSERT
    INTO reporting_media_base_prod.dbo.important_channels_and_accounts(level, channel, id)
    VALUES ('id', 'facebook', '10160516591735508'),
           ('id', 'facebook', '444959489669305'),
           ('id', 'facebook', '549380765919307'),
           ('id', 'facebook', '383142662245543'),
           ('id', 'facebook', '582501782316064'),
           ('id', 'facebook', '291760788147373'),
           ('id', 'facebook', '106957382783892'),
           ('id', 'facebook', '474944557472303'),
           ('id', 'google', '21700000001232575'),
           ('id', 'google', '21700000001497640'),
           ('id', 'google', '21700000001224429'),
           ('id', 'google', '21700000001232881'),
           ('id', 'google', '21700000001228976'),
           ('id', 'google', '21700000001345359'),
           ('id', 'google', '21700000001345356'),
           ('id', 'google', '21700000001498331'),
           ('id', 'google', '21700000001338699'),
           ('id', 'google', '21700000001345350'),
           ('channel', 'fb+ig', NULL),
           ('channel', 'Non Branded Search', NULL),
           ('channel', 'Branded Search', NULL),
           ('channel', 'Shopping', NULL),
           ('channel', 'Programmatic-GDN', NULL)
;
