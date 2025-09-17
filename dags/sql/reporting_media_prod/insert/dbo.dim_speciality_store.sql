create or replace transient table reporting_media_prod.dbo.dim_speciality_store like edw_prod.data_model.dim_store;

insert into reporting_media_prod.dbo.dim_speciality_store
 values (3601, -2, 'JustFab AT', 'JustFab AT', 'JustFab EU', 'JustFab AT', 'JustFab', 'JF', 'Online', 'Store', FALSE, 'AT', 'EU', 'Fast Fashion', 'FF', 'EUR', 'Central Europe Standard Time', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', current_timestamp, current_timestamp),
        (4601, -2, 'FabKids CA', 'FabKids CA', 'FabKids NA', 'FabKids CA', 'FabKids', 'FK', 'Online', 'Store', FALSE, 'CA', 'NA', 'FabKids', 'FK', 'USD', 'Pacific Standard Time', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', current_timestamp, current_timestamp),
        (5201, -2, 'Fabletics AU', 'Fabletics AU', 'Fabletics NA', 'Fabletics AU', 'Fabletics', 'FL', 'Online', 'Store', FALSE, 'AU', 'NA', 'Fabletics', 'FL', 'USD', 'Australian Central Time', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', current_timestamp, current_timestamp),
        (5501, -2, 'ShoeDazzle CA', 'ShoeDazzle CA', 'ShoeDazzle NA', 'ShoeDazzle CA', 'ShoeDazzle', 'SD', 'Online', 'Store', FALSE, 'CA', 'NA', 'Fast Fashion', 'FF', 'USD', 'Pacific Standard Time', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', current_timestamp, current_timestamp),
        (5901, -2, 'JustFab BE', 'JustFab BE', 'JustFab EU', 'JustFab BE', 'JustFab', 'JF', 'Online', 'Store', FALSE, 'BE', 'EU', 'Fast Fashion', 'FF', 'EUR', 'Central Europe Standard Time', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', current_timestamp, current_timestamp),
        (6501, -2, 'Fabletics AT', 'Fabletics AT', 'Fabletics EU', 'Fabletics AT', 'Fabletics', 'FL', 'Online', 'Store', FALSE, 'AT', 'EU', 'Fabletics', 'FL', 'EUR', 'Central Europe Standard Time', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', current_timestamp, current_timestamp)
        ;
