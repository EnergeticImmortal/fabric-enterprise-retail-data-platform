USE WideWorldImporters;
GO

-- Enable CDC on the database
EXEC sys.sp_cdc_enable_db;
GO

-- Enable CDC on the two pipeline tables
EXEC sys.sp_cdc_enable_table
    @source_schema = N'Sales', @source_name = N'Orders',
    @role_name = NULL, @supports_net_changes = 1;
GO

EXEC sys.sp_cdc_enable_table
    @source_schema = N'Sales', @source_name = N'OrderLines',
    @role_name = NULL, @supports_net_changes = 1;
GO

-- Keep captured changes for 14 days instead of the default 3
EXEC sys.sp_cdc_change_job @job_type = N'cleanup', @retention = 20160;
GO