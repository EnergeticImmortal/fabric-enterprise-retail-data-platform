USE master;
GO

-- Restore from backup (paths assume a default SQL Server 2025 instance;
-- check yours with: SELECT SERVERPROPERTY('InstanceDefaultDataPath');)
RESTORE DATABASE WideWorldImporters
FROM DISK = 'C:\SQLBackups\WideWorldImporters-Full.bak'
WITH
    MOVE 'WWI_Primary'         TO 'C:\Program Files\Microsoft SQL Server\MSSQL17.MSSQLSERVER\MSSQL\DATA\WideWorldImporters.mdf',
    MOVE 'WWI_UserData'        TO 'C:\Program Files\Microsoft SQL Server\MSSQL17.MSSQLSERVER\MSSQL\DATA\WideWorldImporters_UserData.ndf',
    MOVE 'WWI_Log'             TO 'C:\Program Files\Microsoft SQL Server\MSSQL17.MSSQLSERVER\MSSQL\DATA\WideWorldImporters.ldf',
    MOVE 'WWI_InMemory_Data_1' TO 'C:\Program Files\Microsoft SQL Server\MSSQL17.MSSQLSERVER\MSSQL\DATA\WideWorldImporters_InMemory_Data_1',
    RECOVERY, STATS = 10;
GO

-- Set a valid owner (prevents CDC errors on restored databases)
ALTER AUTHORIZATION ON DATABASE::WideWorldImporters TO sa;
GO

-- Confirm the database is online
SELECT name, create_date, state_desc
FROM sys.databases
WHERE name = 'WideWorldImporters';
GO