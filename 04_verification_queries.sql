USE WideWorldImporters;
GO

-- 01: Database restored
SELECT name, create_date, state_desc
FROM sys.databases WHERE name = 'WideWorldImporters';
GO

-- 02: Baseline row counts
SELECT 'Customers' AS TableName, COUNT(*) AS TotalRows FROM Sales.Customers
UNION ALL SELECT 'Orders',     COUNT(*) FROM Sales.Orders
UNION ALL SELECT 'OrderLines', COUNT(*) FROM Sales.OrderLines
UNION ALL SELECT 'StockItems', COUNT(*) FROM Warehouse.StockItems
UNION ALL SELECT 'Invoices',   COUNT(*) FROM Sales.Invoices;
GO

-- 03: CDC enabled
SELECT name, is_cdc_enabled FROM sys.databases WHERE name = 'WideWorldImporters';

SELECT s.name AS SchemaName, t.name AS TableName, t.is_tracked_by_cdc
FROM sys.tables t
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE t.is_tracked_by_cdc = 1;
GO

-- 04: SQL Server Agent running + CDC jobs
SELECT servicename, status_desc, startup_type_desc
FROM sys.dm_server_services WHERE servicename LIKE '%Agent%';

EXEC sys.sp_cdc_help_jobs;
GO

-- 05: CDC captured insert / update / delete
SELECT __$operation, __$start_lsn, OrderID, Comments
FROM cdc.Sales_Orders_CT
ORDER BY __$start_lsn, __$seqval;
GO