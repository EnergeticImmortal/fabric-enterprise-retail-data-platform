USE WideWorldImporters;
GO

-- INSERT a test order
INSERT INTO Sales.Orders
    (CustomerID, SalespersonPersonID, ContactPersonID, OrderDate,
     ExpectedDeliveryDate, IsUndersupplyBackordered, Comments, LastEditedBy)
SELECT TOP 1
     CustomerID, SalespersonPersonID, ContactPersonID, CAST(GETDATE() AS date),
     DATEADD(day, 1, CAST(GETDATE() AS date)), 0, 'CDC test order', LastEditedBy
FROM Sales.Orders
ORDER BY OrderID;
GO

-- UPDATE it
UPDATE Sales.Orders SET Comments = 'CDC test order - updated'
WHERE Comments = 'CDC test order';
GO

-- DELETE it
DELETE FROM Sales.Orders WHERE Comments = 'CDC test order - updated';
GO

-- Wait for the capture job, then check: 2 = insert, 3/4 = update, 1 = delete
WAITFOR DELAY '00:00:10';
SELECT __$operation, __$start_lsn, OrderID, Comments
FROM cdc.Sales_Orders_CT
ORDER BY __$start_lsn, __$seqval;
GO