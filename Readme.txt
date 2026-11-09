Readme

End-to-end data platform: on-premises SQL Server → Microsoft Fabric (Bronze/Silver/Gold) → Power BI,
with Change Data Capture (CDC) for incremental loading.

## Architecture
SQL Server (WideWorldImporters, CDC) → On-premises data gateway → Fabric Data Pipeline → Lakehouse (Bronze)

## Progress
- [x] Day 1: SQL Server setup, CDC enabled and tested
- [x] Day 2: Gateway, Fabric connection, initial full load to Bronze
- [ ] Day 3: Incremental CDC pipeline with LSN watermarks
- [ ] Day 4: Silver transformations (PySpark, Delta MERGE)
- [ ] Day 5: Gold star schema, metadata-driven orchestration
- [ ] Day 6: Power BI dashboards
- [ ] Day 7: Tests, documentation, demo

## CDC verification
Test order 73596 was inserted, updated, and deleted. CDC captured all four operations:
2 = insert, 3 = before update, 4 = after update, 1 = delete.

![CDC test](docs/screenshots/05-cdc-insert-update-delete.png)

## Source-to-target reconciliation (initial load)
| Table | SQL Server | Fabric Bronze |
|---|---|---|
| Sales.Orders | 73,595 | 73,595 ✅ |
| Sales.OrderLines | 231,412 | 231,412 ✅ |