## Project explanation

- **Goal**
  - Build an end-to-end retail data platform on Microsoft Fabric that keeps analytics up to date incrementally from an on-premises SQL Server database
  - Handle inserts, updates and deletes correctly, without full reloads and without duplicate data
  - Deliver reconciled, business-ready numbers in Power BI

- **Source system**
  - SQL Server 2025 (Developer edition) running locally, with the Microsoft WideWorldImporters OLTP sample database (a fictional wholesale company)
  - Used the operational database, not the ready-made warehouse version, so the analytics model is built from scratch
  - Main tables: Sales.Orders, Sales.OrderLines, Sales.Customers, Warehouse.StockItems
  - Total source size: ~306,000 rows

- **Change Data Capture (CDC)**
  - CDC enabled on Sales.Orders and Sales.OrderLines
  - SQL Server Agent runs the CDC capture and cleanup jobs
  - Every change is recorded with a Log Sequence Number (LSN) and an operation code: 1 = delete, 2 = insert, 3 = before update, 4 = after update
  - CDC retention extended from 3 to 14 days so changes survive pauses between pipeline runs
  - Verified with an insert/update/delete test before building anything in Fabric

- **Security and connectivity**
  - Dedicated read-only SQL login (fabric_reader) used by Fabric, so the platform can never modify source data
  - Login added to WideWorldImporters' sales-territory roles to pass row-level security on customers (still read-only)
  - On-premises data gateway (standard mode) connects local SQL Server to Fabric using outbound connections only, with no open firewall ports
  - Separate Fabric connection for invoking pipelines from the master pipeline

- **Bronze layer (lh_bronze): raw data**
  - Initial full load of Orders and OrderLines into snapshot tables (sales_orders, sales_orderlines)
  - Starting LSN recorded before the initial load, so no change made during or after it can be missed
  - Incremental CDC loads append changes into history tables (sales_orders_cdc, sales_orderlines_cdc)
  - Each CDC row carries its LSN, sequence value, operation code, pipeline run ID and extraction timestamp for lineage
  - Customers and StockItems loaded with a full reload each run (small tables that change rarely, so CDC would add complexity without benefit)
  - Lookup tables (categories, buying groups, cities, provinces, colors) joined at the source so Bronze gets readable names instead of IDs

- **Watermark control (wh_control)**
  - Fabric Warehouse holds control.cdc_watermark: the last processed LSN per table
  - Each CDC pipeline: reads the last LSN, reads the current max LSN from SQL Server, copies only the changes in between, then updates the watermark
  - If last LSN equals max LSN, the pipeline skips the copy (no changes, no work)
  - Core reliability rule: the watermark moves only after the copy succeeds, so a failed run never skips data
  - Watermark updates have automatic retries to handle transient conflicts

- **Silver layer (lh_silver): clean, current data**
  - PySpark notebooks combine the Bronze snapshot with the CDC history into one current table per entity
  - Window function keeps only the latest change per key (OrderID / OrderLineID), so an entity changed many times becomes one final state
  - Delta Lake MERGE applies changes, but only when the change's LSN is newer than the row's current LSN
  - This makes the MERGE idempotent: rerunning it changes nothing and never creates duplicates
  - LSNs stored as fixed-length hex strings, so text comparison equals time order
  - Soft deletes: deleted rows are flagged with _is_deleted instead of removed, keeping an audit trail
  - Dimension data lightly cleaned (deduplicated, trimmed, missing values filled with "None" / "N/A")

- **Gold layer (lh_gold): star schema for analytics**
  - fact_sales: one row per order line with quantity, unit price, revenue and tax amount (231,413 rows)
  - dim_customer: name, category, buying group, city, state, sales territory
  - dim_product: name, brand, size, color, list and retail prices
  - dim_date: one row per calendar day with year, quarter, month, month name and weekday
  - Deleted rows excluded, so Gold shows only what the business should see
  - Rebuilt from Silver on each run: simple and always consistent at this data size (would switch to incremental MERGE at larger scale)

- **Orchestration**
  - Master pipeline (pl_master_daily_load) runs the full flow with one click
  - CDC loads for Orders and OrderLines run in sequence to avoid concurrent watermark updates
  - Each Silver notebook starts as soon as its CDC load finishes
  - Dimension full load runs in parallel
  - Gold builds only after all upstream steps succeed; if anything fails, Gold keeps its last correct version

- **Power BI**
  - Semantic model in Direct Lake mode, reading Gold Delta tables directly (no import, no refresh schedule)
  - Star relationships from fact_sales to the three dimensions; dim_date marked as the date table
  - Technical key columns hidden from report users
  - DAX measures: Total Revenue, Total Quantity, Order Count, Customer Count, Average Order Value, Revenue PY, Revenue YoY %
  - Sales dashboard: KPI cards, monthly revenue trend, top 10 products, revenue by sales territory, year-over-year matrix, year and customer-category slicers
  - Page filter limited to 2013–2016 to exclude test orders created during development

- **Testing and data quality**
  - Initial load reconciled with source: Orders 73,595 = 73,595, OrderLines 231,412 = 231,412
  - Gold checks: 0 duplicate order lines, 0 orphan customer/product/date keys
  - End-to-end test: an order line quantity change in SQL Server flowed through CDC, Bronze, Silver and Gold; Gold revenue matched SQL Server exactly (177,635,732.40)
  - Idempotency test: rerunning pipelines and notebooks with no new changes produced no duplicates
  - Recovery test: a real failed run was rerun successfully with no data loss or duplication in Silver/Gold
  - Power BI yearly revenue matched SQL Server per year

- **Lessons learned**
  - A pipeline can succeed and still load 0 rows: row-level security on Sales.Customers hid all rows from the read-only login; row-count reconciliation caught it
  - Parallel CDC pipelines caused a snapshot isolation update conflict on the shared watermark table in Fabric Warehouse; fixed by running them in sequence and adding retries
  - The recovery design worked in practice: the failed run left the watermark unchanged, the rerun re-read the same changes, and the Silver MERGE removed the duplicates automatically
  - The gateway can show "online" but fail after the laptop sleeps (error 11062); restarting the gateway service fixed it
  - Copying from an on-premises source requires the destination Lakehouse connection to allow gateway usage
  - The SQL analytics endpoint needs a short time to sync newly created tables
  - Notebook sessions lose their variables when they expire; the settings cell must be rerun first

- **Data volume**
  - ~306,000 source rows become a 231,413-row fact table and three small dimensions, summarized in Power BI into a few dozen numbers
  - Incremental runs move only changed rows; in testing, a run moved 4 Orders changes and 2 OrderLines changes instead of reloading 300,000+ rows
  - The dataset is small, but the design is built to scale: daily runs process only that day's changes regardless of total table size

- **Tools and technologies**
  - SQL Server, SSMS, T-SQL, Change Data Capture, SQL Server Agent
  - On-premises data gateway
  - Microsoft Fabric: Data Factory pipelines, Lakehouses (Delta Lake), Warehouse, notebooks
  - PySpark, Delta Lake MERGE
  - Power BI, DAX, Direct Lake
  - GitHub

- **Future improvements**
  - Metadata-driven orchestration: one parameterized pipeline and notebook driven by a config table, so adding a table means inserting one row
  - Run log table with status, duration and row counts, plus automatic failure when a table loads 0 rows
  - Add Sales.InvoiceLines for profit and margin reporting
  - Slowly changing dimensions (Type 2) to keep history of customer and product changes
  - CI/CD with Fabric Git integration and deployment pipelines
