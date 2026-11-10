#!/usr/bin/env python
# coding: utf-8

# ## nb_gold_star_schema
# 
# null

# In[3]:


from pyspark.sql import functions as F
from datetime import date

BRONZE = "lh_bronze.dbo"
SILVER = "lh_silver.dbo"
GOLD   = "lh_gold.dbo"

def save(df, full_name):
    (df.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(full_name))
    print(f"{full_name}: {spark.read.table(full_name).count()} rows")


# In[5]:


customers = (
    spark.read.table(f"{BRONZE}.customers")
    .dropDuplicates(["CustomerID"])
    .withColumn("CustomerName", F.trim("CustomerName"))
    .fillna({"BuyingGroupName": "None"})
)
save(customers, f"{SILVER}.customers")

stock_items = (
    spark.read.table(f"{BRONZE}.stock_items")
    .dropDuplicates(["StockItemID"])
    .fillna({"Brand": "N/A", "Size": "N/A", "ColorName": "N/A"})
)
save(stock_items, f"{SILVER}.stock_items")


# In[6]:


dim_customer = spark.read.table(f"{SILVER}.customers").select(
    F.col("CustomerID").alias("customer_key"),
    F.col("CustomerName").alias("customer_name"),
    F.col("CustomerCategoryName").alias("customer_category"),
    F.col("BuyingGroupName").alias("buying_group"),
    F.col("CityName").alias("city"),
    F.col("StateProvinceName").alias("state_province"),
    F.col("SalesTerritory").alias("sales_territory"),
)
save(dim_customer, f"{GOLD}.dim_customer")

dim_product = spark.read.table(f"{SILVER}.stock_items").select(
    F.col("StockItemID").alias("product_key"),
    F.col("StockItemName").alias("product_name"),
    F.col("Brand").alias("brand"),
    F.col("Size").alias("size"),
    F.col("ColorName").alias("color"),
    F.col("UnitPrice").alias("list_unit_price"),
    F.col("RecommendedRetailPrice").alias("recommended_retail_price"),
    F.col("IsChillerStock").alias("is_chiller_stock"),
)
save(dim_product, f"{GOLD}.dim_product")


# In[7]:


bounds = (spark.read.table(f"{SILVER}.orders")
          .agg(F.min("OrderDate").alias("min_d"), F.max("OrderDate").alias("max_d"))
          .first())
start = date(bounds.min_d.year, 1, 1)
end   = date(bounds.max_d.year, 12, 31)

dates = spark.sql(
    f"SELECT explode(sequence(to_date('{start}'), to_date('{end}'), interval 1 day)) AS date"
)

dim_date = dates.select(
    F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
    "date",
    F.year("date").alias("year"),
    F.quarter("date").alias("quarter"),
    F.month("date").alias("month"),
    F.date_format("date", "MMMM").alias("month_name"),
    F.date_format("date", "yyyy-MM").alias("year_month"),
    F.date_format("date", "EEEE").alias("day_name"),
    F.dayofweek("date").isin(1, 7).alias("is_weekend"),
)
save(dim_date, f"{GOLD}.dim_date")


# In[8]:


orders = spark.read.table(f"{SILVER}.orders").filter("_is_deleted = false")
lines  = spark.read.table(f"{SILVER}.orderlines").filter("_is_deleted = false")

fact_sales = (
    lines.alias("l")
    .join(orders.alias("o"), "OrderID")
    .select(
        F.col("l.OrderLineID").alias("order_line_id"),
        F.col("OrderID").alias("order_id"),
        F.date_format("o.OrderDate", "yyyyMMdd").cast("int").alias("date_key"),
        F.col("o.CustomerID").alias("customer_key"),
        F.col("l.StockItemID").alias("product_key"),
        F.col("o.SalespersonPersonID").alias("salesperson_id"),
        F.col("l.Quantity").alias("quantity"),
        F.col("l.UnitPrice").alias("unit_price"),
        F.col("l.TaxRate").alias("tax_rate"),
        F.round(F.col("l.Quantity") * F.col("l.UnitPrice"), 2).alias("revenue"),
        F.round(F.col("l.Quantity") * F.col("l.UnitPrice") * F.col("l.TaxRate") / 100, 2).alias("tax_amount"),
    )
)
save(fact_sales, f"{GOLD}.fact_sales")


# In[4]:


fact   = spark.read.table(f"{GOLD}.fact_sales")
d_cust = spark.read.table(f"{GOLD}.dim_customer")
d_prod = spark.read.table(f"{GOLD}.dim_product")
d_date = spark.read.table(f"{GOLD}.dim_date")

print("Fact rows:              ", fact.count())
print("Duplicate order lines:  ", fact.groupBy("order_line_id").count().filter("count > 1").count())
print("Orphan customer keys:   ", fact.join(d_cust, "customer_key", "left_anti").count())
print("Orphan product keys:    ", fact.join(d_prod, "product_key", "left_anti").count())
print("Orphan date keys:       ", fact.join(d_date, "date_key", "left_anti").count())
print("Total revenue:          ", fact.agg(F.sum("revenue")).first()[0])

