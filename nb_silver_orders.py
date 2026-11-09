#!/usr/bin/env python
# coding: utf-8

# ## nb_silver_orders
# 
# null

# In[1]:


from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

INITIAL_LSN   = "0x00000273000055690003"   # LSN recorded before the Day 2 full load
BRONZE_SNAP   = "lh_bronze.dbo.sales_orders"
BRONZE_CDC    = "lh_bronze.dbo.sales_orders_cdc"
SILVER_TABLE  = "lh_silver.dbo.orders"
KEY           = "OrderID"

def table_exists(name):
    try:
        spark.read.table(name)
        return True
    except Exception:
        return False


# In[2]:


if not table_exists(SILVER_TABLE):
    snapshot = (
        spark.read.table(BRONZE_SNAP)
        .dropDuplicates([KEY])
        .withColumn("_cdc_lsn", F.lit(INITIAL_LSN))
        .withColumn("_is_deleted", F.lit(False))
        .withColumn("_silver_updated_at", F.current_timestamp())
    )
    snapshot.write.format("delta").saveAsTable(SILVER_TABLE)
    print(f"Created {SILVER_TABLE} with {snapshot.count()} rows")
else:
    print(f"{SILVER_TABLE} already exists, skipping initial load")


# In[3]:


cdc = spark.read.table(BRONZE_CDC)

w = Window.partitionBy(KEY).orderBy(F.col("cdc_start_lsn").desc(), F.col("cdc_seqval").desc())
latest = cdc.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")

business_cols = [c for c in spark.read.table(SILVER_TABLE).columns if not c.startswith("_")]

changes = latest.select(
    *business_cols,
    F.col("cdc_start_lsn").alias("_cdc_lsn"),
    (F.col("cdc_operation") == 1).alias("_is_deleted"),
    F.current_timestamp().alias("_silver_updated_at"),
)
display(changes.select(KEY, "Comments", "_cdc_lsn", "_is_deleted"))


# In[4]:


target = DeltaTable.forName(spark, SILVER_TABLE)

(
    target.alias("t")
    .merge(changes.alias("s"), f"t.{KEY} = s.{KEY}")
    .whenMatchedUpdateAll(condition="s._cdc_lsn > t._cdc_lsn")
    .whenNotMatchedInsertAll(condition="s._is_deleted = false")
    .execute()
)
print("MERGE complete")


# In[5]:


silver = spark.read.table(SILVER_TABLE)

print("Total rows:     ", silver.count())
print("Active rows:    ", silver.filter("_is_deleted = false").count())
print("Duplicate keys: ", silver.groupBy(KEY).count().filter("count > 1").count())

display(silver.filter("OrderID = 1 OR Comments LIKE 'Day3%'")
              .select(KEY, "Comments", "_cdc_lsn", "_is_deleted"))

