# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql import functions as F

# Reference existing Delta table by path
delta_path = "dbfs:/Volumes/workspace/default/kaggle_volume/delta/events"
deltaTable = DeltaTable.forPath(spark, delta_path)


# COMMAND ----------

# Create incremental data
updates = (
    spark.read.format("delta").load(delta_path)
    .sample(0.02, seed=42)          # small batch
    .withColumn("price", F.col("price") + 5)  # simulate change
)


# COMMAND ----------

# Deduplicate source
updates_clean = updates.dropDuplicates(
    ["user_id", "event_time", "product_id"]
)

# COMMAND ----------

# Merge
deltaTable.alias("t").merge(
    updates_clean.alias("s"),
    """
    t.user_id = s.user_id
    AND t.event_time = s.event_time
    AND t.product_id = s.product_id
    """
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

print("Incremental MERGE completed")


# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC -- Query Historical Versions (Time Travel)
# MAGIC -- step1- View table history
# MAGIC DESCRIBE HISTORY workspace.default.events_table;
# MAGIC
# MAGIC

# COMMAND ----------

# step2- Read an old version
v0 = spark.read.format("delta") \
    .option("versionAsOf", 0) \
    .load(delta_path)

print("Version 0 rows:", v0.count())

# COMMAND ----------

# step3- Read by timestamp
old_data = spark.read.format("delta") \
    .option("timestampAsOf", "2024-01-01") \
    .load(delta_path)

old_data.show(5)


# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC -- OPTIMIZE & ZORDER
# MAGIC OPTIMIZE workspace.default.events_table
# MAGIC ZORDER BY (event_type, user_id);
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC -- Clean Old Files (VACUUM)
# MAGIC VACUUM workspace.default.events_table RETAIN 168 HOURS;
# MAGIC