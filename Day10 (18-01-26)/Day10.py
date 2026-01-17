# Databricks notebook source
## Analyze Query Plans

spark.sql("""
SELECT *
FROM ecommerce.silver.events
WHERE event_type = 'purchase'
""").explain(True)


# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create Partitioned Silver Table
# MAGIC
# MAGIC CREATE TABLE ecommerce.silver.events_part
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (event_type)
# MAGIC AS
# MAGIC SELECT
# MAGIC     *,
# MAGIC     DATE(event_time) AS event_date
# MAGIC FROM ecommerce.silver.events;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Apply OPTIMIZE + ZORDER
# MAGIC
# MAGIC OPTIMIZE ecommerce.silver.events_part
# MAGIC ZORDER BY (user_id, product_id);
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --Check query plan
# MAGIC SELECT * 
# MAGIC FROM ecommerce.silver.events_part
# MAGIC WHERE user_id = 12;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Benchmark improvements**

# COMMAND ----------

import time
start = time.time()
spark.sql("""
SELECT *
FROM ecommerce.silver.events
WHERE user_id = 12345
""").count()

print(f"Time before optimization: {time.time() - start:.2f} seconds")


# COMMAND ----------

start = time.time()
spark.sql("""
SELECT *
FROM ecommerce.silver.events_part
WHERE user_id = 12345
""").count()

print(f"Time after optimization: {time.time() - start:.2f} seconds")
