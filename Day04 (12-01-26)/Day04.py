# Databricks notebook source
# Load data (October sample)
events = spark.read.csv("dbfs:/Volumes/workspace/default/kaggle_volume/2019-Oct.csv",header=True,inferSchema=True)


# COMMAND ----------

events.printSchema()
events.count()


# COMMAND ----------

# Convert CSV to Delta format
delta_path = "dbfs:/Volumes/workspace/default/kaggle_volume/delta/events"

events.write.format("delta").mode("overwrite").save(delta_path)

print("CSV converted to Delta format")


# COMMAND ----------

display(dbutils.fs.ls(delta_path))


# COMMAND ----------

# Create Delta table using PySpark

events.write.format("delta").mode("overwrite").saveAsTable("workspace.default.events_table")

print("Managed Delta table created")


# COMMAND ----------

# Create Delta table using SQL
spark.sql("""
CREATE TABLE workspace.default.events_delta
USING DELTA
AS
SELECT * FROM workspace.default.events_table
""")



# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC -- verify table creation
# MAGIC SHOW TABLES IN workspace.default;
# MAGIC

# COMMAND ----------

# Test Schema Enforcement (Try inserting wrong schema)
from pyspark.sql import Row

try:
    wrong_schema_df = spark.createDataFrame(
        [Row(x="a", y="b", z="c")]
    )

    wrong_schema_df.write.format("delta").mode("append").save(delta_path)

except Exception as e:
    print("Schema enforcement working!")
    print(e)


# COMMAND ----------

# Handle Duplicate Inserts (Problem Example)
events.write.format("delta").mode("append").save(delta_path)


# COMMAND ----------

# Remove duplicates
deduped_events = events.dropDuplicates(
    ["user_id", "event_time", "product_id"]
)

deduped_events.write.format("delta") \
    .mode("append") \
    .save(delta_path)

print("Duplicates removed correctly")
