# Databricks notebook source
# Design 3 level architecture

bronze_path = "dbfs:/Volumes/workspace/default/kaggle_volume/bronze/events"
silver_path = "dbfs:/Volumes/workspace/default/kaggle_volume/silver/events"
gold_path   = "dbfs:/Volumes/workspace/default/kaggle_volume/gold/daily_sales"


# COMMAND ----------

# Build BRONZE Layer (Raw Ingestion)

from pyspark.sql.functions import current_timestamp
# Load raw CSV
raw_events = spark.read.csv("dbfs:/Volumes/workspace/default/kaggle_volume/2019-Oct.csv",
                            header=True,inferSchema=True)

# Add ingestion metadata
bronze_events = raw_events.withColumn("ingestion_time", current_timestamp())

# Write to Bronze Delta
bronze_events.write.format("delta").mode("overwrite").save(bronze_path)

print("Bronze layer created")

# COMMAND ----------

# Build SILVER Layer (Clean & Validate)

from pyspark.sql import functions as F

# Read Bronze
bronze_df = spark.read.format("delta").load(bronze_path)

# Cleaning
silver_events = (bronze_df
                 .filter(F.col("user_id").isNotNull())
                 .filter(F.col("product_id").isNotNull())
                 .filter(F.col("price") > 0)
                 .dropDuplicates(["user_id", "event_time", "product_id"])
                 )

# Write to Silver Delta
silver_events.write.format("delta").mode("overwrite").save(silver_path)

print("Silver layer created")

# COMMAND ----------

# Build GOLD Layer (Business Aggregates)

# Read Silver
silver_df = spark.read.format("delta").load(silver_path)

# Business metrics
gold_daily_sales = (
    silver_df
    .withColumn("event_date", F.to_date("event_time"))
    .groupBy("event_date", "event_type")
    .agg(
        F.count("*").alias("total_events"),
        F.round(F.sum("price"), 2).alias("total_revenue")
    )
)

# Write to Gold Delta
gold_daily_sales.write.format("delta").mode("overwrite").save(gold_path)

print("Gold layer created")

# COMMAND ----------


display(spark.read.format("delta").load(gold_path))
