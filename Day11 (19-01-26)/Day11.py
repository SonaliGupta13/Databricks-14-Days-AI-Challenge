# Databricks notebook source
## Calculate statistical summaries

events = spark.table("ecommerce.silver.events")


# COMMAND ----------

events.describe(["price"]).show()


# COMMAND ----------

## Hypothesis Testing (Weekday vs Weekend)

from pyspark.sql import functions as F

weekday = events.withColumn(
    "event_date", F.to_date("event_time")
).withColumn(
    "is_weekend",
    F.dayofweek("event_date").isin([1, 7])
)

weekday.groupBy("is_weekend", "event_type").count().show()



# COMMAND ----------

## Identify Correlations

events = events.withColumn(
    "conversion_rate",
    F.when(F.col("event_type") == "purchase", 1).otherwise(0)
)

# COMMAND ----------

events.stat.corr("price", "conversion_rate")


# COMMAND ----------

## Feature Engineering for Machine Learning

from pyspark.sql.window import Window

features = (
    events
    .withColumn("hour", F.hour("event_time"))
    .withColumn("day_of_week", F.dayofweek("event_date"))
    .withColumn("price_log", F.log(F.col("price") + 1))
    .withColumn(
        "time_since_first_view",
        F.unix_timestamp("event_time") -
        F.first("event_time").over(
            Window.partitionBy("user_id").orderBy("event_time")
        )
    )
)
