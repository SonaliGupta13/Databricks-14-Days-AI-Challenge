# Databricks notebook source
# Load Full E-commerce Dataset
events = spark.read.csv(
    "dbfs:/Volumes/workspace/default/kaggle_volume/2019-Oct.csv",
    header=True,
    inferSchema=True
)

print(f"Total events: {events.count():,}")
events.printSchema()
display(events.limit(5))


# COMMAND ----------

# Perform Complex Joins

# Create product dimension
products = events.select("product_id", "brand").dropDuplicates()

# Inner Join
inner_join_df = events.join(products, on="product_id", how="inner")

# Left Join
left_join_df = events.join(products, on="product_id", how="left")

display(inner_join_df.limit(5))
display(left_join_df.limit(5))



# COMMAND ----------

# Running Totals Using Window Functions
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Filter purchases
purchases = events.filter(F.col("event_type") == "purchase")

# Window definition
window_spec = Window.partitionBy("brand").orderBy("event_time")

# Running revenue per brand
running_totals = purchases.withColumn("running_revenue",F.sum("price").over(window_spec))

display(
    running_totals.select("event_time", "brand", "price", "running_revenue").limit(10))


# COMMAND ----------

# Create Derived Features

from pyspark.sql import functions as F

events_simple = events.withColumn(
    "revenue",
    F.when(F.col("event_type") == "purchase", F.col("price"))
     .otherwise(0)
)

display(events_simple.select(
    "event_type", "price", "revenue"
).limit(10))
