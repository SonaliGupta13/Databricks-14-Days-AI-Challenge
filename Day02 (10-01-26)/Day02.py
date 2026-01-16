# Databricks notebook source
from pyspark.sql import functions as F

# Load data (October sample)
events = spark.read.csv("dbfs:/Volumes/workspace/default/kaggle_volume/2019-Oct.csv",header=True,inferSchema=True)

# Basic operations
events.select("event_type", "brand", "price").show(10)
events.filter(F.col("price") > 100).count()
events.groupBy("event_type").count().show()
top_brands = (events.groupBy("brand").count().orderBy(F.col("count").desc()).limit(5))
top_brands.show()
