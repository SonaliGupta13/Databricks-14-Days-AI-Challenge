# Databricks notebook source
# Create Simple DataFrame
data = [("iphone",999), ("Samsung",799), ("Macbook",1299)]
df = spark.createDataFrame(data, ["product", "price"])
df.show()

# Filter expensive products
df.filter(df.price > 1000).show()
