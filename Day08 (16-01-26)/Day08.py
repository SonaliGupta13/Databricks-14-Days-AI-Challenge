# Databricks notebook source
# MAGIC %sql
# MAGIC -- Create catalog & schemas
# MAGIC CREATE CATALOG ecommerce;
# MAGIC
# MAGIC -- Use the catalog
# MAGIC USE CATALOG ecommerce;
# MAGIC
# MAGIC CREATE SCHEMA bronze;
# MAGIC CREATE SCHEMA silver;
# MAGIC CREATE SCHEMA gold;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Register Delta tables
# MAGIC CREATE TABLE ecommerce.bronze.events
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT *
# MAGIC FROM delta.`/Volumes/workspace/default/kaggle_volume/bronze/events`;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show table data
# MAGIC SELECT * FROM ecommerce.bronze.events LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Register Delta tables
# MAGIC CREATE TABLE ecommerce.silver.events
# MAGIC USING DELTA
# MAGIC AS SELECT * FROM delta.`/Volumes/workspace/default/kaggle_volume/silver/events`;
# MAGIC
# MAGIC CREATE TABLE ecommerce.gold.daily_sales
# MAGIC USING DELTA
# MAGIC AS SELECT * FROM delta.`/Volumes/workspace/default/kaggle_volume/gold/daily_sales`;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- -- Permissions
# MAGIC GRANT SELECT ON TABLE ecommerce.gold.daily_sales TO `sonali.gupta@vensysco.in`;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_user();
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Controlled view
# MAGIC CREATE VIEW ecommerce.gold.top_products AS
# MAGIC SELECT *
# MAGIC FROM ecommerce.gold.daily_sales
# MAGIC WHERE total_revenue > 1000;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show data from the View
# MAGIC SELECT * FROM ecommerce.gold.top_products;
# MAGIC