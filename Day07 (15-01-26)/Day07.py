# Databricks notebook source
# Add parameter widgets to notebooks
dbutils.widgets.text("source_path", "/default/path")
dbutils.widgets.dropdown("layer", "bronze", ["bronze", "silver", "gold"])


# COMMAND ----------

# Reading widget values
source = dbutils.widgets.get("source_path")
layer = dbutils.widgets.get("layer")


# COMMAND ----------

# Create multi-task job (Bronze → Silver → Gold)
def run_layer(layer_name):
    if layer_name == "bronze":
        print("Running Bronze Layer")
        # Read raw CSV and write Bronze Delta

    elif layer_name == "silver":
        print("Running Silver Layer")
        # Read Bronze Delta, clean data, write Silver

    elif layer_name == "gold":
        print("Running Gold Layer")
        # Read Silver Delta, aggregate, write Gold

# Execute based on widget
run_layer(layer)
