# Databricks notebook source
## Prepare data
from pyspark.sql import functions as F

# Load silver events
events = spark.table("ecommerce.silver.events")

# Simple feature set
ml_data = (
    events
    .filter(F.col("price").isNotNull())
    .withColumn("hour", F.hour("event_time"))
    .withColumn("day_of_week", F.dayofweek("event_time"))
    .select("price", "hour", "day_of_week")
)


# COMMAND ----------

## Vectorize features
from pyspark.ml.feature import VectorAssembler

assembler = VectorAssembler(
    inputCols=["hour", "day_of_week"],
    outputCol="features"
)

final_data = assembler.transform(ml_data).select("features", "price")

## Train-test split
train_data, test_data = final_data.randomSplit([0.8, 0.2], seed=42)

## Train Linear Regression model
from pyspark.ml.regression import LinearRegression

lr = LinearRegression(labelCol="price")

model = lr.fit(train_data)


# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/Volumes/workspace/default/kaggle_volume/mlflow_tmp")


# COMMAND ----------

import os
os.environ["MLFLOW_DFS_TMP"] = "/Volumes/workspace/default/kaggle_volume/mlflow_tmp"


# COMMAND ----------

## Log parameters, metrics, model
import os
import mlflow
import mlflow.spark

# Required for Unity Catalog
os.environ["MLFLOW_DFS_TMP"] = "/Volumes/workspace/default/kaggle_volume/mlflow_tmp"

with mlflow.start_run():
    # Log parameters
    mlflow.log_param("model_type", "LinearRegression")
    mlflow.log_param("features", "hour, day_of_week")

    # Predictions
    predictions = model.transform(test_data)

    from pyspark.ml.evaluation import RegressionEvaluator
    evaluator = RegressionEvaluator(
        labelCol="price",
        predictionCol="prediction",
        metricName="rmse" 
    )

    rmse = evaluator.evaluate(predictions)

    # Log metric
    mlflow.log_metric("rmse", rmse)
    
    # Log model
    mlflow.spark.log_model(model, "linear_regression_model")

    print("RMSE:", rmse)


# COMMAND ----------

## Compare Runs

## Simple Features
with mlflow.start_run():
    mlflow.log_param("features", "hour, day_of_week")
    mlflow.log_param("model", "LinearRegression")
    mlflow.log_metric("rmse", rmse)

## Add more features
with mlflow.start_run():
    mlflow.log_param("features", "hour, day_of_week, is_weekend")
    mlflow.log_param("model", "LinearRegression")
    mlflow.log_metric("rmse", rmse)

