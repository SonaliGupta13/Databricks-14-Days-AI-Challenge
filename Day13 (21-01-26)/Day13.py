# Databricks notebook source
# MAGIC %md
# MAGIC #### **Train 3 Different Models**

# COMMAND ----------

import os
import mlflow
import mlflow.spark

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import (
    LinearRegression,
    DecisionTreeRegressor,
    RandomForestRegressor
)
from pyspark.ml.evaluation import RegressionEvaluator

# Required for Unity Catalog
os.environ["MLFLOW_DFS_TMP"] = "/Volumes/workspace/default/kaggle_volume/mlflow_tmp"

# Load data
data = (
    spark.table("ecommerce.gold.daily_sales")
    .select("total_events", "total_revenue")
    .dropna()
)

train_df, test_df = data.randomSplit([0.8, 0.2], seed=42)

# Feature vector
assembler = VectorAssembler(
    inputCols=["total_events"],
    outputCol="features"
)

train_vec = assembler.transform(train_df).select("features", "total_revenue")
test_vec  = assembler.transform(test_df).select("features", "total_revenue")

# Models
models = {
    "LinearRegression": LinearRegression(labelCol="total_revenue"),
    "DecisionTree": DecisionTreeRegressor(labelCol="total_revenue", maxDepth=5),
    "RandomForest": RandomForestRegressor(labelCol="total_revenue", numTrees=50)
}

evaluator = RegressionEvaluator(
    labelCol="total_revenue",
    predictionCol="prediction",
    metricName="rmse"
)

mlflow.set_experiment("/Shared/mlflow_model_comparison")

for name, model in models.items():
    with mlflow.start_run(run_name=name):
        mlflow.log_param("model", name)
        mlflow.log_param("features", "total_events")

        fitted_model = model.fit(train_vec)
        predictions = fitted_model.transform(test_vec)
        rmse = evaluator.evaluate(predictions)

        mlflow.log_metric("rmse", rmse)
        mlflow.spark.log_model(fitted_model, "model")

        print(f"{name} | RMSE = {rmse:.2f}")


# COMMAND ----------

# MAGIC %md
# MAGIC #### **Compare Metrics in MLflow**

# COMMAND ----------

import mlflow

runs = mlflow.search_runs()

runs[[
    "tags.mlflow.runName",
    "metrics.rmse",
    "params.model"
]]


# COMMAND ----------

clean_runs = runs[[
    "tags.mlflow.runName",
    "metrics.rmse",
    "params.model"
]].rename(columns={
    "tags.mlflow.runName": "run_name",
    "metrics.rmse": "rmse",
    "params.model": "model"
})

clean_runs


# COMMAND ----------

# MAGIC %md
# MAGIC #### **Build Spark ML Pipeline**

# COMMAND ----------

import os
import mlflow
import mlflow.spark

from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

os.environ["MLFLOW_DFS_TMP"] = "/Volumes/workspace/default/kaggle_volume/mlflow_tmp"

data = (
    spark.table("ecommerce.gold.daily_sales")
    .select("total_events", "total_revenue")
    .dropna()
)

train, test = data.randomSplit([0.8, 0.2], seed=42)

assembler = VectorAssembler(
    inputCols=["total_events"],
    outputCol="features"
)

rf = RandomForestRegressor(
    labelCol="total_revenue",
    numTrees=50
)

pipeline = Pipeline(stages=[assembler, rf])

with mlflow.start_run(run_name="RandomForest_Pipeline"):
    pipeline_model = pipeline.fit(train)
    predictions = pipeline_model.transform(test)

    evaluator = RegressionEvaluator(
        labelCol="total_revenue",
        predictionCol="prediction",
        metricName="rmse"
    )

    rmse = evaluator.evaluate(predictions)

    mlflow.log_param("model", "RandomForestPipeline")
    mlflow.log_param("features", "total_events")
    mlflow.log_metric("rmse", rmse)
    mlflow.spark.log_model(pipeline_model, "pipeline_model")

    print("Pipeline RMSE:", rmse)


# COMMAND ----------

# MAGIC %md
# MAGIC #### **Select Best Model**

# COMMAND ----------

best_run = clean_runs.sort_values("rmse").iloc[0]

print("Best Model:", best_run["model"])
print("Best RMSE:", best_run["rmse"])
