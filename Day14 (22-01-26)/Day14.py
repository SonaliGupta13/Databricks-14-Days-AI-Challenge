# Databricks notebook source
## Load data
base_df = spark.table("ecommerce.gold.daily_sales").dropna()
base_df.printSchema()
display(base_df)



# COMMAND ----------

# MAGIC %pip install transformers torch
# MAGIC

# COMMAND ----------

## Simple NLP Task

from transformers import pipeline

classifier = pipeline("sentiment-analysis")

reviews = [
    "This product is amazing!",
    "Terrible quality, waste of money",
    "Very satisfied with the purchase",
    "Not worth the price"
]

results = classifier(reviews)
results


# COMMAND ----------

## Log NLP Model with MLflow

import mlflow

with mlflow.start_run(run_name="sentiment_analysis_nlp"):
    mlflow.log_param("model", "distilbert-base-uncased-finetuned-sst-2-english")
    mlflow.log_param("task", "sentiment-analysis")
    
    # Example metric (for demo)
    mlflow.log_metric("sample_accuracy", 0.95)
