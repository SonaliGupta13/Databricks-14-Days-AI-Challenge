# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM ecommerce.gold.daily_sales;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Analytical Queries**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Revenue Trend with 7-Day Moving Average
# MAGIC
# MAGIC WITH daily_revenue AS (
# MAGIC     SELECT
# MAGIC         event_date, SUM(total_revenue) AS revenue
# MAGIC     FROM ecommerce.gold.daily_sales
# MAGIC     GROUP BY event_date
# MAGIC )
# MAGIC SELECT
# MAGIC     event_date, revenue,
# MAGIC     ROUND(
# MAGIC         AVG(revenue) OVER (
# MAGIC             ORDER BY event_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
# MAGIC         ), 2
# MAGIC     ) AS revenue_7_day_ma
# MAGIC FROM daily_revenue
# MAGIC ORDER BY event_date;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Conversion Funnel (Views → Purchases)
# MAGIC
# MAGIC SELECT
# MAGIC     category_code,
# MAGIC     SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) AS views,
# MAGIC     SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchases,
# MAGIC     ROUND(
# MAGIC         SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) * 100.0 /
# MAGIC         NULLIF(SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END), 0),
# MAGIC         2
# MAGIC     ) AS conversion_rate
# MAGIC FROM ecommerce.silver.events
# MAGIC GROUP BY category_code
# MAGIC ORDER BY conversion_rate DESC;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top Products by Revenue
# MAGIC
# MAGIC SELECT
# MAGIC     product_id, brand, ROUND(SUM(price), 2) AS revenue
# MAGIC FROM ecommerce.silver.events
# MAGIC WHERE event_type = 'purchase'
# MAGIC GROUP BY product_id, brand
# MAGIC ORDER BY revenue DESC
# MAGIC LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Customer Segmentation (Regular / Loyal / VIP)
# MAGIC
# MAGIC WITH customer_metrics AS (
# MAGIC     SELECT
# MAGIC         user_id, COUNT(*) AS purchase_count,SUM(price) AS total_spent
# MAGIC     FROM ecommerce.silver.events
# MAGIC     WHERE event_type = 'purchase'
# MAGIC     GROUP BY user_id
# MAGIC )
# MAGIC SELECT
# MAGIC     CASE
# MAGIC         WHEN purchase_count >= 10 THEN 'VIP'
# MAGIC         WHEN purchase_count >= 5 THEN 'Loyal'
# MAGIC         ELSE 'Regular'
# MAGIC     END AS customer_tier,
# MAGIC     COUNT(*) AS customers,
# MAGIC     ROUND(AVG(total_spent), 2) AS avg_lifetime_value
# MAGIC FROM customer_metrics
# MAGIC GROUP BY customer_tier
# MAGIC ORDER BY avg_lifetime_value DESC;
# MAGIC