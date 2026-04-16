# Databricks notebook source
# DBTITLE 1,Cell 1
# Read CSV from Unity Catalog Volume
df = spark.read.csv("/Volumes/workspace/default/ecommerce/2019-Nov.csv", header=True, inferSchema=True)

df.display()

# COMMAND ----------

df.write.mode("overwrite").parquet(
    "/Volumes/workspace/default/ecommerce/bronze/"
)

# COMMAND ----------

from pyspark.sql.functions import col

df = spark.read.parquet(
    "/Volumes/workspace/default/ecommerce/bronze/"
)

df_clean = df.dropna(subset=["user_id", "product_id", "price"])

df_clean = df_clean.withColumn("price", col("price").cast("double"))

df_clean = df_clean.filter(col("event_type") == "purchase")

display(df_clean)

# COMMAND ----------

df_clean.write.mode("overwrite").parquet(
    "/Volumes/workspace/default/ecommerce/silver/"
)

# COMMAND ----------

df = spark.read.parquet(
    "/Volumes/workspace/default/ecommerce/silver/"
)

df = df.withColumn("total_price", col("price"))

display(df)

# COMMAND ----------

df.write.mode("overwrite").parquet(
    "/Volumes/workspace/default/ecommerce/gold/"
)

# COMMAND ----------

df.createOrReplaceTempView("sales")

# COMMAND ----------

top_products = spark.sql("""
SELECT product_id, SUM(total_price) AS revenue
FROM sales
GROUP BY product_id
ORDER BY revenue DESC
LIMIT 10
""")

display(top_products)

# COMMAND ----------

top_users = spark.sql("""
SELECT user_id, SUM(total_price) AS total_spent
FROM sales
GROUP BY user_id
ORDER BY total_spent DESC
LIMIT 10
""")

display(top_users)

# COMMAND ----------

top_categories = spark.sql("""
SELECT category_code, COUNT(*) AS purchase_count
FROM sales
GROUP BY category_code
ORDER BY purchase_count DESC
LIMIT 10
""")

display(top_categories)

# COMMAND ----------

from pyspark.sql.functions import hour, to_timestamp

df = spark.read.parquet("/Volumes/workspace/default/ecommerce/gold/")

df = df.withColumn("event_time", to_timestamp("event_time"))

df = df.withColumn("hour", hour("event_time"))

df.createOrReplaceTempView("sales_time")

# COMMAND ----------

hourly_sales = spark.sql("""
SELECT hour, SUM(total_price) AS revenue
FROM sales_time
GROUP BY hour
ORDER BY hour
""")

display(hourly_sales)

# COMMAND ----------

top_brands = spark.sql("""
SELECT brand, SUM(total_price) AS revenue
FROM sales
GROUP BY brand
ORDER BY revenue DESC
LIMIT 10
""")

display(top_brands)

# COMMAND ----------

# DBTITLE 1,Cell 14
top_products.toPandas().to_csv("/Volumes/workspace/default/ecommerce/top_products.csv", index=False)
top_users.toPandas().to_csv("/Volumes/workspace/default/ecommerce/top_users.csv", index=False)

# COMMAND ----------

# DBTITLE 1,Cell 15
display(dbutils.fs.ls("/Volumes/workspace/default/ecommerce/"))

# COMMAND ----------

# DBTITLE 1,Download CSV Files
# MAGIC %md
# MAGIC ## Download CSV Files
# MAGIC
# MAGIC Your CSV files are saved in the Unity Catalog Volume:
# MAGIC
# MAGIC **File Locations:**
# MAGIC - `top_products.csv`: `/Volumes/workspace/default/ecommerce/top_products.csv`
# MAGIC - `top_users.csv`: `/Volumes/workspace/default/ecommerce/top_users.csv`
# MAGIC
# MAGIC **To download these files:**
# MAGIC
# MAGIC 1. **Via Catalog Explorer:**
# MAGIC    - Click on "Catalog" in the left sidebar
# MAGIC    - Navigate to: `workspace` → `default` → `ecommerce` (volume)
# MAGIC    - Find the CSV files and click the download icon
# MAGIC
# MAGIC 2. **Via Files UI:**
# MAGIC    - Click on "Workspace" in the left sidebar
# MAGIC    - Navigate to "Files" → "Volumes" → `workspace/default/ecommerce`
# MAGIC    - Right-click on each file and select "Download"
# MAGIC
# MAGIC 3. **Direct file access:** The files are stored at the paths shown above and can be accessed from any notebook in this workspace.

# COMMAND ----------

# DBTITLE 1,Preview CSV Files
# Preview the CSV files before downloading
import pandas as pd

print("Top Products CSV:")
top_products_preview = pd.read_csv("/Volumes/workspace/default/ecommerce/top_products.csv")
display(top_products_preview)

print("\nTop Users CSV:")
top_users_preview = pd.read_csv("/Volumes/workspace/default/ecommerce/top_users.csv")
display(top_users_preview)