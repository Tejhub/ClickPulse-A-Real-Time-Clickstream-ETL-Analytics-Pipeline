from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum as _sum,
    countDistinct,
    min,
    max,
    desc
)

spark = SparkSession.builder \
    .appName("ClickPulseGoldForTableau") \
    .getOrCreate()

# --------------------------------------------------
# Paths (LOCAL FS — IMPORTANT)
# --------------------------------------------------
silver_path = "file:/home/talentum/ETL_Project/data_lake/silver"
csv_base_path = "file:/home/talentum/ETL_Project/tableau_csv"

# --------------------------------------------------
# Read SILVER
# --------------------------------------------------
silver_df = spark.read.parquet(silver_path)

# --------------------------------------------------
# 1️⃣ Revenue by Category
# --------------------------------------------------
revenue_by_category = silver_df \
    .filter(col("category_code").isNotNull()) \
    .filter(col("price").isNotNull()) \
    .groupBy("category_code") \
    .agg(_sum("price").alias("total_revenue"))

revenue_by_category.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(f"{csv_base_path}/revenue_by_category")

# --------------------------------------------------
# 2️⃣ Top Products by Revenue
# --------------------------------------------------
top_products = silver_df \
    .filter(col("product_id").isNotNull()) \
    .filter(col("price").isNotNull()) \
    .withColumn("product_id", col("product_id").cast("string")) \
    .groupBy("product_id") \
    .agg(_sum("price").alias("product_revenue")) \
    .orderBy(desc("product_revenue")) \
    .limit(10)

top_products.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(f"{csv_base_path}/top_products")

# --------------------------------------------------
# 3️⃣ Revenue per User
# --------------------------------------------------
revenue_per_user = silver_df \
    .filter(col("user_id").isNotNull()) \
    .filter(col("price").isNotNull()) \
    .withColumn("user_id", col("user_id").cast("string")) \
    .groupBy("user_id") \
    .agg(
        countDistinct("order_id").alias("total_orders"),
        _sum("price").alias("total_spent")
    ) \
    .orderBy(desc("total_spent"))

revenue_per_user.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(f"{csv_base_path}/revenue_per_user")

# --------------------------------------------------
# 4️⃣ User Activity (First & Last Event Time)
# --------------------------------------------------
user_activity = silver_df \
    .filter(col("user_id").isNotNull()) \
    .withColumn("user_id", col("user_id").cast("string")) \
    .groupBy("user_id") \
    .agg(
        min("event_time").alias("first_activity_time"),
        max("event_time").alias("last_activity_time")
    )

user_activity.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(f"{csv_base_path}/user_activity")

spark.stop()

