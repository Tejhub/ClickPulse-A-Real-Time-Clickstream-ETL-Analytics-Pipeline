
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, approx_count_distinct, to_date, dense_rank
from pyspark.sql.window import Window

load_dotenv()

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
BUCKET = os.getenv("BUCKET_NAME")

spark = SparkSession.builder\
            .appName("ClickPulseGoldForTableau")\
            .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY)\
            .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY)\
            .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

silver_df = spark.read.parquet(f"s3a://{BUCKET}/silver/")

silver_df = silver_df.withColumn("event_date", to_date("event_time"))

gold_df = (
    silver_df
    .groupBy("event_date", "brand", "category_code")
    .agg(
        sum("price").alias("total_revenue"),
        approx_count_distinct("order_id").alias("total_orders"),
        approx_count_distinct("user_id").alias("unique_customers")
    )
)

brand_window = Window.partitionBy("event_date").orderBy(col("total_revenue").desc())

gold_df = gold_df.withColumn(
    "brand_rank_daily",
    dense_rank().over(brand_window)
)

gold_df = gold_df.repartition(10, "event_date")

gold_df.write\
    .mode("append")\
    .partitionBy("event_date")\
    .parquet(f"s3a://{BUCKET}/gold/business_metrics")

spark.stop()
