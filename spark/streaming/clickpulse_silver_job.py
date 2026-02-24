import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

load_dotenv()

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
BUCKET = os.getenv("BUCKET_NAME")

spark = SparkSession.builder\
            .appName("ClickPulseSilverBatch")\
            .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY)\
            .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY)\
            .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

bronze_df = spark.read.parquet(f"s3a://{BUCKET}/bronze/")

silver_df = (
    bronze_df
    .filter(col("price").isNotNull())
    .filter(col("price") > 0)
    .filter(col("order_id").isNotNull())
    .filter(col("user_id").isNotNull())
    .filter(col("category_code").isNotNull())
    .dropDuplicates(["order_id", "event_time"])
)

# Incremental write by partition overwrite (better than full overwrite)
silver_df.write\
    .mode("append")\
    .partitionBy("year", "month", "day")\
    .parquet(f"s3a://{BUCKET}/silver/")

spark.stop()
