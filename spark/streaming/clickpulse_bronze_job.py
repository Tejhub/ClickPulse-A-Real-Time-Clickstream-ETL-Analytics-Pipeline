import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, year, month, dayofmonth
from pyspark.sql.types import StructType, StringType, DoubleType

load_dotenv()

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID") 
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
BUCKET = os.getenv("BUCKET_NAME")

spark = SparkSession.builder\
            .appName("ClickPulseBronzeStreaming")\
            .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY)\
            .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY)\
            .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType()\
            .add("event_time", StringType())\
            .add("order_id", StringType())\
            .add("product_id", StringType())\
            .add("category_id", StringType())\
            .add("category_code", StringType())\
            .add("brand", StringType())\
            .add("price", DoubleType())\
            .add("user_id", StringType())

df = spark.readStream\
            .format("kafka")\
            .option("kafka.bootstrap.servers", "localhost:9092")\
            .option("subscribe", "clickpulse_realtime_event")\
            .option("startingOffsets", "earliest")\
            .load()

parsed = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

parsed = parsed.withColumn(
    "event_time",
    to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss 'UTC'")
).withWatermark("event_time", "10 minutes")

parsed = parsed.withColumn("year", year("event_time"))\
            .withColumn("month", month("event_time"))\
            .withColumn("day", dayofmonth("event_time"))

query = parsed.writeStream\
                .format("parquet")\
                .option("path", f"s3a://{BUCKET}/bronze/")\
                .option("checkpointLocation", f"s3a://{BUCKET}/checkpoints/bronze/")\
                .partitionBy("year", "month", "day")\
                .outputMode("append")\
                .trigger(processingTime="30 seconds")\
                .start()

query.awaitTermination()
