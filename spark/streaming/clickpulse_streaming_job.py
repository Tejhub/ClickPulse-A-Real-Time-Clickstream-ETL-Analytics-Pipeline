
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType

spark = SparkSession.builder     .appName("ClickPulseFullDatasetStreaming")     .getOrCreate()

schema = StructType()     .add("event_time", StringType())     .add("order_id", StringType())     .add("product_id", StringType())     .add("category_id", StringType())     .add("category_code", StringType())     .add("brand", StringType())     .add("price", DoubleType())     .add("user_id", StringType())

df = spark.readStream     .format("kafka")     .option("kafka.bootstrap.servers", "localhost:9092")     .option("subscribe", "clickpulse_events")     .load()

parsed = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

query = parsed.writeStream \
    .format("parquet") \
    .option("path", "file:/home/talentum/ETL_Project/data_lake/bronze") \
	.option("checkpointLocation", "file:/home/talentum/ETL_Project/spark/checkpoints") \
    .trigger(processingTime="10 seconds") \
    .outputMode("append") \
    .start()


query.awaitTermination()
