from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

# ---------------------------------------
# 1. Spark Session
# ---------------------------------------
spark = SparkSession.builder \
    .appName("ClickPulseSilverBatch") \
    .getOrCreate()

# ---------------------------------------
# 2. Read Bronze (RAW streaming output)
# ---------------------------------------
bronze_df = spark.read.parquet(
    "file:/home/talentum/ETL_Project/data_lake/bronze"
)

# ---------------------------------------
# 3. Silver Cleaning Rules
# ---------------------------------------
silver_df = (
    bronze_df
    # Parse timestamp
    .withColumn(
        "event_time",
        to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss")
    )

    # Keep only valid rows
    .filter(col("price").isNotNull())
    .filter(col("order_id").isNotNull())
    .filter(col("user_id").isNotNull())

    # VERY IMPORTANT FIX:
    # Remove rows where category_code is numeric / corrupted
    .filter(col("category_code").isNotNull())
    .filter(col("category_code").rlike("[a-zA-Z]"))

    # Remove duplicate orders
    .dropDuplicates(["order_id"])
)

# ---------------------------------------
# 4. Write Silver (overwrite, single file)
# ---------------------------------------
silver_df.coalesce(1).write \
    .mode("overwrite") \
    .parquet("file:/home/talentum/ETL_Project/data_lake/silver")

spark.stop()

