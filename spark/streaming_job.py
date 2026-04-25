import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    dayofmonth,
    from_json,
    hour,
    month,
    to_timestamp,
    year,
)

from config.config import (
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION,
    S3_BUCKET, S3_PREFIX,
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC,
    SPARK_APP_NAME, SPARK_MASTER,
    SPARK_CHECKPOINT_DIR, SPARK_TRIGGER_INTERVAL,
)
from spark.schema import TRANSACTION_SCHEMA


def create_spark_session():
    builder = (
        SparkSession.builder
        .appName(SPARK_APP_NAME)
        .master(SPARK_MASTER)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{AWS_REGION}.amazonaws.com")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.sql.shuffle.partitions", "4")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def run_streaming_job(spark):
    s3_path = f"s3a://{S3_BUCKET}/{S3_PREFIX}"
    checkpoint_path = f"{SPARK_CHECKPOINT_DIR}/transactions"

    print(f"Reading from Kafka topic: {KAFKA_TOPIC}")
    print(f"Writing Delta Lake to: {s3_path}")
    print(f"Checkpoint at: {checkpoint_path}\n")

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw_stream
        .select(from_json(col("value").cast("string"), TRANSACTION_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("event_timestamp", to_timestamp(col("timestamp")))
        .withColumn("year",  year(col("event_timestamp")))
        .withColumn("month", month(col("event_timestamp")))
        .withColumn("day",   dayofmonth(col("event_timestamp")))
        .withColumn("hour",  hour(col("event_timestamp")))
        .filter(col("transaction_id").isNotNull())
        .filter(col("amount") > 0)
    )

    query = (
        parsed.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("path", s3_path)
        .partitionBy("year", "month", "day")
        .trigger(processingTime=SPARK_TRIGGER_INTERVAL)
        .start()
    )

    print("Streaming job started. Waiting for data...")
    query.awaitTermination()


if __name__ == "__main__":
    spark = create_spark_session()
    try:
        run_streaming_job(spark)
    except KeyboardInterrupt:
        print("\nStreaming job stopped by user.")
    finally:
        spark.stop()
