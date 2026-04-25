from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

TRANSACTION_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("customer_name", StringType(), True),
        StructField("customer_email", StringType(), True),
        StructField("customer_country", StringType(), True),
        StructField("amount", DoubleType(), False),
        StructField("currency", StringType(), True),
        StructField("status", StringType(), False),
        StructField("product_category", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("timestamp", StringType(), False),
        StructField("ip_address", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("payment_method", StringType(), True),
    ]
)
