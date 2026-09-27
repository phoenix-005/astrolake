from pyspark.sql import SparkSession
import argparse
import fastavro
import io
from typing import Iterator
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import pandas_udf, col
import json


def parse_value(value_series: pd.Series) -> pd.DataFrame:
    results = []
    for value in value_series:
        if not value:
            results.append(None)
            continue
        bytes_stream = io.BytesIO(value)
        record = next(fastavro.reader(bytes_stream), None)
        results.append(record)

    return pd.DataFrame(results)


def main():
    parser = argparse.ArgumentParser(description="Spark Ingestion")
    parser.add_argument("--storage_endpoint", required=True)
    parser.add_argument("--storage_access_key", required=True)
    parser.add_argument("--storage_secret_key", required=True)
    parser.add_argument("--warehouse_path", required=True)
    parser.add_argument("--kafka_server", required=True)
    parser.add_argument("--kafka_security_protocol", required=True)
    parser.add_argument("--kafka_sasl_mechanism", required=True)
    parser.add_argument("--kafka_topics", required=True)
    parser.add_argument("--kafka_username", required=True)
    parser.add_argument("--kafka_password", required=True)
    parser.add_argument("--alert_type", required=True)
    args = parser.parse_args()

    alert_type = args.alert_type
    table = f"astro_lake.silver.{alert_type}_alerts"

    jaas_config = (
        'org.apache.kafka.common.security.scram.ScramLoginModule required '
        f'username="{args.kafka_username}" '
        f'password="{args.kafka_password}";'
    )

    spark = SparkSession.builder \
        .appName("kafka_to_iceberg") \
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", "1000") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3,org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0,org.apache.hadoop:hadoop-aws:3.5.0,com.amazonaws:aws-java-sdk-bundle:1.12.797") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.astro_lake", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.astro_lake.type", "hadoop") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.catalog.astro_lake.warehouse", args.warehouse_path) \
        .config("spark.hadoop.fs.s3a.endpoint", args.storage_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", args.storage_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", args.storage_secret_key) \
        .getOrCreate()

    df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", args.kafka_server) \
        .option("kafka.security.protocol", args.kafka_security_protocol) \
        .option("kafka.sasl.mechanism", args.kafka_sasl_mechanism) \
        .option("kafka.sasl.jaas.config", jaas_config) \
        .option("subscribe", args.kafka_topics) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    with open("schemas/babamul_ztf_schema.json") as schema_file:
        schema_dict = json.load(schema_file)

    value_schema = StructType.fromJson(schema_dict)
    parse_value_udf = pandas_udf(parse_value, returnType=value_schema)
    df = df.withColumn("data", parse_value_udf(col("value"))) \
            .select("*", "data.*") \
            .drop("value", "data")

    if not spark.catalog.tableExists(table):
        empty_df = spark.createDataFrame([], schema=df.schema)
        empty_df.writeTo(table) \
                .using("iceberg") \
                .create()

    query = df.writeStream \
        .format("iceberg") \
        .trigger(availableNow=True) \
        .option("checkpointLocation", f"{args.warehouse_path}/checkpoints/{args.kafka_topics.replace(",", "_")}") \
        .toTable(table)

    query.awaitTermination()


if __name__ == '__main__':
    main()
