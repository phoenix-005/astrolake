from pyspark.sql import SparkSession
import argparse
import fastavro
import io
from typing import Iterator
import pandas as pd
from pyspark.sql.types import *
import json


def parse_avro(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    for batch_df in iterator:
        batch_df = batch_df.reset_index(drop=True)
        bytes_stream = io.BytesIO(batch_df["value"])
        for record in fastavro.reader(bytes_stream):
            batch_df["value"] = record
            break
        expanded_df = pd.DataFrame(batch_df["value"].tolist())

        expanded_df = pd.concat([batch_df, expanded_df], axis=1) \
                        .drop(columns=["value"])
        yield expanded_df


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
        f'username={args.username}'
        f'password={args.password}'
    )

    spark = SparkSession.builder \
        .appName("kafka_to_iceberg") \
        .config("spark.jars.packages",
                "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0,org.apache.hadoop:hadoop-aws:3.5.0,com.amazonaws:aws-java-sdk-bundle:1.12.797") \
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

    kafka_metadata_schema = StructType([
        StructField("key", StringType()),
        StructField("topic", StringType()),
        StructField("partition", IntegerType()),
        StructField("offset", LongType()),
        StructField("timestamp", TimestampType()),
        StructField("timestampType", IntegerType()),
    ])
    with open(f"schemas/{alert_type}_schema.json") as f:
        schema_json_string = f.read()
    value_schema = StructType.fromJson(json.loads(schema_json_string))

    full_table_schema = StructType(kafka_metadata_schema.fields + value_schema.fields)

    if not spark.catalog.tableExists(table):
        empty_df = spark.createDataFrame([], schema=full_table_schema)
        empty_df.writeTo(table) \
                .using("iceberg") \
                .create()

    df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", args.kafka_server) \
        .option("kafka.security.protocol", args.security_protocol) \
        .option("kafka.sasl.mechanism", args.sasl_mechanism) \
        .option("kafka.sasl.jaas.config", jaas_config) \
        .option("subscribe", args.kafka_topics) \
        .option("startingOffsets", "earliest") \
        .load()

    df = df.mapInPandas(parse_avro, schema=full_table_schema)

    query = df.writeStream() \
        .format("iceberg") \
        .trigger(availableNow=True) \
        .option("checkpointLocation", f"{args.warehouse_path}/checkpoints/{args.kafka_topics.replace(",", "_")}") \
        .toTable(table)

    query.awaitTermination()