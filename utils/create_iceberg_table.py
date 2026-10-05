from pyspark.sql import SparkSession
import argparse
from pyspark.sql import functions as F


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True, help="Stream project")
    args = parser.parse_args()

    project = args.project

    spark = SparkSession.builder \
        .appName("CreateTableAndSchema") \
        .getOrCreate()

    try:
        table = f"astrolake.silver.{project}"

        if not spark.catalog.tableExists(table):
            schema_sample_path = f"s3a://warehouse/astrolake/schemas/{project}/schema_sample.avro"
            schema_ddl = spark.read.format("avro").load(schema_sample_path).schema.toDDL()
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {table} (topic STRING, partition INT, offset BIGINT, timestamp_type INT,
                timestamp BIGINT, {schema_ddl}) USING iceberg
                """)
            spark.sql(f"CREATE TABLE IF NOT EXISTS {table} ({schema_ddl}) USING iceberg")

            # Save schema in parquet
            df = spark.table(table)
            df.write.mode("overwrite").parquet(f"s3a://warehouse/astrolake/schemas/{project}/schema_sample.parquet")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()