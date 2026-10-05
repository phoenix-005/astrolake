from pyspark.sql import SparkSession
import argparse


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id", required=True, help="DAG run id to get DAG run ingestion location")
    parser.add_argument("--project", required=True, help="Stream project")
    args = parser.parse_args()

    ingestion_folder_name = args.run_id.replace(":", "").replace("+", "")
    project = args.project

    spark = SparkSession.builder \
        .appName("ParquetToIceberg") \
        .getOrCreate()

    try:
        table = f"astrolake.silver.{project}"

        new_data_df = spark.read.parquet(f"s3a://warehouse/astrolake/ingestion/{ingestion_folder_name}/")

        new_data_df.writeTo(table).append()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()