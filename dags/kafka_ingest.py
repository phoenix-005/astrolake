import pendulum
from airflow.sdk import DAG, task, Variable
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from utils.ingestion import avro_to_parquet
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator

PROJECTS = ["ztf", "lsst"]
SPARK_PACKAGES = "org.apache.hadoop:hadoop-aws:3.5.0,org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0,org.apache.iceberg:iceberg-aws-bundle:1.11.0"


def create_dag(dag_id, dag_project):
    with DAG(
            dag_id=dag_id,
            start_date=pendulum.datetime(2026, 1, 1, tz="America/New_York"),
            schedule=None,
    ) as dag:
        to_parquet = ConsumeFromTopicOperator(
            kafka_config_id="kafka-default",
            task_id="ingest_alerts",
            topics=Variable.get(f"{dag_project}_topics", deserialize_json=True),
            apply_function_batch=avro_to_parquet,
            apply_function_kwargs={"run_id": "{{ run_id }}", "project": dag_project},
            commit_cadence="end_of_operator",
            max_batch_size=1000,
        )

        to_iceberg = SparkSubmitOperator(
            task_id="push_to_iceberg",
            application="utils/to_iceberg_with_spark.py",
            conn_id="spark_default",
            packages=SPARK_PACKAGES,
            conf={
                "spark.hadoop.fs.s3a.access.key": "{{ conn.warehouse_default.login }}",
                "spark.hadoop.fs.s3a.secret.key": "{{ conn.warehouse_default.password }}",
                "spark.hadoop.fs.s3a.endpoint": "{{ conn.warehouse_default.extra_dejson.endpoint_url }}",
                "spark.hadoop.fs.s3a.endpoint.region": "{{ conn.warehouse_default.extra_dejson.region_name }}",
                "spark.hadoop.fs.s3a.path.style.access": "true",
                "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
                "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
                "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                "spark.sql.catalog.astrolake": "org.apache.iceberg.spark.SparkCatalog",
                "spark.sql.catalog.astrolake.warehouse": "s3a://warehouse/astrolake",
                "spark.sql.catalog.astrolake.type": "hadoop",
                "spark.sql.catalog.astrolake.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
                "spark.sql.catalog.astrolake.s3.endpoint": "{{ conn.warehouse_default.extra_dejson.endpoint_url }}",
                "spark.sql.catalog.astrolake.client.region": "{{ conn.warehouse_default.extra_dejson.region_name }}",
                "spark.sql.catalog.astrolake.s3.path-style-access": 'true',
                "spark.sql.catalog.astrolake.s3.access-key-id": "{{ conn.warehouse_default.login }}",
                "spark.sql.catalog.astrolake.s3.secret-access-key": "{{ conn.warehouse_default.password }}",
            },
            application_args=[
                "--run_id", "{{ run_id }}",
                "--project", dag_project,
            ]
        )

        # delete temporary files
        clean_ingestion = S3DeleteObjectsOperator(
            task_id="clean",
            bucket="warehouse",
            prefix="astrolake/ingestion/{{ run_id | replace(':', '') | replace('+', '') }}",
            aws_conn_id="warehouse_default",
        )
        to_parquet >> to_iceberg >> clean_ingestion

    return dag

for project in PROJECTS:
    generated_dag_id = f"kafka_{project}_ingest"

    globals()[generated_dag_id] = create_dag(
        dag_id=generated_dag_id,
        dag_project=project,
    )