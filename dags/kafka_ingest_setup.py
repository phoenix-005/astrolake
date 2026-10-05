import pendulum
from airflow.sdk import DAG, task, Variable
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from utils.ingestion import save_sample_avro_ocf
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

PROJECTS = ["ztf", "lsst"]
SPARK_PACKAGES = "org.apache.hadoop:hadoop-aws:3.5.0,org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0,org.apache.iceberg:iceberg-aws-bundle:1.11.0,org.apache.spark:spark-avro_2.13:4.1.0"


def create_dag(dag_id, dag_project):
    with DAG(
            dag_id=dag_id,
            start_date=pendulum.datetime(2026, 1, 1, tz="America/New_York"),
            schedule=None,
    ) as dag:
        save_schema_sample = ConsumeFromTopicOperator(
            kafka_config_id="kafka-default",
            task_id="save_schema_sample",
            topics=Variable.get(f"{dag_project}_topics", deserialize_json=True),
            apply_function=save_sample_avro_ocf,
            apply_function_kwargs={"project": dag_project},
            commit_cadence="never",
            max_messages=1,
        )

        create_iceberg_table = SparkSubmitOperator(
            task_id="create_table",
            application="utils/create_iceberg_table.py",
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
                "--project", dag_project,
            ]
        )

        save_schema_sample >> create_iceberg_table

    return dag

for project in PROJECTS:
    generated_dag_id = f"setup_kafka_{project}_ingest"

    globals()[generated_dag_id] = create_dag(
        dag_id=generated_dag_id,
        dag_project=project,
    )