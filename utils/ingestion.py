import fastavro
import io
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
import uuid


def save_sample_avro_ocf(message, project):
    s3_hook = S3Hook(aws_conn_id='warehouse_default')
    bucket_name = "warehouse"
    s3_hook.load_bytes(
        bytes_data=message.value(),
        key=f"astrolake/schemas/{project}/schema_sample.avro",
        bucket_name=bucket_name,
        replace=True,
    )


def avro_to_parquet(messages, run_id, project):
    ingestion_folder_name = run_id.replace(":", "").replace("+", "")

    s3_hook = S3Hook(aws_conn_id='warehouse_default')
    bucket_name = "warehouse"
    schema_object = s3_hook.get_wildcard_key(
        wildcard_key=f"astrolake/schemas/{project}/schema_sample.parquet/*.parquet",
        bucket_name=bucket_name,
    )
    file_buffer = io.BytesIO(schema_object.get()["Body"].read())
    parquet_schema = pq.read_schema(file_buffer)

    records = []
    for message in messages:
        if message and not message.error():
            records.append({
                "topic": message.topic(),
                "partition": message.partition(),
                "offset": message.offset(),
                "timestamp_type": message.timestamp()[0],
                "timestamp": message.timestamp()[1],
                **next(fastavro.reader(io.BytesIO(message.value()))),
            })
    table = pa.Table.from_pylist(records, schema=parquet_schema)

    buffer = io.BytesIO()
    pq.write_table(table, buffer)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"astrolake/ingestion/{ingestion_folder_name}/batch_{timestamp}_{uuid.uuid4().hex[:6]}.parquet"

    s3_hook.load_bytes(
        bytes_data=buffer.getvalue(),
        key=file_name,
        bucket_name=bucket_name,
        replace=True,
    )
