from airflow import DAG, XComArg
from airflow.decorators import task
import pendulum
import datetime

from airflow.providers.amazon.aws.operators.s3 import (
    S3CopyObjectOperator, S3ListOperator , S3DeleteObjectsOperator
)
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

#define the ingest and destination buckets
S3_INGEST_BUCKET = "caxe-demo-bucket"
S3_INTEGER_BUCKET = "caxe-demo-bucket/example-integer-bucket"
S3_INTEGER_BUCKET = "caxe-demo-bucket/example-not-integer-bucket"


with DAG(
    dag_id="2_4_exemple_dag_expand_kwargs",
    start_date=pendulum.datetime(2022,10,10, tz="UTC"),
    schedule=None,
    catchup=False,
    doc_md=__doc__,
):

    #fetch the file names from the injest S3 bucket
    list_files_ingest_bucket = S3ListOperator(
        task_id="list_file_ingest_bucket",
        aws_conn_id="aws_default",
        bucket=S3_INGEST_BUCKET
    )

    @task
    def read_keys_from_s3(source_key_list):
        # Featch the contents from all files in the S3_INGEST_BUCKET
        s3_hook = S3Hook(aws_conn_id='aws_default')
        print(source_key_list)
        content_list = []
        for key in source_key_list:
            file_content = s3_hook.read_key(
                key=key,
                bucket_name = S3_INGEST_BUCKET
            )
            content_list.append(file_content)
        return content_list 
