from urllib import response
import boto3
import os
import logging
from airflow.hooks.base import BaseHook

logger = logging.getLogger(__name__)


def get_s3_client():
    conn = BaseHook.get_connection("aws_default")
    aws_access_key = conn.login
    aws_secret_key = conn.password
    region_name = conn.extra_dejson.get("region_name", "ap-south-1")

    return boto3.client(
        "s3",
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=region_name,
    )


def download_from_s3(bucket, key, local_path):
    logger.info(f"Downloading {key} from {bucket} to {local_path}")
    s3_client = get_s3_client()
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    s3_client.download_file(bucket, key, local_path)
    logger.info(f"Successfully downloaded file to {local_path}")
    return local_path


def upload_to_s3(local_path, bucket, key):
    logger.info(f"Uploading {local_path} to {bucket}/{key}")
    s3_client = get_s3_client()
    s3_client.upload_file(local_path, bucket, key)
    logger.info(f"Successfully uploaded file to {bucket}/{key}")
    return f"s3://{bucket}/{key}"


def file_in_bucket(bucket_name, file_key):
    s3_client = get_s3_client()
    try:
        s3_client.head_object(Bucket=bucket_name, Key=file_key)
        return True
    except Exception:
        return False


def get_unprocessed_files(source_bucket, processed_bucket, file_extensions, prefix=""):
    s3_client = get_s3_client()

    res = s3_client.list_objects_v2(Bucket=source_bucket, Prefix=prefix)

    all_files = []

    if "Contents" in res:
        all_files = [
            obj["Key"]
            for obj in res["Contents"]
            if any(obj["Key"].lower().endswith(ext) for ext in file_extensions)
        ]

    unprocessed_files = []

    for file_key in all_files:
        processed_key = f"processed/{os.path.basename(file_key)}"
        if not file_in_bucket(processed_bucket, processed_key):
            unprocessed_files.append(file_key)

    return unprocessed_files
