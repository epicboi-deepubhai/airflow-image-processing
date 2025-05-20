from datetime import timedelta, datetime
from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from utils.s3_operations import download_from_s3, upload_to_s3, file_in_bucket
from utils.image_processing import resize_image, add_watermark
from utils.notifications import send_sns_notification
import os
import logging

logger = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

SOURCE_BUCKET = Variable.get("source_bucket", default_var="original-images")
PROCESSED_BUCKET = Variable.get("processed_bucket", default_var="processed-images")
SNS_TOPIC_ARN = Variable.get("sns_topic_arn")
TEMP_DIR = Variable.get("temp_dir", default_var="/tmp/airflow_image_processing")

os.makedirs(TEMP_DIR, exist_ok=True)

with DAG(
    "image_processing_pipeline",
    default_args=default_args,
    description="Process images from S3, add watermark, and resize",
    start_date=datetime(2025, 4, 21),
    schedule_interval="@daily",
    catchup=False,
    tags=["image-processing", "s3", "sns"],
) as dag:

    check_for_new_images = S3KeySensor(
        task_id="check_for_new_images",
        bucket_name=SOURCE_BUCKET,
        bucket_key="*.jpg",
        wildcard_match=True,
        aws_conn_id="aws_default",
        timeout=60 * 5,
        poke_interval=60,
        mode="poke",
    )

    list_new_images = S3ListOperator(
        task_id="list_new_images",
        bucket=SOURCE_BUCKET,
        prefix="",
        delimiter="/",
        aws_conn_id="aws_default",
    )

    def download_image(**context):
        ti = context["ti"]
        s3_files = ti.xcom_pull(task_ids="list_new_images")

        logger.info(f"Found pottetial {len(s3_files)} files to process: {s3_files}")

        downloaded_files = []
        for file_key in s3_files:
            if file_key.endswith(".jpg") or file_key.endswith(".jpeg"):

                processed_key = f"processed/{os.path.basename(file_key)}"
                if file_in_bucket(PROCESSED_BUCKET, processed_key):
                    continue

                local_path = os.path.join(TEMP_DIR, os.path.basename(file_key))
                download_from_s3(SOURCE_BUCKET, file_key, local_path)
                downloaded_files.append(
                    {"original_key": file_key, "local_path": local_path}
                )

        if not downloaded_files:
            logger.info("No new files to process.")

        return downloaded_files

    download_task = PythonOperator(
        task_id="download_images",
        python_callable=download_image,
        provide_context=True,
        dag=dag,
    )

    def resize_images(**context):
        print(context)
        ti = context["ti"]
        downloaded_files = ti.xcom_pull(task_ids="download_images")

        if not downloaded_files:
            logger.info("No files to resize")
            return []

        resized_files = []
        for file_info in downloaded_files:
            local_path = file_info["local_path"]
            resize_image(local_path)
            resized_files.append(file_info)

        return resized_files

    resize_task = PythonOperator(
        task_id="resize_images",
        python_callable=resize_images,
        provide_context=True,
        dag=dag,
    )

    def watermark_image(**context):
        ti = context["ti"]
        resized_files = ti.xcom_pull(task_ids="resize_images")

        if not resized_files:
            logger.info("No files to watermark.")
            return []

        watermarked_files = list()
        for file_info in resized_files:
            local_path = file_info["local_path"]
            add_watermark(local_path)
            watermarked_files.append(file_info)

        return watermarked_files

    watermark_task = PythonOperator(
        task_id="watermark_images",
        python_callable=watermark_image,
        provide_context=True,
        dag=dag,
    )

    def upload_processed_images(**context):
        ti = context["ti"]
        watermarked_files = ti.xcom_pull(task_ids="watermark_images")

        if not watermarked_files:
            logger.info("No files to upload")
            return []

        uploaded_files = []
        for file_info in watermarked_files:
            local_path = file_info["local_path"]
            original_key = file_info["original_key"]

            processed_key = f"processed/{os.path.basename(original_key)}"
            s3_url = upload_to_s3(local_path, PROCESSED_BUCKET, processed_key)

            uploaded_files.append(
                {
                    "original_key": original_key,
                    "processed_key": processed_key,
                    "local_path": local_path,
                    "s3_url": s3_url,
                }
            )

            if os.path.exists(local_path):
                os.remove(local_path)
                logger.info(f"Removed temporary file: {local_path}")

        return uploaded_files

    upload_task = PythonOperator(
        task_id="upload_processed_images",
        python_callable=upload_processed_images,
        provide_context=True,
        dag=dag,
    )

    def send_notifications(**context):
        ti = context["ti"]
        uploaded_files = ti.xcom_pull(task_ids="upload_processed_images")

        if not uploaded_files:
            logger.info("No files to notify about")
            return

        for file_info in uploaded_files:
            original_key = file_info["original_key"]
            processed_key = file_info["processed_key"]

            subject = "Image Processing Notification"
            message = f"""
                \nImage processing complete!\n\n
                Original image: {original_key}
                Processed image: {processed_key}
            """

            send_sns_notification(SNS_TOPIC_ARN, subject, message)

    notify_task = PythonOperator(
        task_id="send_notifications",
        python_callable=send_notifications,
        provide_context=True,
        dag=dag,
    )

    (
        check_for_new_images
        >> list_new_images
        >> download_task
        >> resize_task
        >> watermark_task
        >> upload_task
        >> notify_task
    )
