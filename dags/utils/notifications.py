import boto3
import logging
from airflow.hooks.base import BaseHook

logger = logging.getLogger(__name__)

def get_sns_client():
    """Get boto3 SNS client with proper credentials"""
    conn = BaseHook.get_connection('aws_default')
    aws_access_key = conn.login
    aws_secret_key = conn.password
    region_name = conn.extra_dejson.get('region_name', 'ap-south-1')
    
    return boto3.client(
        'sns',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=region_name
    )

def send_sns_notification(topic_arn, subject, message):
    """Send notification via SNS"""
    logger.info(f"Sending SNS notification to {topic_arn}")
    
    sns_client = get_sns_client()
    response = sns_client.publish(
        TopicArn=topic_arn,
        Subject=subject,
        Message=message
    )
    
    logger.info(f"SNS notification sent: {response['MessageId']}")
    return response['MessageId']