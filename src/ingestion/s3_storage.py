# src/ingestion/s3_storage.py

import boto3
from src.config import S3_BUCKET_NAME, S3_REGION_NAME, AWS_ACCESS_KEY, AWS_SECRET_KEY
from src.utils.logger import logger
import uuid

class S3StorageService:
    def __init__(self):
        self.s3_client = boto3.client(
            "s3",
            region_name=S3_REGION_NAME,
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
        )

    def upload_data(self, data: str):
        object_key = f"data_{uuid.uuid4()}.json"
        response = self.s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=object_key,
            Body=data,
            ContentType="application/json",
        )
        logger.info(f"Data uploaded to S3 with key: {object_key}")
        return response
