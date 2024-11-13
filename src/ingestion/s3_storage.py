import boto3
import json
import logging
from datetime import datetime
from botocore.exceptions import ClientError
from typing import Dict, Optional
import asyncio

logger = logging.getLogger(__name__)

class S3Manager:
    def __init__(self, settings):
        self.settings = settings
        self.storage_errors = 0
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
            region_name=settings.AWS_REGION
        )
        
    async def store_data(self, data: Dict, retry_count: int = 0) -> bool:
        """
        Store processed data in S3
        Returns True if successful, False otherwise
        """
        try:
            # Generate S3 key based on timestamp and data
            timestamp = datetime.utcnow().strftime('%Y/%m/%d/%H/%M')
            key = f"{timestamp}/{data.get('id', datetime.utcnow().timestamp())}.json"
            
            # Convert data to JSON string
            json_data = json.dumps(data)
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.settings.S3_BUCKET_NAME,
                Key=key,
                Body=json_data.encode('utf-8'),
                ContentType='application/json'
            )
            
            logger.info(f"Successfully stored data in S3: {key}")
            return True
            
        except ClientError as e:
            self.storage_errors += 1
            logger.error(f"Error storing data in S3: {str(e)}")
            
            # Implement retry logic
            if retry_count < self.settings.MAX_RETRY_ATTEMPTS:
                logger.info(f"Retrying S3 upload. Attempt {retry_count + 1} of {self.settings.MAX_RETRY_ATTEMPTS}")
                await asyncio.sleep(2 ** retry_count)  # Exponential backoff
                return await self.store_data(data, retry_count + 1)
            
            return False
            
    async def check_bucket_exists(self) -> bool:
        """Check if the configured S3 bucket exists"""
        try:
            self.s3_client.head_bucket(Bucket=self.settings.S3_BUCKET_NAME)
            return True
        except ClientError:
            return False
            
    async def create_bucket_if_not_exists(self):
        """Create the S3 bucket if it doesn't exist"""
        if not await self.check_bucket_exists():
            try:
                self.s3_client.create_bucket(
                    Bucket=self.settings.S3_BUCKET_NAME,
                    CreateBucketConfiguration={
                        'LocationConstraint': self.settings.AWS_REGION
                    }
                )
                logger.info(f"Created S3 bucket: {self.settings.S3_BUCKET_NAME}")
            except ClientError as e:
                logger.error(f"Error creating S3 bucket: {str(e)}")
                raise