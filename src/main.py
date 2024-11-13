# src/main.py

import asyncio
from fastapi import FastAPI
from src.ingestion.kafka_consumer import KafkaConsumerService
from src.ingestion.s3_storage import S3StorageService
from src.utils.data_processing import process_data
from src.utils.logger import logger

app = FastAPI()

# Initialize services
consumer_service = KafkaConsumerService()
s3_service = S3StorageService()

@app.on_event("startup")
async def startup_event():
    await consumer_service.start_consumer()
    asyncio.create_task(consume_and_store_data())

async def consume_and_store_data():
    await consumer_service.consume_messages(save_to_s3)

async def save_to_s3(raw_data: bytes):
    processed_data = process_data(raw_data)
    s3_service.upload_data(processed_data)
