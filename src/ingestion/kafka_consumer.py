# src/ingestion/kafka_consumer.py

from aiokafka import AIOKafkaConsumer
from src.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC
from src.utils.logger import logger

class KafkaConsumerService:
    def __init__(self):
        self.consumer = AIOKafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id="ethdataflow-group"
        )

    async def start_consumer(self):
        await self.consumer.start()
        logger.info("Kafka consumer started")

    async def consume_messages(self, process_func):
        try:
            async for msg in self.consumer:
                logger.info(f"Message received: {msg.value}")
                await process_func(msg.value)
        finally:
            await self.consumer.stop()
