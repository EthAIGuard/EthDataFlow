from aiokafka import AIOKafkaConsumer
import json
import logging
from datetime import datetime
from typing import Dict, Optional, List
import asyncio
from ..utils.data_processing import process_data

logger = logging.getLogger(__name__)

class KafkaManager:
    def __init__(self, settings):
        self.settings = settings
        self.consumer = None
        self.messages_processed = 0
        self.last_processed_timestamp = None
        self.running = False
        
    async def start(self):
        """Start the Kafka consumer"""
        self.consumer = AIOKafkaConsumer(
            self.settings.KAFKA_TOPIC,
            bootstrap_servers=self.settings.KAFKA_BOOTSTRAP_SERVERS,
            group_id=self.settings.KAFKA_GROUP_ID,
            auto_offset_reset=self.settings.KAFKA_AUTO_OFFSET_RESET,
            enable_auto_commit=True
        )
        
        await self.consumer.start()
        self.running = True
        asyncio.create_task(self._consume_messages())
        logger.info(f"Started consuming from topic: {self.settings.KAFKA_TOPIC}")
        
    async def stop(self):
        """Stop the Kafka consumer"""
        self.running = False
        if self.consumer:
            await self.consumer.stop()
            
    async def _consume_messages(self):
        """Main consumption loop"""
        try:
            while self.running:
                message_batch = await self.consumer.getmany(timeout_ms=1000)
                for tp, messages in message_batch.items():
                    for message in messages:
                        try:
                            # Process message
                            data = json.loads(message.value.decode())
                            processed_data = self.process_message(data)
                            
                            # Update metrics
                            self.messages_processed += 1
                            self.last_processed_timestamp = datetime.utcnow().isoformat()
                            
                            logger.debug(f"Processed message from topic {tp.topic}, partition {tp.partition}")
                        except Exception as e:
                            logger.error(f"Error processing message: {str(e)}")
                            
        except Exception as e:
            logger.error(f"Error in consumer loop: {str(e)}")
            raise
            
    def process_message(self, message: Dict) -> Dict:
        """Process a single message"""
        try:
            # Apply data processing logic
            processed_data = process_data(message)
            return processed_data
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")
            raise
            
    async def list_topics(self) -> List[str]:
        """List available Kafka topics"""
        try:
            if not self.consumer:
                await self.start()
            topics = await self.consumer.topics()
            return list(topics)
        except Exception as e:
            logger.error(f"Error listing topics: {str(e)}")
            return []