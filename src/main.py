from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import logging
from datetime import datetime
from typing import Dict, Optional
from .config import Settings
from .ingestion.kafka_consumer import KafkaManager
from .ingestion.s3_storage import S3Manager
from .utils.logger import setup_logger

# Inisialisasi aplikasi FastAPI
app = FastAPI(
    title="EthDataFlow",
    description="Service for data ingestion from Kafka to S3 in EthAIGuard platform",
    version="1.0.0"
)

# Setup CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Setup logging
logger = setup_logger()

# Load settings
settings = Settings()

# Initialize managers
kafka_manager = KafkaManager(settings)
s3_manager = S3Manager(settings)

@app.on_event("startup")
async def startup_event():
    """Initialize services on startup"""
    try:
        await kafka_manager.start()
        logger.info("Kafka consumer started successfully")
    except Exception as e:
        logger.error(f"Failed to start Kafka consumer: {str(e)}")
        raise

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    await kafka_manager.stop()
    logger.info("Kafka consumer stopped")

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}

@app.get("/metrics")
async def get_metrics():
    """Get processing metrics"""
    return {
        "messages_processed": kafka_manager.messages_processed,
        "storage_errors": s3_manager.storage_errors,
        "last_processed": kafka_manager.last_processed_timestamp
    }

@app.post("/process")
async def process_message(message: Dict):
    """
    Manually process a message and store it in S3
    """
    try:
        # Process message
        processed_data = kafka_manager.process_message(message)
        
        # Store in S3
        success = await s3_manager.store_data(processed_data)
        if not success:
            raise HTTPException(status_code=500, detail="Failed to store data in S3")
            
        return {"status": "success", "message": "Data processed and stored successfully"}
    except Exception as e:
        logger.error(f"Error processing message: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/topics")
async def list_topics():
    """List available Kafka topics"""
    return {"topics": await kafka_manager.list_topics()}