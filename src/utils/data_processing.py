# src/utils/data_processing.py

import json
from src.utils.logger import logger

def process_data(raw_data: bytes) -> str:
    try:
        # Assuming the raw data is in JSON format
        data = json.loads(raw_data.decode("utf-8"))
        # Add processing logic here if needed
        logger.info(f"Processed data: {data}")
        return json.dumps(data)  # Return as JSON string for S3 storage
    except Exception as e:
        logger.error(f"Data processing failed: {e}")
        raise e
