import json
from typing import Dict, Any, Optional, Union
from datetime import datetime
import hashlib
import logging
from .logger import setup_logger

logger = setup_logger(__name__)

class DataProcessor:
    """Class for handling data processing operations"""
    
    @staticmethod
    def validate_json(data: Union[str, Dict]) -> Dict:
        """
        Validate and parse JSON data
        
        Args:
            data: JSON string or dictionary
            
        Returns:
            Dict: Validated JSON data
            
        Raises:
            ValueError: If data is invalid
        """
        if isinstance(data, str):
            try:
                return json.loads(data)
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON data: {str(e)}")
                raise ValueError(f"Invalid JSON format: {str(e)}")
        elif isinstance(data, dict):
            return data
        else:
            raise ValueError("Data must be JSON string or dictionary")

    @staticmethod
    def normalize_timestamp(timestamp: Optional[Union[str, int, float]]) -> str:
        """
        Normalize timestamp to ISO format
        
        Args:
            timestamp: Input timestamp in various formats
            
        Returns:
            str: ISO formatted timestamp
        """
        if timestamp is None:
            return datetime.utcnow().isoformat()
            
        if isinstance(timestamp, (int, float)):
            # Assume Unix timestamp
            return datetime.fromtimestamp(timestamp).isoformat()
        
        try:
            # Try parsing string timestamp
            return datetime.fromisoformat(timestamp).isoformat()
        except (ValueError, TypeError):
            logger.warning(f"Invalid timestamp format: {timestamp}, using current time")
            return datetime.utcnow().isoformat()

    @staticmethod
    def generate_id(data: Dict) -> str:
        """
        Generate unique ID for data record
        
        Args:
            data: Input data dictionary
            
        Returns:
            str: Generated unique ID
        """
        # Create string from sorted data items for consistent hashing
        data_str = json.dumps(data, sort_keys=True)
        return hashlib.sha256(data_str.encode()).hexdigest()[:16]

    @staticmethod
    def clean_data(data: Dict) -> Dict:
        """
        Clean and standardize data fields
        
        Args:
            data: Input data dictionary
            
        Returns:
            Dict: Cleaned data
        """
        cleaned = {}
        
        for key, value in data.items():
            # Convert keys to snake_case
            clean_key = key.lower().replace(" ", "_")
            
            # Clean string values
            if isinstance(value, str):
                clean_value = value.strip()
            else:
                clean_value = value
                
            cleaned[clean_key] = clean_value
            
        return cleaned

    @staticmethod
    def enrich_data(data: Dict) -> Dict:
        """
        Enrich data with additional metadata
        
        Args:
            data: Input data dictionary
            
        Returns:
            Dict: Enriched data
        """
        enriched = data.copy()
        
        # Add metadata
        enriched["_metadata"] = {
            "processed_at": datetime.utcnow().isoformat(),
            "record_id": DataProcessor.generate_id(data),
            "version": "1.0"
        }
        
        return enriched

def process_data(data: Union[str, Dict]) -> Dict:
    """
    Main function to process incoming data
    
    Args:
        data: Input data as JSON string or dictionary
        
    Returns:
        Dict: Processed data
        
    Raises:
        ValueError: If data processing fails
    """
    try:
        # Validate JSON
        validated_data = DataProcessor.validate_json(data)
        
        # Clean data
        cleaned_data = DataProcessor.clean_data(validated_data)
        
        # Handle timestamp if present
        if "timestamp" in cleaned_data:
            cleaned_data["timestamp"] = DataProcessor.normalize_timestamp(cleaned_data["timestamp"])
        
        # Enrich with metadata
        processed_data = DataProcessor.enrich_data(cleaned_data)
        
        logger.info(
            "Data processed successfully",
            extra={
                "extra_data": {
                    "record_id": processed_data["_metadata"]["record_id"]
                }
            }
        )
        
        return processed_data
        
    except Exception as e:
        logger.error(f"Error processing data: {str(e)}")
        raise ValueError(f"Data processing failed: {str(e)}")