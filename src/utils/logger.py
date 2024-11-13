import logging
import sys
from datetime import datetime
import json
from typing import Any, Dict
import traceback

class JsonFormatter(logging.Formatter):
    """Custom JSON formatter for structured logging"""
    def format(self, record: logging.LogRecord) -> str:
        log_data: Dict[str, Any] = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno
        }

        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = {
                "type": record.exc_info[0].__name__,
                "message": str(record.exc_info[1]),
                "stacktrace": traceback.format_exception(*record.exc_info)
            }

        # Add extra fields if present
        if hasattr(record, "extra_data"):
            log_data["extra"] = record.extra_data

        return json.dumps(log_data)

def setup_logger(name: str = "ethdataflow") -> logging.Logger:
    """
    Setup application logger with JSON formatting and appropriate handlers
    
    Args:
        name (str): Logger name
        
    Returns:
        logging.Logger: Configured logger instance
    """
    logger = logging.getLogger(name)
    
    # Clear any existing handlers
    logger.handlers.clear()
    
    # Set logging level from environment or default to INFO
    logger.setLevel(logging.INFO)
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(JsonFormatter())
    logger.addHandler(console_handler)
    
    # Prevent propagation to root logger
    logger.propagate = False
    
    return logger

class LoggerContextManager:
    """Context manager for adding context to logs"""
    def __init__(self, logger: logging.Logger, **context):
        self.logger = logger
        self.context = context
        self.original_extra = {}

    def __enter__(self):
        # Store original extra data if exists
        for handler in self.logger.handlers:
            if hasattr(handler, "formatter") and hasattr(handler.formatter, "extra"):
                self.original_extra[handler] = handler.formatter.extra.copy()
            else:
                self.original_extra[handler] = {}
                
            # Update with new context
            handler.formatter.extra = {
                **self.original_extra[handler],
                **self.context
            }
        return self.logger

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Restore original extra data
        for handler in self.logger.handlers:
            if handler in self.original_extra:
                handler.formatter.extra = self.original_extra[handler]

def log_execution_time(logger: logging.Logger):
    """Decorator to log function execution time"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            start_time = datetime.now()
            result = func(*args, **kwargs)
            execution_time = (datetime.now() - start_time).total_seconds()
            
            logger.info(
                f"Function {func.__name__} executed",
                extra={
                    "extra_data": {
                        "execution_time_seconds": execution_time,
                        "function_name": func.__name__
                    }
                }
            )
            return result
        return wrapper
    return decorator