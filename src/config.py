# src/config.py

import os

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "eth_data_flow_topic")

# S3 Configuration
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "ethdataflow-bucket")
S3_REGION_NAME = os.getenv("S3_REGION_NAME", "us-east-1")

# AWS Credentials (replace with AWS secrets manager in production)
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY", "your-access-key")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY", "your-secret-key")
