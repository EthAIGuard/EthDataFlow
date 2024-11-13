# EthDataFlow

EthDataFlow is a microservice component of the EthAIGuard platform, designed to handle real-time data ingestion, processing, and distribution. Built with Python and FastAPI, EthDataFlow leverages Apache Kafka for data streaming and Amazon S3 for scalable storage. 

## Table of Contents
- [Overview](#overview)
- [Architecture](#architecture)
- [Setup](#setup)
- [Usage](#usage)
- [Folder Structure](#folder-structure)
- [Technologies](#technologies)
- [Contributing](#contributing)
- [License](#license)

## Overview

EthDataFlow is responsible for ingesting data from various sources in real-time, transforming and preparing it for analysis, and distributing it to relevant microservices. It ensures reliable and secure data handling, supporting EthAIGuard’s compliance and monitoring capabilities.

## Architecture

- **Apache Kafka**: Facilitates data streaming, organizing data in topics for efficient real-time processing.
- **FastAPI**: Provides RESTful API capabilities for data processing endpoints.
- **Amazon S3**: Stores large datasets and historical data for compliance auditing.

### Data Flow

1. **Data Ingestion**: Data is ingested via Kafka topics, ensuring real-time availability.
2. **Data Processing**: Transformations and validations occur in the processing layer.
3. **Data Distribution**: Processed data is stored in Amazon S3 for historical access and shared with other components.

## Setup

### Prerequisites

- Docker
- Python 3.8+
- Access to an Apache Kafka Cluster
- AWS S3 Bucket and AWS CLI configured

### Installation

1. Clone the repository:
    ```bash
    git clone https://github.com/yourusername/EthDataFlow.git
    cd EthDataFlow
    ```

2. Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```

3. Configure environment variables:
    - Set Kafka and S3 credentials in `config.py`.

### Running the Service

- **Docker**:
    ```bash
    docker-compose up
    ```
- **Locally**:
    ```bash
    uvicorn src.main:app --reload
    ```

## Usage

API documentation is available at `http://localhost:8000/docs` when the service is running locally.

### Example Requests

Ingest data into Kafka:
```bash
curl -X POST "http://localhost:8000/ingest" -H "Content-Type: application/json" -d '{"data": "sample data"}'
