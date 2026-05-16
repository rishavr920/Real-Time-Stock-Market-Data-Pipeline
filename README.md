# Real-Time Stock Market Data Pipeline

A real-time data engineering pipeline that simulates live stock market events, streams data through Apache Kafka, stores processed records in AWS S3, and enables serverless analytics using AWS Glue and AWS Athena.

---

## Overview

This project demonstrates an end-to-end real-time data pipeline commonly used in modern data engineering systems.

The pipeline simulates stock market events (such as AAPL and TSLA price updates), processes them in real time, stores them in a scalable cloud-based data lake, and allows SQL-based analytics on top of the stored data.

---

## Architecture

### 1. Producer Service

* Generates simulated real-time stock market data
* Publishes events continuously to Apache Kafka topics

Example event:

```json
{
  "symbol": "AAPL",
  "price": 150.50,
  "timestamp": "2026-05-16T10:30:00"
}
```

---

### 2. Apache Kafka

* Acts as the distributed message broker
* Buffers and streams incoming events reliably
* Decouples producers and consumers for scalability

---

### 3. Consumer Service

* Reads messages from Kafka
* Processes data in batches
* Uploads batch files to AWS S3 in Parquet/JSON format

---

### 4. AWS S3 (Data Lake)

* Stores streaming stock market data
* Provides scalable and cost-effective cloud storage
* Serves as the central data lake for analytics

---

### 5. AWS Glue

* Crawls S3 data automatically
* Infers schema and creates metadata tables
* Builds a searchable Data Catalog without manual schema creation

---

### 6. AWS Athena

* Runs SQL queries directly on S3 data
* Enables serverless analytics without managing databases
* Useful for aggregations, filtering, and trend analysis

Example query:

```sql
SELECT AVG(price)
FROM stock_data
WHERE symbol = 'AAPL';
```

---

## Tech Stack

* Python
* Apache Kafka
* AWS S3
* AWS Glue
* AWS Athena
* Streamlit
* Parquet

---

## Key Features

* Real-time event streaming
* Distributed messaging with Kafka
* Batch-based cloud uploads
* Data lake architecture on AWS
* Automatic schema discovery using Glue Crawlers
* Serverless SQL analytics with Athena
* Real-time dashboard visualization using Streamlit

---

## Project Workflow

```text
Producer → Kafka → Consumer → AWS S3 → AWS Glue → AWS Athena
```

---

## Running the Project Locally

### 1. Clone the Repository

```bash
git clone <repository-url>
cd 02-stock-market-pipeline
```

---

### 2. Create and Activate Virtual Environment

```bash
python -m venv venv
source venv/bin/activate
```

---

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

---

### 4. Configure AWS Credentials

Create a `.env` file and add:

```env
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=your_region
S3_BUCKET_NAME=your_bucket_name
```

---

### 5. Start Kafka

```bash
brew services start kafka
```

---

### 6. Run Producer

```bash
python producer.py
```

---

### 7. Run Consumer

```bash
python consumer.py
```

---

### 8. Launch Dashboard (Optional)

```bash
streamlit run dashboard.py
```

Dashboard:

```text
http://localhost:8501
```

---

## Use Cases

* Real-time analytics systems
* Financial data pipelines
* Event-driven architectures
* Streaming data processing
* Data lake implementations
* Serverless querying workflows

---

## Learning Outcomes

This project demonstrates practical experience with:

* Distributed streaming systems
* Real-time data ingestion
* Cloud-based storage architectures
* Serverless analytics
* Data engineering workflow design
* Scalable event processing systems

---

## Future Improvements

* Dockerized deployment
* Kafka cluster setup
* Spark-based stream processing
* Airflow orchestration
* CI/CD integration
* Real-time alerting system
* Monitoring with Prometheus and Grafana
