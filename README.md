# Streaming Data Platform

## Project Overview

This project demonstrates an end-to-end real-time data pipeline using Apache Kafka, Apache Spark, and PostgreSQL.

The system simulates e-commerce order events, streams them through Kafka, processes them using Spark, performs business analytics, and stores the processed data in PostgreSQL for reporting and analysis.

---

## Architecture

Data Generator
      ↓
Kafka Producer
      ↓
Kafka Topic (orders)
      ↓
Spark Consumer
      ↓
Data Transformations
      ↓
Business Analytics
      ↓
PostgreSQL

---

## Tech Stack

* Python
* Apache Kafka
* Apache Spark (PySpark)
* PostgreSQL
* Docker
* Git & GitHub

---

## Project Components

### Kafka Layer

**producer.py**

* Generates order events
* Publishes messages to Kafka topic

**consumer.py**

* Consumes messages from Kafka
* Verifies message delivery

### Spark Layer

**parse_orders.py**

* Reads order events from Kafka
* Parses JSON messages
* Applies transformations
* Calculates business metrics

**load_to_postgres.py**

* Reads Kafka data
* Converts timestamps
* Loads processed data into PostgreSQL

### Database Layer

**sales_orders**

* Stores processed order records
* Supports reporting and analytics

---

## Analytics Implemented

### Category Revenue

Calculates total revenue by product category.

### Product Revenue

Calculates total revenue by product.

### Average Order Value

Calculates average order value across all orders.

### Top Customers

Identifies highest-spending customers.

### Daily Revenue

Calculates revenue generated per day.

---

## Sample Order Event

json
{
  "order_id": 1001,
  "customer_id": 501,
  "product": "Laptop",
  "category": "Electronics",
  "quantity": 2,
  "price": 60000,
  "timestamp": "2026-06-19 10:00:00"
}

---

## Challenges Faced

### Spark Python Version Mismatch

Issue:

* Spark driver and worker were using different Python versions.

Solution:

* Explicitly configured PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON.

### PostgreSQL Connection Issues

Issue:

* Spark was connecting to a different PostgreSQL instance.

Solution:

* Identified multiple PostgreSQL services running on the machine and connected to the correct instance.

### Timezone Handling

Issue:

* PostgreSQL rejected Asia/Calcutta timezone.

Solution:

* Configured Spark and JDBC connections to use UTC timezone.

### Timestamp Data Type Mismatch

Issue:

* PostgreSQL TIMESTAMP column received STRING data.

Solution:

* Converted order_timestamp using to_timestamp() before loading.

---

## Learning Outcomes

* Kafka Producer and Consumer Development
* Real-Time Data Streaming
* Spark DataFrame Transformations
* Kafka Integration with Spark
* PostgreSQL JDBC Integration
* Docker-based Development
* Git Branching and Merging
* Production-style Debugging and Troubleshooting

---

## Future Enhancements

* Spark Structured Streaming
* Dashboard using Power BI or Streamlit
* Airflow Orchestration
* AWS Deployment
* Data Quality Validation
* Monitoring and Alerting
