# 🚀 Installation and Setup Guide

This guide will help you set up and run the E-Commerce Big Data Analytics Pipeline on your local machine.

## 📋 Table of Contents

1. [Prerequisites](#prerequisites)
2. [System Requirements](#system-requirements)
3. [Installation Steps](#installation-steps)
4. [Configuration](#configuration)
5. [Running the Pipeline](#running-the-pipeline)
6. [Accessing Services](#accessing-services)
7. [Troubleshooting](#troubleshooting)
8. [Stopping Services](#stopping-services)

---

## Prerequisites

Before you begin, ensure you have the following installed:

- Docker (20.10+)
- Docker Compose (2.0+)
- Git
- Python 3.8+
- Java 11+

### Verify Installation

```bash
docker --version
docker compose version
python3 --version
java --version
git --version
```

---

## System Requirements

### Minimum Requirements

| Component | Requirement |
|------------|-------------|
| RAM | 8 GB |
| CPU | 4 Cores |
| Storage | 20 GB Free |
| Network | Internet Connection |

### Recommended Requirements

| Component | Requirement |
|------------|-------------|
| RAM | 16 GB+ |
| CPU | 8 Cores+ |
| Storage | 50 GB SSD |
| Network | High-Speed Internet |

---

# Installation Steps

## Step 1: Clone Repository

```bash
git clone https://github.com/piyush9786/ecommerce-bigdata-pipeline.git

cd ecommerce-bigdata-pipeline
```

---

## Step 2: Create Required Directories

```bash
mkdir -p data
mkdir -p logs
```

---

## Step 3: Place Dataset

Copy your e-commerce CSV dataset into the data directory.

```bash
cp ~/Downloads/ecommerce-transactions-raw.csv data/
```

Project Structure:

```text
ecommerce-bigdata-pipeline/
│
├── data/
│   └── ecommerce-transactions-raw.csv
│
├── scripts/
├── spark/
├── airflow/
├── flask_app/
├── docker-compose.yml
└── README.md
```

---

## Step 4: Configure Environment Variables

Create `.env` file.

```env
AIRFLOW_UID=50000
AIRFLOW_GID=0

POSTGRES_USER=bigdata
POSTGRES_PASSWORD=bigdata123
POSTGRES_DB=metastore

KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
```

---

## Step 5: Start Docker Services

Pull and build containers:

```bash
docker compose pull

docker compose build

docker compose up -d
```

Check status:

```bash
docker compose ps
```

Expected Services:

```text
hadoop_namenode
hadoop_datanode
zookeeper
kafka
spark_master
spark_worker
postgres_db
airflow_webserver
airflow_scheduler
```

---

# Configuration

## Step 6: Verify Services

### Check PostgreSQL

```bash
docker compose exec postgres_db \
psql -U bigdata -d metastore -c "SELECT 1;"
```

### Check Kafka

```bash
docker compose exec kafka \
kafka-topics --bootstrap-server localhost:9092 --list
```

### Check Spark UI

Open:

```text
http://localhost:8080
```

### Check Airflow UI

Open:

```text
http://localhost:8088
```

---

## Step 7: Upload Data to HDFS

Copy file into Hadoop:

```bash
docker compose cp \
data/ecommerce-transactions-raw.csv \
hadoop_namenode:/tmp/ecommerce.csv
```

Create HDFS directory:

```bash
docker compose exec hadoop_namenode \
hdfs dfs -mkdir -p /ecommerce/raw
```

Upload file:

```bash
docker compose exec hadoop_namenode \
hdfs dfs -put -f \
/tmp/ecommerce.csv \
/ecommerce/raw/
```

Verify:

```bash
docker compose exec hadoop_namenode \
hdfs dfs -ls /ecommerce/raw/
```

---

# Running the Pipeline

## Step 8: Run Kafka Producer

Install dependencies:

```bash
pip install kafka-python pandas
```

Run producer:

```bash
python scripts/kafka_producer.py
```

Producer Functions:

- Reads data from CSV/HDFS
- Converts rows to JSON
- Sends messages to Kafka
- Streams data in batches

---

## Step 9: Run Spark ETL Job

Submit Spark Job:

```bash
docker compose exec spark_master \
spark-submit \
--master spark://spark-master:7077 \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,org.postgresql:postgresql:42.2.18 \
--total-executor-cores 4 \
--executor-memory 2G \
--driver-memory 1G \
/opt/spark_etl.py
```

Spark ETL Functions:

- Read Streaming Data from Kafka
- Data Cleaning
- Feature Engineering
- Aggregations
- PostgreSQL Storage

---

## Step 10: Start Flask Dashboard

Install dependencies:

```bash
pip install flask flask-cors psycopg2-binary plotly
```

Run dashboard:

```bash
python flask_app/app.py
```

Dashboard URL:

```text
http://localhost:5000
```

---

# Accessing Services

| Service | URL |
|----------|------|
| Flask Dashboard | http://localhost:5000 |
| Spark Master UI | http://localhost:8080 |
| Spark Worker UI | http://localhost:8081 |
| Airflow UI | http://localhost:8088 |
| Hadoop NameNode | http://localhost:9870 |
| PostgreSQL | localhost:5433 |
| Kafka | localhost:9092 |

---

# Dashboard Modules

## 1. Overview

Displays:

- Total Revenue
- Total Orders
- Average Order Value
- Revenue Trends

---

## 2. Categories

Displays:

- Category Performance
- Subcategory Analysis
- Return Analysis

---

## 3. Geography

Displays:

- State-wise Revenue
- Customer Distribution
- Seller Distribution

---

## 4. Payments

Displays:

- Payment Modes
- UPI Usage
- Regional Preferences

---

## 5. Trends

Displays:

- Monthly Trends
- Seasonal Trends
- Growth Analysis

---

## 6. Customers

Displays:

- Customer Segmentation
- Loyalty Analysis
- Device Usage

---

# Troubleshooting

## Docker Containers Not Starting

Check logs:

```bash
docker compose logs
```

Restart services:

```bash
docker compose down

docker compose up -d
```

---

## Port Already In Use

Find process:

```bash
sudo netstat -tulpn | grep :5000
```

Kill process:

```bash
sudo kill -9 <PID>
```

---

## Kafka Connection Issues

Check Kafka:

```bash
docker compose ps kafka
```

Restart Kafka:

```bash
docker compose restart kafka
```

View logs:

```bash
docker compose logs kafka
```

---

## PostgreSQL Connection Issues

Check status:

```bash
docker compose ps postgres_db
```

Restart:

```bash
docker compose restart postgres_db
```

---

## Spark Job Failure

View Spark Logs:

```bash
docker compose logs spark_master

docker compose logs spark_worker
```

Increase Resources:

```bash
--total-executor-cores 8

--executor-memory 4G
```

---

## Airflow DAG Not Visible

Restart Airflow:

```bash
docker compose restart airflow_webserver

docker compose restart airflow_scheduler
```

Check logs:

```bash
docker compose logs airflow_webserver

docker compose logs airflow_scheduler
```

---

# Monitoring

## Spark

```text
http://localhost:8080
```

Monitor:

- Running Jobs
- Executors
- Memory Usage
- Task Performance

---

## Hadoop

```text
http://localhost:9870
```

Monitor:

- HDFS Usage
- Data Nodes
- File Storage

---

## Airflow

```text
http://localhost:8088
```

Monitor:

- DAG Runs
- Task Success
- Scheduling

---

# Stopping Services

Stop all containers:

```bash
docker compose down
```

Verify:

```bash
docker compose ps
```

---

## Remove Containers and Volumes

```bash
docker compose down -v
```

---

## Remove Everything

```bash
docker compose down -v --rmi all --remove-orphans
```

---

# Quick Start

Run everything:

```bash
docker compose up -d

sleep 120

python flask_app/app.py
```

Open Dashboard:

```text
http://localhost:5000
```

---

# Technology Stack

- Apache Kafka
- Apache Spark
- Hadoop HDFS
- Apache Airflow
- PostgreSQL
- Flask
- Docker
- Python

---

# Support

For issues and feature requests:

- Create a GitHub Issue
- Check project documentation
- Review logs for debugging

---

## 🎉 Project Ready

After setup you will have:

✅ Hadoop Data Lake  
✅ Kafka Streaming Pipeline  
✅ Spark ETL Processing  
✅ PostgreSQL Data Warehouse  
✅ Airflow Orchestration  
✅ Flask Analytics Dashboard  
✅ Dockerized Infrastructure

Happy Data Engineering! 🚀
