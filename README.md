# 🚀 Big Data End-to-End E-Commerce Analytics Pipeline

![Python](https://img.shields.io/badge/Python-3.10-blue)
![Kafka](https://img.shields.io/badge/Apache-Kafka-orange)
![Spark](https://img.shields.io/badge/Apache-Spark-red)
![Airflow](https://img.shields.io/badge/Apache-Airflow-green)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Database-blue)
![Docker](https://img.shields.io/badge/Docker-Containerized-blue)

End-to-End Real-Time E-Commerce Analytics Pipeline using Kafka, Spark, Airflow, PostgreSQL, Docker, and Flask.
# 🚀 Big Data End-to-End E-Commerce Analytics Pipeline

## 📌 Project Overview

This project implements a complete end-to-end Big Data Analytics Pipeline for processing and analyzing e-commerce transaction data. The system combines real-time streaming, ETL processing, workflow orchestration, data warehousing, and dashboard visualization using modern data engineering technologies.

The pipeline ingests raw transaction data, processes and cleans it in real time, stores it in a structured database, and presents business insights through an interactive dashboard.

---

## 🎯 Objectives

* Build a real-time data ingestion system using Apache Kafka.
* Process and clean streaming data using Apache Spark Structured Streaming.
* Automate workflows using Apache Airflow.
* Store processed data in PostgreSQL.
* Develop a Flask-based analytics dashboard.
* Containerize the entire ecosystem using Docker.
* Enable one-click deployment and execution.

---

## 🏗️ System Architecture

![AR](AR.jpeg)

---

## 🛠️ Technology Stack

| Component              | Technology                        |
| ---------------------- | --------------------------------- |
| Data Ingestion         | Apache Kafka                      |
| Stream Processing      | Apache Spark Structured Streaming |
| Workflow Orchestration | Apache Airflow                    |
| Database               | PostgreSQL                        |
| Backend API            | Flask                             |
| Containerization       | Docker & Docker Compose           |
| Programming Language   | Python                            |
| Visualization          | HTML, CSS, JavaScript             |

---

## 📂 Project Structure

```text
E-Commerce-Analytics-Pipeline/
│
├── data/
│   └── ecommerce_transactions.csv
│
├── kafka/
│   └── kafka_producer.py
│
├── spark/
│   └── spark_etl.py
│
├── airflow/
│   └── ecommerce_spark_etl_pipeline.py
│
├── dashboard/
│   ├── app.py
│   ├── templates/
│   └── static/
│
├── database/
│   └── schema.sql
│
├── docker-compose.yml
├── run_full_pipeline.sh
├── requirements.txt
└── README.md
```

---

## ⚙️ Key Features

* Real-time data streaming with Kafka
* Spark Structured Streaming ETL
* Data cleaning and feature engineering
* Automated workflow orchestration using Airflow
* PostgreSQL analytical storage
* REST API development using Flask
* Interactive analytics dashboard
* Dockerized deployment
* End-to-end automation

---

## 🔄 Data Processing Operations

### Data Cleaning

* Remove null transaction IDs
* Remove duplicate records
* Handle missing values
* Standardize categorical fields
* Parse multiple date formats
* Validate numerical fields

### Feature Engineering

* Transaction Year
* Transaction Month
* Day of Week
* Total Revenue
* On-Time Delivery Flag

---

## 📊 Dashboard Analytics

The dashboard provides insights into:

* Overall Business KPIs
* Revenue by Category
* Monthly Revenue Trends
* State-wise Performance
* Payment Mode Distribution
* Customer Segmentation
* Device Usage Analytics
* Loyalty Program Analysis

---

## 🔗 API Endpoints

| Endpoint                  | Description               |
| ------------------------- | ------------------------- |
| /api/summary              | Overall business KPIs     |
| /api/category_revenue     | Revenue by category       |
| /api/monthly_revenue      | Monthly revenue trends    |
| /api/state_performance    | State-wise analytics      |
| /api/payment_distribution | Payment mode insights     |
| /api/segment_analysis     | Customer segmentation     |
| /api/device_usage         | Device analytics          |
| /api/loyalty_analysis     | Loyalty customer behavior |

---

## 🚀 Deployment

### Start Complete Pipeline

```bash
chmod +x run_full_pipeline.sh
./run_full_pipeline.sh
```

### Docker Deployment

```bash
docker-compose up -d
```

---

## 📈 Workflow

1. Load CSV Dataset
2. Stream Data to Kafka
3. Process Data using Spark ETL
4. Store Cleaned Data in PostgreSQL
5. Orchestrate Tasks using Airflow
6. Serve APIs using Flask
7. Visualize Analytics through Dashboard

---

## 🔥 Challenges Faced

* Kafka Consumer Lag
* Schema Consistency Management
* Handling Missing Data

---

## 📌 Future Enhancements

* AWS S3 / Azure Data Lake Integration
* Real-Time Dashboard Updates using WebSockets
* Machine Learning Recommendation Engine
* CI/CD Pipeline Automation
* Kubernetes-Based Scaling

---

## 👨‍💻 Team

Group 12 – Big Data Analytics Project

### Contributors

* Piyush Shinde
* Payal Waghela
* Abhishek Mohile
* Utkarsha Koli
* Lokesh Bhoge
* Simran Gulakari

---

## 📜 License

This project is developed for educational and academic purposes.
