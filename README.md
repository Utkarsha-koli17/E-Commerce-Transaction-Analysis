```markdown
# 🛒 E-Commerce Big Data Analytics Pipeline

## 📋 Project Overview

A production-grade, end-to-end big data analytics pipeline that processes e-commerce transaction data using modern distributed technologies. This system ingests raw CSV data through Hadoop HDFS, streams it via Apache Kafka, processes and cleans it using Apache Spark Structured Streaming, stores it in PostgreSQL, and visualizes insights through an interactive Flask-based dashboard.

**Course:** PG-DBDA (Post Graduate Diploma in Big Data Analytics) - Statistics & Data Insights Module  
**Institution:** C-DAC Kharghar, Navi Mumbai  
**Dataset:** 2 Million e-commerce transactions (~500MB CSV, 30 columns)  
**Status:** ✅ Successfully Processed 1,129,887 records  
**Date Range:** January 2022 – December 2024

---

## 🏗️ System Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        E-COMMERCE BIG DATA PIPELINE                          │
└─────────────────────────────────────────────────────────────────────────────┘

┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│   Raw CSV    │───▶│   Hadoop     │───▶│   Apache     │───▶│   Apache     │───▶│  PostgreSQL  │
│   (2M rows)  │    │    HDFS      │    │    Kafka     │    │    Spark     │    │   Database   │
│   500MB      │    │ /ecommerce/  │    │  3 Partitions│    │ Structured   │    │  metastore   │
└──────────────┘    │    raw/      │    └──────────────┘    │  Streaming   │    └──────────────┘
                    └──────────────┘                        └──────────────┘          │
                                                                                      ▼
                                                                             ┌──────────────┐
                                                                             │    Flask     │
                                                                             │    API +     │
                                                                             │   Dashboard  │
                                                                             │  localhost   │
                                                                             │   :5000      │
                                                                             └──────────────┘
                                                                                      │
                                                                                      ▼
                                                                             ┌──────────────┐
                                                                             │   Plotly.js  │
                                                                             │  Interactive │
                                                                             │  Charts      │
                                                                             └──────────────┘
```

### Architecture Layers

1. **Data Source Layer**: Raw CSV file with 2M e-commerce transactions
2. **Storage Layer**: Hadoop HDFS for distributed data storage
3. **Ingestion Layer**: Apache Kafka for real-time data streaming
4. **Processing Layer**: Apache Spark Structured Streaming for ETL
5. **Storage Layer**: PostgreSQL for analytics-ready data
6. **Presentation Layer**: Flask REST API + Plotly Dashboard
7. **Orchestration Layer**: Apache Airflow for workflow management

---

## 🔧 Technology Stack

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| **Operating System** | Pop!OS | Latest | Development environment |
| **Containerization** | Docker | Latest | Service isolation |
| **Distributed Storage** | Hadoop HDFS | 3.3.6 | Raw data storage |
| **Message Streaming** | Apache Kafka | 3.6.1 | Real-time data ingestion |
| **Data Processing** | Apache Spark | 3.1.1 | ETL and data cleaning |
| **Database** | PostgreSQL | 15 | Analytics data warehouse |
| **Orchestration** | Apache Airflow | 2.8.1 | Workflow scheduling |
| **Backend API** | Flask | 3.0.0 | REST API development |
| **Visualization** | Plotly.js | Latest | Interactive charts |
| **Runtime** | Java | 11 | Spark & Hadoop runtime |
| **Language** | Python | 3.11 | Application development |

---

## 📁 Project Structure

```
~/bigdata-env/
├── docker-compose.yml              # Docker orchestration configuration
├── run_full_pipeline.sh            # One-click pipeline runner
├── stop-ecommerce.sh               # Stop all services
└── project/
    ├── README.md                   # This documentation
    ├── SETUP.md                    # Installation guide
    ├── ARCHITECTURE.md             # Detailed architecture
    ├── API_DOCUMENTATION.md        # API reference
    ├── PROJECT_SUMMARY.md          # Executive summary
    ├── requirements.txt            # Python dependencies
    │
    ├── data/
    │   └── ecommerce.csv           # Raw data (500MB, 2M records)
    │
    ├── scripts/
    │   ├── kafka_producer.py       # Reads CSV and sends to Kafka
    │   └── spark_etl.py            # Spark ETL with 30-column cleaning
    │
    ├── flask_app/
    │   ├── app.py                  # Flask REST API (18 endpoints)
    │   └── templates/
    │       ├── base.html           # Base template with navigation
    │       ├── overview.html       # Page 1: KPIs and summary
    │       ├── categories.html     # Page 2: Category analysis
    │       ├── geography.html      # Page 3: Geographic insights
    │       ├── payments.html       # Page 4: Payment patterns
    │       ├── trends.html         # Page 5: Time series analysis
    │       └── customers.html      # Page 6: Customer segments
    │
    └── dags/
        └── ecommerce_spark_etl_dag.py  # Airflow DAG for scheduling
```

---

## 🔄 Data Flow

### Complete Pipeline Flow

```
1. CSV Upload to HDFS
   ↓
   Command: hdfs dfs -put ecommerce.csv /ecommerce/raw/
   
2. Kafka Producer reads from HDFS
   ↓
   Script: kafka_producer.py
   Batch Size: 1,000 records
   Topic: ecommerce-transactions (3 partitions)
   
3. Spark Streaming reads from Kafka
   ↓
   Mode: Structured Streaming
   Checkpoint: /tmp/spark-checkpoint-ecommerce
   
4. Data Cleaning & Transformation (30 columns)
   ↓
   - Date parsing (multiple formats)
   - Price validation (0-500K range)
   - Discount fixes (0-100%)
   - Rating clipping (1-5 range)
   - Return logic validation
   - State standardization
   
5. Write to PostgreSQL
   ↓
   Table: ecommerce_cleaned
   Mode: Append
   
6. Flask API queries PostgreSQL
   ↓
   18 REST endpoints
   
7. Dashboard displays visualizations
   ↓
   6 interactive pages with Plotly
```

---

##  Key Features

### Data Processing
- ✅ **30+ Data Quality Rules**: Comprehensive cleaning for all columns
- ✅ **Multi-format Date Parsing**: Handles YYYY-MM-DD, DD/MM/YYYY, DD-MM-YYYY
- ✅ **Outlier Detection**: Removes extreme values in prices, ratings, quantities
- ✅ **Derived Columns**: Calculates revenue, delivery days, on-time delivery flag
- ✅ **Duplicate Removal**: Ensures unique transactions
- ✅ **NULL Handling**: Proper handling of missing values

### Analytics Dashboard
- ✅ **6 Interactive Pages**: Overview, Categories, Geography, Payments, Trends, Customers
- ✅ **Real-time Visualizations**: Hover, zoom, pan capabilities
- ✅ **Responsive Design**: Works on all screen sizes
- ✅ **18 REST API Endpoints**: Complete API coverage
- ✅ **Business KPIs**: Revenue, orders, return rate, delivery performance

### Infrastructure
- ✅ **Docker Containerization**: All services isolated and portable
- ✅ **Scalable Architecture**: Can scale horizontally
- ✅ **Airflow Orchestration**: Automated workflow scheduling
- ✅ **Checkpoint Recovery**: Spark streaming fault tolerance

---

##  Installation & Setup

### Prerequisites

- **Operating System**: Linux (Pop!OS/Ubuntu recommended) or macOS
- **Docker**: Version 20.10 or higher
- **Docker Compose**: Version 2.0 or higher
- **Python**: Version 3.8 or higher
- **RAM**: Minimum 8GB (16GB recommended)
- **Disk Space**: Minimum 20GB free space

### Quick Start

```bash
# 1. Clone the repository
git clone https://github.com/piyush9786/ecommerce-bigdata-pipeline.git
cd ecommerce-bigdata-pipeline

# 2. Start all Docker services
cd ~/bigdata-env
docker compose up -d

# 3. Wait for services to initialize (30 seconds)
sleep 30

# 4. Run the complete pipeline
cd ~/bigdata-env/project
./run_full_pipeline.sh

# 5. Start Flask dashboard
source venv/bin/activate
python flask_app/app.py
```

### Access Points

| Service | URL | Credentials |
|---------|-----|-------------|
| **Flask Dashboard** | http://localhost:5000 | No authentication |
| **Spark Master UI** | http://localhost:8080 | No authentication |
| **Spark Worker UI** | http://localhost:8081 | No authentication |
| **Airflow UI** | http://localhost:8088 | admin/admin |
| **PostgreSQL** | localhost:5433 | bigdata/bigdata123 |

---

## 📊 Dashboard Features

### Page 1: Overview Dashboard
- **KPIs**: Total Orders, Revenue, Customers, AOV, Return Rate, On-Time Delivery
- **Charts**: Revenue by Category, Monthly Revenue Trend

### Page 2: Categories Analysis
- Category vs Sub-Category revenue breakdown
- Price distribution by category
- Return reasons analysis
- Performance metrics table

### Page 3: Geography Insights
- State-wise revenue (Top 10 states)
- Delivery performance by state
- Return rates by region
- Interstate commerce flow (Top 20 routes)

### Page 4: Payment Patterns
- Payment mode distribution (UPI, Card, COD, Net Banking, Wallet)
- Return rates by payment method
- Payment preferences by state
- Payment patterns by customer segment

### Page 5: Trend Analysis
- Year-over-year comparison (2022-2024)
- Day of week analysis
- Category trends over time
- Monthly revenue patterns

### Page 6: Customer Segments
- Segment-wise performance (Premium, Basic, etc.)
- Device usage patterns (Mobile, Desktop, Tablet)
- Rating distribution
- Loyalty member impact analysis

---

## 🔌 API Documentation

### Overview APIs

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/summary` | GET | Overall business metrics and KPIs |
| `/api/category_revenue` | GET | Revenue breakdown by product category |
| `/api/monthly_revenue` | GET | Monthly revenue trend over time |

### Category APIs

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/category_subcategory` | GET | Category vs sub-category breakdown |
| `/api/category_price_distribution` | GET | Price statistics by category |
| `/api/return_reasons` | GET | Return reasons by category |

### Geography APIs

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/state_performance` | GET | Performance metrics by buyer state |
| `/api/interstate_flow` | GET | Top interstate commerce routes |

### Payment APIs

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/payment_distribution` | GET | Payment mode statistics |
| `/api/payment_by_state` | GET | Payment preferences by state |
| `/api/payment_by_segment` | GET | Payment patterns by segment |

### Trend APIs

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/yearly_comparison` | GET | Year-over-year performance |
| `/api/day_of_week` | GET | Order distribution by day |
| `/api/category_trends` | GET | Category trends over time |

### Customer APIs

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/segment_analysis` | GET | Customer segment performance |
| `/api/device_usage` | GET | Device type statistics |
| `/api/rating_distribution` | GET | Customer rating distribution |
| `/api/loyalty_analysis` | GET | Loyalty member impact |

---

## 📈 Performance Metrics

### Data Processing Performance
- **Total Records Processed**: 1,129,887
- **Processing Time**: ~40 minutes (105 batches)
- **Records per Batch**: ~9,999
- **Success Rate**: 100% (for processed batches)
- **Throughput**: ~28,000 records/minute

### Business Metrics
- **Total Revenue**: ₹583.45 Crore
- **Average Order Value**: ₹5,163.83
- **Unique Customers**: 441,462
- **Return Rate**: 15.01%
- **On-Time Delivery**: 30.22%

### System Performance
- **API Response Time**: < 100ms (average)
- **Database Size**: ~500MB (1.1M records)
- **Memory Usage**: 2GB (Spark executor)
- **CPU Usage**: 4 cores

---

## 🧹 Data Cleaning Rules

### Transaction Data
1. **Transaction ID**: Remove NULLs and duplicates
2. **User ID**: Cast to integer, remove negatives
3. **Product ID**: Validate format and presence

### Product Information
4. **Product Category**: Standardize to title case
5. **Sub-Category**: Standardize to title case
6. **Product Price**: Remove NULLs, validate range (0-₹500K), remove outliers

### Pricing & Discounts
7. **Discount Percent**: Fix negatives → 0%, cap >100% → 100%, handle NULLs → 0%
8. **Final Price**: Recalculate if inconsistent, fix negatives, handle NULLs

### Order Details
9. **Quantity**: Remove NULLs, ensure positive integers, cap at 100
10. **Transaction Date**: Parse multiple formats, remove invalid dates
11. **Payment Mode**: Standardize to uppercase

### Delivery Information
12. **Delivery Date**: Parse multiple formats, handle NULLs
13. **Delivery Days**: Calculate from dates, remove negatives

### Return Management
14. **Return Flag**: Standardize to 0/1
15. **Return Reason**: Clear if return_flag = 0, standardize
16. **Return Date**: Parse only for returned items

### Ratings
17. **Seller Rating**: Clip to 1-5 range
18. **Customer Rating**: Clip to 1-5 range

### Geographic Data
19. **Buyer State**: Standardize to uppercase
20. **Seller State**: Standardize to uppercase

### Customer Information
21. **Customer Segment**: Standardize categories
22. **Loyalty Member**: Standardize Yes/No

### Financial Calculations
23. **GST Amount**: Recalculate if negative (18% of final price × quantity)
24. **Shipping Cost**: Fix negatives → 0, cap outliers >₹500 → ₹200
25. **Refund Amount**: Fix for non-returned items

### Session & Device Data
26. **Device Type**: Standardize (Mobile, Desktop, Tablet)
27. **Browser Type**: Standardize
28. **Session Duration**: Remove extreme outliers

### Analytics Columns (Added)
29. **Transaction Year/Month/Day**: Extracted from date
30. **Total Revenue**: Calculated as final_price × quantity
31. **Is On-Time Delivery**: Boolean (delivery_days ≤ 7)

---

## 🛠️ Troubleshooting

### Common Issues

**Issue: Docker containers not starting**
```bash
# Check Docker logs
docker compose logs

# Restart containers
docker compose down
docker compose up -d
```

**Issue: Port already in use**
```bash
# Check which process is using the port
sudo netstat -tulpn | grep :5000

# Kill the process
sudo kill -9 <PID>
```

**Issue: Permission denied**
```bash
# Fix permissions
sudo chown -R $USER:$USER ~/bigdata-env
chmod -R 755 ~/bigdata-env
```

**Issue: Out of memory**
```bash
# Increase Docker memory limit
# Edit /etc/docker/daemon.json
{
  "default-shm-size": "2g"
}

# Restart Docker
sudo systemctl restart docker
```

**Issue: Kafka connection errors**
```bash
# Check Kafka is running
docker ps | grep kafka

# Check Kafka logs
docker logs kafka

# Restart Kafka
docker restart kafka
```

**Issue: Spark job failures**
```bash
# Check Spark worker logs
docker logs spark_worker

# Increase executor memory in spark-submit
--executor-memory 4G --conf spark.executor.heartbeatInterval=300s
```

---

## 🔮 Future Enhancements

### Short-term (1-2 months)
- [ ] Fix Kafka connection issues for complete data processing
- [ ] Process remaining 870K records
- [ ] Add database indexes for performance
- [ ] Implement error handling and retries
- [ ] Add unit tests

### Medium-term (3-6 months)
- [ ] Real-time alerting system
- [ ] Predictive analytics (ML models)
- [ ] Customer churn prediction
- [ ] Demand forecasting
- [ ] A/B testing framework

### Long-term (6-12 months)
- [ ] Horizontal scaling (Kafka cluster)
- [ ] Multi-region deployment
- [ ] Advanced ML models
- [ ] Mobile app integration
- [ ] Real-time recommendations

---

## 📝 License

This project is created as part of PG-DBDA course at C-DAC Kharghar, Navi Mumbai.

**Educational Use Only** - For academic and learning purposes.

---

## 👥 Author

**Piyush Shinde**  
- **Email**: piyushinde3082@gmail.com
- **Course**: PG-DBDA (Post Graduate Diploma in Big Data Analytics)
- **Institution**: C-DAC Kharghar, Navi Mumbai
- **GitHub**: https://github.com/piyush9786

---

## 🙏 Acknowledgments

- Apache Software Foundation for open-source big data tools
- C-DAC faculty for guidance and support
- PostgreSQL, Kafka, and Spark communities

---

## 📞 Support

For issues and questions:
- **GitHub Issues**: https://github.com/piyush9786/ecommerce-bigdata-pipeline/issues
- **Email**: piyushinde3082@gmail.com

---

**Last Updated**: June 2026  
**Version**: 1.0.0  
**Status**: ✅ Production Ready (Partial Data)
```

---

## 📋 Additional Files to Create

Create these additional files in your repository for complete documentation:

### 1. **SETUP.md** - Installation Guide
```markdown
#  Setup Guide

## Prerequisites
- Docker 20.10+
- Docker Compose 2.0+
- Python 3.8+
- 8GB+ RAM
- 20GB+ disk space

## Installation Steps
[Detailed steps here]

## Verification
[Verification commands here]
```

### 2. **ARCHITECTURE.md** - Detailed Architecture
```markdown
# ️ System Architecture

## High-Level Architecture
[Architecture diagram and details]

## Component Details
[Detailed component descriptions]

## Data Flow
[Complete data flow documentation]
```

### 3. **API_DOCUMENTATION.md** - Complete API Reference
```markdown
# 📡 API Documentation

## Base URL
http://localhost:5000

## Endpoints
[Complete API endpoint documentation with examples]
```

### 4. **PROJECT_SUMMARY.md** - Executive Summary
```markdown
# 📊 Project Summary

## Key Achievements
- ✅ 1,129,887 records processed
- ✅ 6-page interactive dashboard
- ✅ 18 REST API endpoints
- ✅ Complete Docker orchestration

## Technology Stack
[Technology table]

## Business Metrics
[Key business metrics]
```

---

## 🎯 Quick Commands Reference

```bash
# Start all services
cd ~/bigdata-env && docker compose up -d

# Run complete pipeline
cd ~/bigdata-env/project && ./run_full_pipeline.sh

# Start Flask dashboard
cd ~/bigdata-env/project && source venv/bin/activate && python flask_app/app.py

# Stop all services
cd ~/bigdata-env && ./stop-ecommerce.sh

# Check data count
docker exec -it postgres_db psql -U bigdata -d metastore -c "SELECT COUNT(*) FROM ecommerce_cleaned;"

# View logs
docker compose logs -f

# Restart specific service
docker restart kafka
```

---
