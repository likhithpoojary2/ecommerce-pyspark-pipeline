# E-commerce Big Data Analytics Pipeline (PySpark + Databricks)

## Project Overview

This project implements an end-to-end ETL/ELT data pipeline using PySpark in Databricks to process large-scale e-commerce user behavior data and generate actionable business insights.

The system is designed using a multi-layer architecture (Bronze, Silver, Gold) to ensure scalability, data quality, and efficient transformation of large datasets.

---

## Tech Stack

* Python
* PySpark
* Databricks (Distributed Computing Environment)
* Spark SQL
* Streamlit (Dashboard)
* Pandas and Matplotlib

---

## Architecture

The pipeline follows a Bronze → Silver → Gold layered architecture:

### Bronze Layer (Raw Ingestion)

* Ingests raw CSV data from source
* Stores data in distributed storage
* No transformations applied

### Silver Layer (Data Cleaning and Validation)

* Removes null values
* Filters invalid records (e.g., negative price)
* Selects relevant events (purchase data)
* Applies schema normalization

### Gold Layer (Business Transformation)

* Performs aggregations using PySpark
* Generates business insights such as:

  * Top products by revenue
  * Top users by spending
* Optimized for analytics and reporting

---

## Key Features

* Built scalable PySpark ETL pipeline for large datasets
* Processed and transformed data in a distributed computing environment (Databricks)
* Ensured data quality through validation, cleansing, and normalization
* Used Spark SQL and DataFrame APIs for efficient transformations
* Generated business insights from raw data
* Developed an interactive dashboard using Streamlit

---

## Insights Generated

* Top selling products by revenue
* High-value customers (top users)
* Revenue distribution across products
* Customer purchasing behavior

---

## Dashboard

The project includes an interactive dashboard built using Streamlit to visualize analytics results.

Features:

* KPI metrics
* Product and user analysis
* Clean and interactive UI

---

## Screenshots

### Databricks Processing

<img width="1376" height="666" alt="image" src="https://github.com/user-attachments/assets/ea42f834-ea1e-40a8-8d37-b79fa796d57a" />

<img width="1360" height="727" alt="image" src="https://github.com/user-attachments/assets/d82838f3-151b-4ad7-a537-fbea660d3f0b" />

<img width="1393" height="756" alt="image" src="https://github.com/user-attachments/assets/eff290a2-adb1-409c-a992-f55abd2be3fa" />

<img width="1400" height="557" alt="image" src="https://github.com/user-attachments/assets/d26e112f-c840-4899-a9dc-4cd460e3c1b5" />

<img width="1411" height="581" alt="image" src="https://github.com/user-attachments/assets/6ade16b7-ebcf-4a67-a1bc-0f80e657fb98" />




### Streamlit Dashboard

<img width="1919" height="868" alt="image" src="https://github.com/user-attachments/assets/92128eb1-2622-4aad-9f0c-d4029bbce2b4" />


---

## How to Run This Project

### 1. Clone Repository

```bash
git clone https://github.com/<your-username>/ecommerce-pyspark-pipeline.git
cd ecommerce-pyspark-pipeline
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Run Dashboard

```bash
streamlit run dashboard/dashboard.py
```

---

## Execution Environment

Due to local environment limitations (Hadoop dependencies on Windows), the PySpark pipeline is executed in Databricks, which simulates a real-world distributed data processing environment.

---

## Scalability

* Designed to handle large-scale datasets (GB-level)
* Uses distributed processing via PySpark
* Easily extendable to cloud platforms such as AWS EMR, Azure Synapse, and HDInsight

---

## Future Improvements

* Real-time data processing using Kafka
* Automated job scheduling
* Integration with cloud storage (S3/ADLS)
* Machine learning-based recommendation system

---


## Author

Your Name
(https://github.com/likhithpoojary2/)
