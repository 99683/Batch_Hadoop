# Stroke Prediction Big Data Pipeline

A scalable architecture for predicting stroke risks using **Hadoop**, **Spark**, **Kafka**, and **Streamlit**. Combines batch processing and real-time streaming with machine learning.

## 📋 Prerequisites

- Docker (for containerized environment)
- Java 8+ (for Hadoop and Spark)
- Python 3.10+ (with dependencies: `pyspark`, `pandas`, `streamlit`, `psycopg2`, `plotly`)
- Hadoop 3.x, Spark 3.5.1, Kafka 3.5.1, PostgreSQL 14+

## 🚀 Features

- **Batch Processing**:  
  - Data cleaning, encoding, and storage in HDFS/Parquet  
  - Random Forest model training (92% accuracy)  
- **Real-Time Streaming**:  
  - Kafka producers/consumers for live data ingestion  
  - Spark Streaming for instant predictions  
- **Visualization**:  
  - Interactive Streamlit dashboard  
  - PostgreSQL integration for predictions storage  

## 🛠️ Tech Stack

| Component          | Purpose                              |
|--------------------|--------------------------------------|
| **Hadoop (HDFS)**  | Distributed data storage             |
| **Spark**          | Batch/stream processing & ML         |
| **Kafka**          | Real-time data streaming             |
| **PostgreSQL**     | Predictions database                 |
| **Streamlit**      | Dashboard for monitoring             |

## ⚙️ Installation

1. **Clone the repo**:
   ```bash
   git clone https://github.com/99683/Batch_Hadoop.git

   # Hadoop
$HADOOP_HOME/sbin/hadoop-daemon.sh start datanode
hdfs dfsadmin -safemode leave

# Kafka & Zookeeper
./start-kafka-zookeeper.sh
/usr/local/kafka/bin/kafka-topics.sh --create --topic pandemic-stream

# PostgreSQL
service postgresql start

hdfs dfs -put stroke.csv /data/pandemic/
# 1. Preprocess data
spark-submit Process.py

# 2. Train model
spark-submit Rf_model.py

# 3. Evaluate model
spark-submit Rf_evaluating.py

#4Start Real-Time Prediction
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 --jars /root/batch/postgresql-42.7.3.jar kafka_predict_storeall.py

# Start Kafka producer (in another terminal)
python kafka_producer_real2.py

# Preprocess data flow
spark-submit --master yarn --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1  --jars /usr/local/spark/jars/postgresql-42.2.18.jar /root/batch/kafka_preprocess_storeall.py

# Prediction 
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 --jars /root/batch/postgresql-42.7.3.jar kafka_predict_storeall.py

#visualizing
streamlit run /root/batch/streamlit_app3.py --server.address 0.0.0.0 --server.port 16010 --server.enableCORS=false  
access :
http://localhost:16010


Batch_Hadoop/
├── process.py              # Data preprocessing script
├── Rf_model.py             # Model training script
├── Rf_evaluating.py        # Model evaluation script
├── kafka_producer_real.py  # Kafka producer for real-time data
├── kafka_predict_storeall.py # Spark Streaming prediction script
├── streamlit_app3.py       # Streamlit dashboard
├── stroke.csv             # Sample dataset
└── README.md              # This file


