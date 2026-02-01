🚀 ClickPulse – Real-Time Clickstream Analytics Pipeline

ClickPulse is an end-to-end real-time clickstream analytics project designed to simulate and process live user behavior data in an e-commerce environment. The project replays a real e-commerce clickstream dataset as streaming events using Apache Kafka and processes them in real time using Spark Structured Streaming (PySpark), following an industry-standard Bronze–Silver–Gold data lake architecture.

🧠 Project Overview  

Modern applications generate massive volumes of user interaction data every second. ClickPulse demonstrates how such data can be ingested, processed, and prepared for analytics in real time.  

This project focuses on:

Real-time event ingestion  
Stream processing  
Data lake design  
Analytics-ready data preparation  

🔄 Architecture & Workflow

E-commerce Dataset  
        ↓  
Kafka (Replay Producer – Real-Time Events)  
        ↓  
Spark Structured Streaming (PySpark)  
        ↓  
Bronze Layer (Raw Events)  
        ↓  
Silver Layer (Cleaned & Structured Data)  
        ↓  
Gold Layer (Aggregated Metrics)  
        ↓  
     Tableau   

🛠 Tech Stack

- Apache Kafka – Real-time event streaming
- Apache Spark Structured Streaming (PySpark) – Stream processing
- Spark SQL – Data transformations
- Python – Data handling and replay producer
- Parquet – Data lake storage format
- Tableau – Data visualization (optional)
- Local Setup – Cost-efficient, cloud-ready design

▶️ How to Run the Project (Local)  

1️⃣ Prerequisites  

Java 8 or 11  
Python 3.8+  
Apache Kafka  
Apache Spark  

Required Python packages:

pip install pandas kafka-python pyspark

2️⃣ Start Kafka  
zookeeper-server-start.sh config/zookeeper.properties  
kafka-server-start.sh config/server.properties  


Create topic:

kafka-topics.sh --create \
  --topic clickpulse_events \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

3️⃣ Start Spark Streaming  
spark-submit spark/streaming/clickpulse_streaming_job.py

4️⃣ Start Replay Producer  
python kafka/producer/clickstream_replay_producer.py  

Streaming data will start flowing into the Bronze layer.

5️⃣ Build Silver Layer  
spark-submit spark/batch/bronze_to_silver.py

6️⃣ Build Gold Layer  
spark-submit spark/batch/silver_to_gold.py

👨‍💻 Author  

Tejas Gurav  
Aspiring Data Engineer | Big Data | Streaming Systems
