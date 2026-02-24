🚀 ClickPulse – Real-Time Clickstream Analytics Pipeline

ClickPulse is an end-to-end real-time clickstream analytics project designed to simulate and process live user behavior data in an e-commerce environment. The project replays a real e-commerce clickstream dataset as streaming events using Apache Kafka and processes them in real time using Spark Structured Streaming (PySpark), implementing a cloud-based Bronze–Silver–Gold Medallion Architecture on Amazon S3.

🧠 Project Overview  

Modern applications generate massive volumes of user interaction data every second. ClickPulse demonstrates how such data can be ingested, processed, and prepared for analytics in real time.  

This project focuses on:

Real-time event ingestion using Kafka
Distributed stream processing with Spark Structured Streaming
Cloud data lake implementation using Amazon S3
Cloud data lake implementation using Amazon S3
Analytics-ready data preparation  

🔄 Architecture & Workflow

E-commerce Dataset  
        ↓  
Kafka (Producer – Real-Time Events)  
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
- Amazon S3 – Cloud data lake storage
- Python – Data handling and replay producer
- Parquet – Data lake storage format
- Tableau – Data visualization (optional)
- Local Setup – Cost-efficient, cloud-ready design

👨‍💻 Author  

Tejas Gurav  
Aspiring Data Engineer | Big Data 
