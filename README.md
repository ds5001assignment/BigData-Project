# Real-Time E-Commerce Anomaly Detection System 🚀

A distributed, fault-tolerant stream processing pipeline engineered to detect micro-outages and business-impacting anomalies in enterprise e-commerce environments. Built with **Apache Spark Structured Streaming** and **Apache Kafka**, this system processes live transaction data, evaluates statistical thresholds in real-time, and dispatches state-aware automated alerts.

## 🏗️ Architecture Overview

This project implements the "Speed Layer" of a Lambda Architecture, deployed across a multi-node Windows cluster managed by Hadoop YARN and networked via Tailscale VPN.

* **Ingestion:** Python-based probabilistic traffic generator streaming to an Apache Kafka broker.
* **Processing Engine:** Apache Spark Structured Streaming computing 30-second tumbling windows with 1-minute watermarking.
* **State Management:** In-memory tracking of active incidents to calculate Mean Time to Recovery (MTTR) and prevent Alert Fatigue.
* **Persistence:** Raw streaming data is simultaneously appended to HDFS for offline End-of-Day (EOD) batch reporting.
* **Alerting:** Automated, dynamic HTML email dispatching via SMTP for Critical/High severity anomalies and System Recoveries.

## ✨ Key Features

* **Real-Time Mathematical Aggregations:** Calculates dynamic failure rates, revenue at risk (USD), and latency standard deviations ($\sigma$) on the fly.
* **State-Aware Notifications:** Differentiates between new outages, ongoing degradations, and system recoveries. 
* **Distributed Parallelism:** Utilizes Round-Robin partitioning to distribute Kafka stream payloads across multiple YARN worker nodes, preventing Data Skew.
* **Exactly-Once Semantics:** Fault-tolerant checkpointing ensures reliable processing even during worker node failures.

## 🛠️ Technology Stack

* **Core:** Python 3.x, PySpark (3.x)
* **Streaming & Messaging:** Apache Kafka, Spark Structured Streaming
* **Cluster Management & Storage:** Hadoop YARN, HDFS
* **Networking:** Tailscale

## 🚀 Quick Start

### Prerequisites
* Hadoop Cluster (Master + Worker nodes) running YARN and HDFS.
* Apache Kafka broker active and accessible.
* `GMAIL_SENDER`, `GMAIL_RECEIVER`, and `GMAIL_APP_PASSWORD` configured in the driver node's environment variables.

### Submitting the Spark Job
Submit the pipeline to the YARN cluster using `client` mode. Ensure the `spark.driver.host` matches your Tailscale VPN IP to allow the ApplicationMaster to route data back successfully:

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 \
  --num-executors 4 \
  --executor-memory 2g \
  --conf "spark.driver.host=YOUR_TAILSCALE_IP" \
  --conf "spark.driver.port=7077" \
  anomaly-detector.py

```

### Running the Generator
Once the Spark job is actively listening to the topic, start the synthetic traffic generator to simulate e-commerce funnels and inject localized anomalies:

```bash
python ecommerce_traffic_generator.py
```
