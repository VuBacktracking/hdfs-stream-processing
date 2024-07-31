# Stream Data Processing using Hadoop Ecosystem

## Overview

* Fetch compressed data from a URL.
* Utilize PySpark for data processing, leveraging HDFS for storage and monitoring resources via Apache Hadoop YARN.
* Employ a data generator to simulate streaming data and transmit it to Apache Kafka.
* Implement PySpark (Spark Streaming) to consume and process streaming data from Kafka topics.
* Persist streaming data into Elasticsearch for storage and subsequent visualization using Kibana.
* Store streaming data into MinIO, a cloud-native object storage service.
* Utilize Apache Airflow for orchestrating the entire data pipeline workflow.

## System Architecture
<p align = "center">
    <img src="assets/architecture.png" alt="workflow">
</p>

## Prequisites
Before runing this script, ensure you have the following installed.\
**Note**:  The project was set up on Ubuntu 22.04 OS.

* Ubuntu 22.04 (prefered, but you can use Ubuntu 20.04)
* Apache Hadoop HDFS (installed locally)
* Apache Spark (installed locally)
* Apache Kafka (installed locally)
* Apache Airflow
* Docker
* Minio, Elastic Search, Kibana
