#!/bin/bash

set -e
# Run DataFrame to Kafka script
python3 kafka/kafka_producer.py -i hdfs:///user/stream_data/output/sensors.parquet -e parquet -t office_input -rst 2
