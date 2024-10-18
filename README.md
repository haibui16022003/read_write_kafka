# Kafka Producer-Consumer Project

This project demonstrates a Kafka producer-consumer setup using Python and the `confluent_kafka` library. It includes separate classes for producing and consuming messages, as well as a combined producer-consumer class.

## Installation
1. Clone the repository:
2. Install the required packages:
```bash
pip install -r requirements.txt
```
3. Set up your Kafka and MongoDB configuration in `configuration/kafka_config.ini` and `configuration/mongo_config.ini` respectively.
4. Usage
To use send messages to Kafka topic, run the producer script:
```bash
python get_data.py
```
To consume messages from Kafka topic and locate it into MongoDB, run the consumer script:
```bash
python save_data.py
```
