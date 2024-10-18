import json
import time
# from confluent_kafka import Consumer, KafkaError, KafkaException
from kafka.kafka_consumer import KafkaConsumer


class DataStoreConsumer(KafkaConsumer):
    def __init__(self, internal_config, topic, mongo_client, mongo_db, mongo_collection, batch_size=1000):
        super().__init__(internal_config, topic)
        self.batch = []
        self.batch_size = batch_size
        self.collection = mongo_client[mongo_db][mongo_collection]
        self.last_insert_time = time.time()
        self.last_message_time = time.time()
        self.TIMEOUT = 5
        self.IDLE_TIMEOUT = 10

    def insert_batch(self):
        try:
            if self.batch:
                self.collection.insert_many(self.batch)
                self.batch = []
                print(f'Inserted {len(self.batch)} records into MongoDB.')
            self.batch.clear()
        except Exception as e:
            print(f'Error: {e}')
            self.batch.clear()

    def process_message(self, msg):
        try:
            self.batch.append(json.loads(msg.value().decode('utf-8')))
            current_time = time.time()

            # Insert batch if batch size is reached
            if len(self.batch) >= self.batch_size:
                self.insert_batch()
                self.last_insert_time = current_time

            # Insert batch if timeout is reached
            elif current_time - self.last_insert_time >= self.TIMEOUT:
                self.insert_batch()
                self.last_insert_time = current_time

            self.last_insert_time = current_time
        except Exception as e:
            print(f'Error: {e}')

    def idle_timeout(self):
        current_time = time.time()
        if current_time - self.last_message_time >= self.IDLE_TIMEOUT and self.batch:
            self.insert_batch()
            self.last_message_time = current_time
                
    def finalize(self):
        if self.batch:
            self.insert_batch()
