from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
from abc import ABC, abstractmethod
from kafka_configuration import kafka_config, producer_config, topic_read, topic_write


class KafkaConsumer(ABC):
    def __init__(self, config, topics):
        self.consumer = Consumer(config)
        if isinstance(topics, str):
            topics = [topics]
        self.consumer.subscribe(topics)

    def consume(self):
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        print(f'{msg.topic()} [{msg.partition()}] reached end at offset {msg.offset()}')
                    else:
                        print(f'Error: {msg.error()}')
                else:
                    self.process_message(msg)
                    print(
                        f'Received message: {msg.value().decode("utf-8")} from topic {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')
        except KafkaException as ke:
            print(f'Kafka Exception: {ke}')
        except KeyboardInterrupt:
            print('Interrupted by user')
        finally:
            self.finalize()
            self.consumer.close()

    @abstractmethod
    def process_message(self, msg):
        pass

    @abstractmethod
    def finalize(self):
        pass


class KafkaProducerConsumer(KafkaConsumer):
    def __init__(self, producer_config, consumer_config, consume_topic, produce_topic):
        super().__init__(consumer_config, consume_topic)
        self.producer = Producer(producer_config)
        self.topic = produce_topic

    def process_message(self, msg):
        try:
            message = msg.value().decode('utf-8')
            self.producer.produce(self.topic, message, callback=self.delivery_report)
            # Handle internal message queue to avoid buffer overflow
            self.producer.poll(0)
        except Exception as e:
            print(f'Error processing message: {e}')

    def finalize(self):
        print('Flushing producer')
        self.producer.flush()

    def delivery_report(self, err, msg):
        if err is not None:
            print(f'Message delivery failed: {err}')
        else:
            print(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')


if __name__ == '__main__':
    # Ensure these variables are defined properly
    producer = KafkaProducerConsumer(producer_config, kafka_config, topic_read, topic_write)
    producer.consume()
