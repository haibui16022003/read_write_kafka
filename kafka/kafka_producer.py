from confluent_kafka import Producer
from kafka.kafka_consumer import KafkaConsumer


class KafkaProducer(KafkaConsumer):
    def __init__(self, producer_config, consumer_config, consume_topic, produce_topic):
        super().__init__(consumer_config, consume_topic)
        self.topic = produce_topic
        self.producer = Producer(producer_config)

    def process_message(self, msg):
        try:
            message = msg.value().decode('utf-8')
            self.producer.produce(self.topic, message, callback=self.call_back)
        except Exception as e:
            print(f'Error: {e}')

    def finalize(self):
        self.producer.flush()

    def call_back(self, error, message):
        if error is not None:
            print(f'Error: {error}')
        else:
            print(f'Message delivered to {message.topic()} [{message.partition()}] at offset {message.offset()}')