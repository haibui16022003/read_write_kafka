from kafka.kafka_producer import KafkaProducer
from kafka_configuration import kafka_config, producer_config, topic_read, topic_write

if __name__ == '__main__':
    producer = KafkaProducer(producer_config, kafka_config, topic_read, topic_write)
    producer.consume()
