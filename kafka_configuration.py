import configparser

config = configparser.ConfigParser()
config.read('configuration/kafka_config.ini')

kafka_config = {
    'bootstrap.servers': config.get('kafka', 'bootstrap.servers'),
    'group.id': config.get('kafka', 'group.id'),
    'auto.offset.reset': config.get('kafka', 'auto.offset.reset'),
    'security.protocol': config.get('kafka', 'security.protocol'),
    'sasl.mechanism': config.get('kafka', 'sasl.mechanism'),
    'sasl.username': config.get('kafka', 'sasl.username'),
    'sasl.password': config.get('kafka', 'sasl.password'),
}

producer_config = {
    'bootstrap.servers': config.get('kafka-producer', 'bootstrap.servers'),
    'security.protocol': config.get('kafka-producer', 'security.protocol'),
    'sasl.mechanism': config.get('kafka-producer', 'sasl.mechanism'),
    'sasl.username': config.get('kafka-producer', 'sasl.username'),
    'sasl.password': config.get('kafka-producer', 'sasl.password'),
}

local_consumer_config = {
    'bootstrap.servers': config.get('kafka-local-consumer', 'bootstrap.servers'),
    'group.id': config.get('kafka-local-consumer', 'group.id'),
    'auto.offset.reset': config.get('kafka-local-consumer', 'auto.offset.reset'),
    'security.protocol': config.get('kafka-local-consumer', 'security.protocol'),
    'sasl.mechanism': config.get('kafka-local-consumer', 'sasl.mechanism'),
    'sasl.username': config.get('kafka-local-consumer', 'sasl.username'),
    'sasl.password': config.get('kafka-local-consumer', 'sasl.password'),
}

topic_read = config.get('kafka', 'topic')
topic_write = 'test'

# print(kafka_config)
