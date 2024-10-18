from kafka.kafka_store_consumer import DataStoreConsumer
from kafka_configuration import local_consumer_config, topic_write
from pymongo import MongoClient
import configparser

config = configparser.ConfigParser()
config.read('configuration/mongo_config.ini')
mongo_config = {
    "uri": config.get('mongo', 'uri'),
    "db": config.get('mongo', 'db'),
    "collection": config.get('mongo', 'collection')
}

if __name__ == '__main__':
    consumer = DataStoreConsumer(
        internal_config=local_consumer_config,
        topic=topic_write,
        mongo_client=MongoClient(mongo_config['uri']),
        mongo_db=mongo_config['db'],
        mongo_collection=mongo_config['collection']
    )
    consumer.consume()
