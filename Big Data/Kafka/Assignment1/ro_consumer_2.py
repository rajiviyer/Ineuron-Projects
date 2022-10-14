import argparse
import yaml
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer


with open('config.yml') as stream:
    config = yaml.safe_load(stream)


FILE_PATH = config['FILEPATH']
columns = config['DATA_COLUMNS']

API_KEY = config['API_KEY']
API_SECRET_KEY = config['API_SECRET_KEY']
BOOTSTRAP_SERVER = config['BOOTSTRAP_SERVER']
SECURITY_PROTOCOL = config['SECURITY_PROTOCOL']
SSL_MECHANISM = config['SSL_MECHANISM']
ENDPOINT_SCHEMA_URL = config['ENDPOINT_SCHEMA_URL']
SCHEMA_REGISTRY_API_KEY = config['SCHEMA_REGISTRY_API_KEY']
SCHEMA_REGISTRY_API_SECRET = config['SCHEMA_REGISTRY_API_SECRET']


def sasl_conf():

    sasl_conf = {'sasl.mechanism': SSL_MECHANISM,
                 # Set to SASL_SSL to enable TLS support.
                #  'security.protocol': 'SASL_PLAINTEXT'}
                'bootstrap.servers':BOOTSTRAP_SERVER,
                'security.protocol': SECURITY_PROTOCOL,
                'sasl.username': API_KEY,
                'sasl.password': API_SECRET_KEY
                }
    return sasl_conf



def schema_config():
    return {'url':ENDPOINT_SCHEMA_URL,
    
    'basic.auth.user.info':f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}"

    }


class ROrder:   
    def __init__(self,record:dict):
        for k,v in record.items():
            setattr(self,k,v)
        
        self.record=record
   
    @staticmethod
    def dict_to_rorder(data:dict,ctx):
        return ROrder(record=data)

    def __str__(self):
        return f"{self.record}"


def main(topic, group):

    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    schema = schema_registry_client.get_latest_version(f"{topic}-value")
    schema_str = schema.schema.schema_str

    json_deserializer = JSONDeserializer(schema_str,
                                         from_dict=ROrder.dict_to_rorder)

    consumer_conf = sasl_conf()
    consumer_conf.update({
                     'group.id': group,
                     'auto.offset.reset': "earliest"})

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

    rorder_list = []

    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            rorder = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))

            if rorder is not None:
                rorder_list.append(rorder)
                print("User record {}: rorder: {}\n"
                      .format(msg.key(), rorder))
        except KeyboardInterrupt:
            break
    print(f"Items processed: {len(rorder_list)}")
    consumer.close()

main("restaurant-take-away-data","g1")