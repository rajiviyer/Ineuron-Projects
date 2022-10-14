#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# A simple example demonstrating use of JSONSerializer.

import argparse
import yaml
from uuid import uuid4
from six.moves import input
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
#from confluent_kafka.schema_registry import *
import pandas as pd
from typing import List


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


def get_rorder_instance(file_path):
    df=pd.read_csv(file_path)
    #df=df.iloc[:,1:]
    rorders:List[ROrder]=[]
    count = 0
    for data in df.values:
        count+=1
        rorder=ROrder(dict(zip(columns,data)))
        rorders.append(rorder)
        yield rorder

def rorder_to_dict(rorder:ROrder, ctx):
    """
    Returns a dict representation of a User instance for serialization.
    Args:
        user (User): User instance.
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
    Returns:
        dict: Dict populated with user attributes to be serialized.
    """

    # User._address must not be serialized; omit from dict
    return rorder.record


def delivery_report(err, msg):
    """
    Reports the success or failure of a message delivery.
    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.
    """

    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def main(topic):
    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    schema = schema_registry_client.get_latest_version(f"{topic}-value")
    schema_str = schema.schema.schema_str

    string_serializer = StringSerializer('utf_8')
    json_serializer = JSONSerializer(schema_str, schema_registry_client, rorder_to_dict)

    producer = Producer(sasl_conf())

    print("Producing user records to topic {}. ^C to exit.".format(topic))
    #while True:
        # Serve on_delivery callbacks from previous calls to produce()
    producer.poll(0.0)
    try:
        for rorder in get_rorder_instance(file_path=FILE_PATH):

            print(rorder)
            producer.produce(topic=topic,
                            key=string_serializer(str(uuid4()), rorder_to_dict),
                            value=json_serializer(rorder, SerializationContext(topic, MessageField.VALUE)),
                            on_delivery=delivery_report)
            #break
    except KeyboardInterrupt:
        pass
    except ValueError:
        print("Invalid input, disrorderding record...")
        pass

    print("\nFlushing records...")
    producer.flush()

main("restaurant-take-away-data")
