# Assignment

* Downloaded Dataset 1 - https://github.com/shashank-mishra219/Confluent-Kafka-Setup/blob/main/restaurant_orders.csv

Data set is downloaded as restaurant_orders.csv
Change **config.yml** to customize the details required for running Confluent Kafka

## Task
1. Setup Confluent Kafka Account
2. Create one kafka topic named as "restaurent-take-away-data" with 3 partitions
3. Setup key (string) & value (json) schema in the confluent schema registry

### Solution
* All are manual steps. No code involved here

## Task
4. Write a kafka producer program (python or any other language) to read data records from restaurent data csv file, 
   make sure schema is not hardcoded in the producer code, read the latest version of schema and schema_str from schema registry and use it for
   data serialization.
5. From producer code, publish data in Kafka Topic one by one and use dynamic key while publishing the records into the Kafka Topic   

### Solution
* Code present in **ro_producer.py**

## Task
6. Write kafka consumer code and create two copies of same consumer code and save it with different names (kafka_consumer_1.py & kafka_consumer_2.py), 
   again make sure lates schema version and schema_str is not hardcoded in the consumer code, read it automatically from the schema registry to desrialize the data. 
   Now test two scenarios with your consumer code:
    a.) Use "group.id" property in consumer config for both consumers and mention different group_ids in kafka_consumer_1.py & kafka_consumer_2.py,
        apply "earliest" offset property in both consumers and run these two consumers from two different terminals. Calculate how many records each consumer
        consumed and printed on the terminal
    b.) Use "group.id" property in consumer config for both consumers and mention same group_ids in kafka_consumer_1.py & kafka_consumer_2.py,
        apply "earliest" offset property in both consumers and run these two consumers from two different terminals. Calculate how many records each consumer
        consumed and printed on the terminal

### Solution
* Code present in **ro_consumer_1.py** & **ro_consumer_2.py**
* With different consumer groups
	- Consumer 1 Items processed: 74818
	- Consumer 2 Items processed: 74818
* With same consumer group
	- Consumer 1 Items processed: 50025
	- Consumer 2 Items processed: 24793

## Task
7. Once above questions are done, write another kafka consumer to read data from kafka topic and from the consumer code create one csv file "output.csv"
   and append consumed records output.csv file

### Solution
* Code present in **ro_consumer.py**
