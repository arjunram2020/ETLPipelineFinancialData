# Importing necessary libraries for producer.py - from data.csv to Kafka topic
from kafka import KafkaProducer
import json
import csv
# Create a Kafka producer to send data to the Kafka topic
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))
with open('/Users/arjunramakrishnan/Downloads/NYSE_shortened.csv') as csvfile:
    #Read the CSV file using DictReader and handled the delimeter using ;
    reader = csv.DictReader(csvfile, delimiter=',')
    #sent each user (row) of the CSV file to Kafka topic
    for row in reader:
        print(row)
        producer.send(topic = 'earnings_topic', value = row)
        producer.flush()


