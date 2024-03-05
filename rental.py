# Import libraries
import socket
from confluent_kafka import avro,SerializingProducer
from confluent_kafka.avro import AvroProducer
from time import sleep
import logging
import random
import datetime
from datetime import timedelta

# Function for loading avro schema
def load_avro_schema_from_file():
    value_schema = avro.load('avro_schema/value.avsc')
    key_schema = avro.load('avro_schema/key.avsc')
    return key_schema, value_schema

# Function for checking messages info (topic, offset, and partition)
def message(err,msg):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logging.basicConfig(format='%(asctime)s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        filename='producer.log',
                        filemode='w')    
    if err is not None:
        print('Error: {}'.format(err))
    else:
        message = '''topic: {}, Offset: {}, Partition {}'''.format(msg.topic(), msg.offset(), msg.partition())   
        logger.info(message)
        print(message)

# Function for generating random rental_date and returned_date
def date_rented():
    start_date = datetime.datetime.now()
    end_date = start_date - timedelta(days=100)

    rental_date = (start_date + (end_date - start_date) * random.random())
    
    returned_date = rental_date + timedelta(days=random.randint(2,7))
    return rental_date, returned_date

# Function for sending the record to kafka
def send_record():
    key_schema, value_schema = load_avro_schema_from_file()
    conf = {'bootstrap.servers': 'localhost:9092',
        'client.id': socket.gethostname(),
        'enable.idempotence':'true',
        'schema.registry.url': 'http://localhost:8081',
        'compression.type':'snappy',
        'acks':'-1'
        }
    producer = AvroProducer(conf, default_key_schema=key_schema, default_value_schema=value_schema)
    for i in range (1,10):
        rental_date, returned_date = date_rented()
        key = {"key":"This is key " + str(i)}
        value = {"rental_date": rental_date.strftime("%Y-%m-%d %H:%M:%S.%f"),
            "inventory_id": random.randint(1,4581),
            "customer_id": random.randint(1,599),
            "return_date": returned_date.strftime("%Y-%m-%d %H:%M:%S.%f"),
            "staff_id": random.randint(1,2)       
            }
        try:
            producer.produce(topic='rental', key=key, value=value, callback=message)
        except Exception as e:
            print(f"Exception while producing record value - {i}: {e}")
            break
        else:
            print(f"Successfully producing record - {i}")
        producer.flush()
        sleep(0.03)
    print(i)

if __name__ == "__main__":
    send_record()