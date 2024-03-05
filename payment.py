# Import libraries
from confluent_kafka.avro import AvroConsumer
from confluent_kafka import avro
import json
import mysql.connector

# Function for inserting data to MySQL
def insert_data(rental_date, rental_id):
    try:
        connection = mysql.connector.connect(host='127.0.0.1',
                                database='sakila',
                                user='root',
                                password='root',
                                port=3309)

        mySql_insert_query = """INSERT INTO sakila.payment(customer_id, staff_id, rental_id, amount, payment_date)
                                SELECT r.customer_id, 
                                r.staff_id, 
                                r.rental_id,
                                f.rental_rate AS amount, 
                                "{}" AS payment_date FROM sakila.rental r
                                JOIN sakila.inventory i ON r.inventory_id = i.inventory_id
                                JOIN sakila.film f ON i.film_id = f.film_id
                                WHERE r.rental_id = {};
                                """.format(rental_date, rental_id)

        cursor = connection.cursor()
        cursor.execute(mySql_insert_query)
        connection.commit()
        cursor.close()

    except mysql.connector.Error as error:
        print("Failed to insert record into table {}".format(error))

# Function for running KafkaConsumer and inserting data to MySQL
def read_messages():
    consumer_config = {"bootstrap.servers": "localhost:9092",
                       "schema.registry.url": "http://localhost:8081",
                       "group.id": "sakila_payment",
                       "auto.offset.reset": "latest"}

    consumer = AvroConsumer(consumer_config)
    consumer.subscribe(["sakila-rental"])

    while(True):
        try:
            records = consumer.poll(2)
        except Exception as e:
            print(f"Exception while trying to poll messages - {e}")
        else:
            if records:
                rental_id = records.value().get("rental_id")
                rental_date = records.value().get("rental_date")
                insert_data(rental_date, rental_id)
                print("rental_id: {}, rental_date: {}".format(rental_id, rental_date))
                consumer.commit()
            else:
                print("No new messages at this point. Try again later.")
    consumer.close()

if __name__ == "__main__":
    read_messages()