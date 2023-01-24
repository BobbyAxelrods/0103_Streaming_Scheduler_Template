
# You are a data engineer at a data analytics consulting company. You have been assigned to a 
# project that aims to de-congest the national highways by analyzing the road traffic data 
# from different toll plazas. As a vehicle passes a toll plaza, the vehicle’s data like
#  vehicle_id,vehicle_type,toll_plaza_id and timestamp are streamed to Kafka. 
# Your job is to create a data pipe line that collects the streaming data and loads it into a database.

# Start a MySQL Database server.
# Create a table to hold the toll data.
# Start the Kafka server.
# Install the Kafka python driver.
# Install the MySQL python driver.
# Create a topic named toll in kafka.
# Download streaming data generator program.
# Customize the generator program to steam to toll topic.
# Download and customise streaming data consumer.
# Customize the consumer program to write into a MySQL database table.
# Verify that streamed data is being collected in the database table.



# wget https://archive.apache.org/dist/kafka/2.8.0/kafka_2.12-2.8.0.tgz
# tar -xzf kafka_2.12-2.8.0.tgz
# start_mysql
# mysql --host=127.0.0.1 --port=3306 --user=root --password=Mjk0NDQtcnNhbm5h
# create database tolldata;
# use tolldata;

# create table livetolldata(timestamp datetime,vehicle_id int,vehicle_type char(15),toll_plaza_id smallint);
# exit
# python3 -m pip install kafka-python
# python3 -m pip install mysql-connector-python 


# Starting Random Generator 

"""
Top Traffic Simulator
"""
from time import sleep, time, ctime
from random import random, randint, choice
from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

TOPIC = 'set your topic here'

VEHICLE_TYPES = ("car", "car", "car", "car", "car", "car", "car", "car",
                 "car", "car", "car", "truck", "truck", "truck",
                 "truck", "van", "van")
for _ in range(100000):
    vehicle_id = randint(10000, 10000000)
    vehicle_type = choice(VEHICLE_TYPES)
    now = ctime(time())
    plaza_id = randint(4000, 4010)
    message = f"{now},{vehicle_id},{vehicle_type},{plaza_id}"
    message = bytearray(message.encode("utf-8"))
    print(f"A {vehicle_type} has passed by the toll plaza {plaza_id} at {now}.")
    producer.send(TOPIC, message)
    sleep(random() * 2)


#Starting streaming data consumer 


"""
Streaming data consumer
"""
from datetime import datetime
from kafka import KafkaConsumer
import mysql.connector

TOPIC='toll'
DATABASE = 'tolldata'
USERNAME = 'root'
PASSWORD = 'MTkyNTctbXVoZHNo'

print("Connecting to the database")
try:
    connection = mysql.connector.connect(host='localhost', database=DATABASE, user=USERNAME, password=PASSWORD)
except Exception:
    print("Could not connect to database. Please check credentials")
else:
    print("Connected to database")
cursor = connection.cursor()

print("Connecting to Kafka")
consumer = KafkaConsumer(TOPIC)
print("Connected to Kafka")
print(f"Reading messages from the topic {TOPIC}")
for msg in consumer:

    # Extract information from kafka

    message = msg.value.decode("utf-8")

    # Transform the date format to suit the database schema
    (timestamp, vehcile_id, vehicle_type, plaza_id) = message.split(",")

    dateobj = datetime.strptime(timestamp, '%a %b %d %H:%M:%S %Y')
    timestamp = dateobj.strftime("%Y-%m-%d %H:%M:%S")

    # Loading data into the database table

    sql = "insert into livetolldata values(%s,%s,%s,%s)"
    result = cursor.execute(sql, (timestamp, vehcile_id, vehicle_type, plaza_id))
    print(f"A {vehicle_type} was inserted into the database")
    connection.commit()
connection.close()

# cd kafka_2.12-2.8.0
# bin/zookeeper-server-start.sh config/zookeeper.properties

# cd kafka_2.12-2.8.0
# bin/kafka-server-start.sh config/server.properties

# cd kafka_2.12-2.8.0
# bin/kafka-topics.sh --create --topic toll --bootstrap-server localhost:9092 

# cd kafka_2.12-2.8.0
# bin/kafka-console-producer.sh --topic toll --bootstrap-server localhost:9092
