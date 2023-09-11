from confluent_kafka import Producer
from faker import Faker 
import json 
import time
import logging
import random 



#Configure the format of the logs
#Everytime a new event becomes available, logs will be appended in a producer.log file

fake = Faker()
logging.basicConfig(format='%(asctime)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='producer.log',
                    filemode='w')

logger = logging.getLogger()

#Creating producer by specifying the port of the Kaka cluster
p = Producer({'bootstrap.servers':'localhost:9092'})
print('Kafka Producer has been initiated...')

#A callback function that takes care of acks and errors
#When a valid message is available, it is decoded to utf-8 and printed in the preferred format.
#The same message is also appended in the log file

def receipt(err,msg):
    if err is not None:
        print('Error: {}'.format(err))
    else:
        message = 'Produced message on topic {} with value of {}\n'.format(msg.topic(),msg.value().decode('utf-8'))
        logger.info(message)
        print(message)

#Write a producer loop that simulates the creation of ten users' sign-up events in the current month
# Each message is converted from dictionary to json format to be sent to kafka.
def main():
    for i in range(10):
        data = {
            'user-id': fake.random_int(min=20000,max=100000),
            'user-name': fake.name(),
            'user-address':fake.street_address()+' | '+fake.city()+' | '+fake.country_code(),
            'platform': random.choice(['Mobile','Laptop','Tablet']),
            'signup_at': str(fake.date_time_this_month())
        }
        m = json.dumps(data)
        #poll requests any previous events to producer. If found they are sent to callback
        p.poll(1) # it will block for a maximum of 1 millisecond
        p.produce('user-tracker',m.encode('utf-8'),callback=receipt)
        p.flush() #waits for all outstanding messages to be acknowledged by broker
        time.sleep(3)

if __name__ == '__main__':
    main()


