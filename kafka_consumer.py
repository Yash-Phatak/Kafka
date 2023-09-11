#Consumers are built to read and often clean, transform or enrich messages from topics
# Consumers run as an unbounded loop that awaits for messages to be delivered to the topics they are listening to
#Messages can be read from the beginning (very first message in the topic) or start from a specific position in the topic, known as offset, which is tracked by Zookeeper.
from confluent_kafka import Consumer

c = Consumer({'bootstrap.servers':'localhost:9092',
              'group.id':'python-consumer',
              'auto.offset.reset':'earliest'})

#Determine the name of the topic that must be consumed or subscribed to
print('Available topics to consume: ',c.list_topics().topics)
c.subscribe(['user-tracker'])

#Starting to receive events and create an infinite loop
#the loop polls the topic, looking for available messages
#consumer can always be adjusted to start and stop from a specific offset

def main():
    while True:
        msg = c.poll(1.0) #timeout
        if msg is None:
            continue
        if msg.error():
            print('Error: {}'.format(msg.error()))
            continue
        data = msg.value().decode('utf-8')
        print(data)
    c.close()

if __name__ == '__main__':
    main()