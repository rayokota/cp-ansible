import os
import json
import time
from confluent_kafka import Consumer, KafkaError

TOPIC = os.environ['TOPIC_NAME']
SOURCE_BROKER = os.environ['SOURCE_BROKER']
NUM_MESSAGES = int(os.environ['NUM_MESSAGES'])

c = Consumer({
    'bootstrap.servers': SOURCE_BROKER + ':9092',
    'group.id': 'verification.consumer.group-%d' %  int(time.time()),
    'auto.offset.reset': 'earliest'
})

c.subscribe([TOPIC])

ids = set()
num_messages_consumed=0

while True and len(ids) < NUM_MESSAGES:
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    print('Received message: {}, headers: {}'.format(msg.value().decode('utf-8'),
                                                     msg.headers()))
    msg = json.loads(msg.value().decode('utf-8'))
    ids.update([msg["id"]])
    num_messages_consumed+=1

print("Consumed %d messages in the topic %s" % (num_messages_consumed, TOPIC))
c.close()