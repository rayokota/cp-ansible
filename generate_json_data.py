import os
import json
import random
from confluent_kafka import Producer

TOPIC = os.environ['TOPIC_NAME']
SOURCE_BROKER = os.environ['SOURCE_BROKER']
NUM_MESSAGES = int(os.environ['NUM_MESSAGES'])

msg = '{"id":%d, "ts":%s, "temp":%d}'
x = msg % (1, 2, 50)
assert json.loads(x)

producer = Producer({
    'bootstrap.servers': SOURCE_BROKER + ':9092',
    'broker.version.fallback': '0.10.0.0',
    'api.version.fallback.ms': 0
})

random.seed(0)

for i in range(NUM_MESSAGES):
    jittered_ts = 1540220627603 + i*10*1000
    temperature = random.randint(0,100)
    newmsg = msg % (i, jittered_ts, temperature)
    print (newmsg)
    producer.produce(TOPIC, newmsg)
    producer.flush()