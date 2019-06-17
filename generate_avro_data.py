import os
import random
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

TOPIC = os.environ['TOPIC_NAME']
SOURCE_BROKER = os.environ['SOURCE_BROKER']
SR = os.environ['SR']
NUM_MESSAGES = int(os.environ['NUM_MESSAGES'])


key_schema_str = """
{
   "namespace": "confluent.replicator.alex.test",
   "name": "key",
   "type": "record",
   "fields" : [
     {
       "name" : "id",
       "type" : "string"
     }
   ]
}
"""

value_schema_str = """
{
   "namespace": "confluent.replicator.alex.test",
   "name": "value",
   "type": "record",
   "fields" : [
     {
       "name" : "ts",
       "type" : "string"
     }
   ]
}
"""

key_schema = avro.loads(key_schema_str)
value_schema = avro.loads(value_schema_str)

avroProducer = AvroProducer({
    'bootstrap.servers': SOURCE_BROKER + ':9092',
    'schema.registry.url': 'http://' + SR + ':8081'
}, default_key_schema=key_schema, default_value_schema=value_schema)

random.seed(0)

for i in range(NUM_MESSAGES):
    jittered_ts = 1540220627603 + i*10*1000
    temperature = random.randint(0,100)
    newmsg_key = {"id": str(i)}
    newmsg_value = {"ts": str(jittered_ts)}
    avroProducer.produce(topic=TOPIC, key=newmsg_key, value=newmsg_value)
    avroProducer.flush()