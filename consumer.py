import json
from time import sleep
from kafka import KafkaConsumer

consumer = KafkaConsumer('socmed',bootstrap_servers=['localhost:9092'])

for message in consumer:
    data = json.loads(message.value)
    print(data)
    sleep(10)