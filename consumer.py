import json
from time import sleep
from kafka import KafkaConsumer

consumerfb = KafkaConsumer('facebook',bootstrap_servers=['localhost:9092'])

for message in consumerfb:
    data = json.loads(message.value)
    print(data)
    sleep(10)


consumertwt = KafkaConsumer('twitter',bootstrap_servers=['localhost:9092'])

for message in consumertwt:
    data = json.loads(message.value)
    print(data)
    sleep(10)


consumeryt = KafkaConsumer('youtube',bootstrap_servers=['localhost:9092'])

for message in consumerfb:
    data = json.loads(message.value)
    print(data)
    sleep(10)


consumerig = KafkaConsumer('instagram',bootstrap_servers=['localhost:9092'])

for message in consumerig:
    data = json.loads(message.value)
    print(data)
    sleep(10)