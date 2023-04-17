import json
from time import sleep
from kafka import KafkaProducer
import requests
from requests.auth import HTTPBasicAuth


username='a57de080-f7bc-4022-93dc-612d2af58d31'
producer=KafkaProducer(bootstrap_servers=['localhost:9092'])

auth = HTTPBasicAuth(username,'')
response = requests.get("http://128.199.176.197:7551/streaming",stream=True,auth=auth)

def get_data():
    response = requests.get("http://128.199.176.197:7551/streaming",stream=True,auth=auth)
    buffer = ""
    for line in response.iter_lines():
        line = line.decode('utf-8')
        if line == '[{':
            buffer = "{"
        elif line == '},{':
            buffer += '}'
            yield json.loads(buffer)
            sleep(1)
            buffer = '{'  
        elif line == '  }' and "7b4700b2-0801-4626-8cf1-33c8f71dd9f4" in buffer:
            buffer += '}}'
            yield json.loads(buffer)
            sleep(1)
            break
        else:
            buffer += line
        
for data in get_data():
    producer.send('socmed',json.dumps(data).encode('utf-8'))
