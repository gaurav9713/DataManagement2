import pandas as pd
import os
import json
from datetime import datetime
import time
from google.cloud import pubsub_v1


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'C:/Users/gaura/AppData/Roaming/gcloud/application_default_credentials.json'
PUB_SUB_TOPIC = 'EMDAT'
PUB_SUB_PROJECT = 'datamanagement2-385917'

df = pd.read_csv('data/emdat.csv', delimiter=';')

# creating the json data to publish to pub/sub

def push_payload(message):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PUB_SUB_PROJECT, PUB_SUB_TOPIC)
    data = json.dumps(message).encode('utf-8')
    future = publisher.publish(topic_path, data=data)
    print('Pushed message to topic!')

for _, row in df.iterrows():
    data = {str(column):row[column] for column in df.columns}
    # print(data)
    payload = {
        "timestamp": time.time(),
        "payload": data
    }

    push_payload(payload)





# print(df.head())
# print(df.shape)