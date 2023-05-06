import pandas as pd
import os
import json
from datetime import datetime
import time
from google.cloud import pubsub_v1


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'C:/Users/gaura/AppData/Roaming/gcloud/application_default_credentials.json'
PUB_SUB_TOPIC = 'EMDAT'
PUB_SUB_PROJECT = 'datamanagement2-385917'
PUB_SUB_SUBSCRIPTION = 'EMDAT-sub'

timeout = 3.0

def process_payload(message):
    print(f"Received {message.data}.")
    message.ack()

def consume_payload(project, subscription, callback, period):
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project, subscription=subscription)
    print(f"Listening for messages on {subscription_path}..\n")
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    with subscriber:
            try:
                # When `timeout` is not set, result() will block indefinitely,
                # unless an exception is encountered first.                
                streaming_pull_future.result(timeout=period)
            except TimeoutError:
                streaming_pull_future.cancel()

consume_payload(PUB_SUB_PROJECT, PUB_SUB_SUBSCRIPTION, process_payload, timeout)
