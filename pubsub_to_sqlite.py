import os
import json
from google.cloud import pubsub_v1

topic_name = 'projects/nest-psp-1739980377792/topics/nest-events'
subscription_name = 'projects/nest-psp-1739980377792/subscriptions/nest-events-sub'
pull_timeout_sec = 10

def callback(message):
    data = json.loads(message.data.decode('utf-8'))
    
    # Extract key fields
    event_id = data.get('eventId')
    timestamp = data.get('timestamp')
    if 'resourceUpdate' in data:
        traits = data['resourceUpdate'].get('traits', {})
        for trait_name, trait_data in traits.items():
            print(f"Event: {event_id}")
            print(f"Timestamp: {timestamp}")
            print(f"Trait: {trait_name}")
            print(f"Data: {trait_data}")
            print("-" * 50)
    # message.ack()

with pubsub_v1.SubscriberClient() as subscriber:
    future = subscriber.subscribe(subscription_name, callback)
    try:
        future.result(timeout=pull_timeout_sec)
    except KeyboardInterrupt:
        future.cancel()
    except TimeoutError:
        print("Pull timeout reached, terminating")
        future.cancel()