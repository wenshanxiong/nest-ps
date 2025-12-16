import os
import json
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
from google.cloud import pubsub_v1
import db_util

# Configure logging with rotation
log_handler = RotatingFileHandler('nest-ps.log', maxBytes=10*1024*1024, backupCount=2)
log_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(log_handler)

topic_name = 'projects/nest-psp-1739980377792/topics/nest-events'
subscription_name = 'projects/nest-psp-1739980377792/subscriptions/nest-events-sub'
pull_timeout_sec = 5

def process_message(message):
    """Process a single Pub/Sub message"""
    try:
        data = json.loads(message.message.data.decode('utf-8'))
        
        # Extract key fields
        event_id = data.get('eventId')
        timestamp = data.get('timestamp')
        
        if 'resourceUpdate' in data:
            traits = data['resourceUpdate'].get('traits', {})
            for trait_name, trait_data in traits.items():
                db_util.insert_event(
                    'nest-events.db',
                    event_id,
                    trait_name,
                    timestamp,
                    trait_data
                )
            logger.info(f"Event: {event_id}")
            return True
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        return False

db_util.init_event_store('nest-events.db')

def main():
    subscriber = pubsub_v1.SubscriberClient()
    
    try:
        logger.info(f"Listening for messages on {subscription_name}...")
        response = subscriber.pull(
            request={
                "subscription": subscription_name,
                "max_messages": 100,
            },
            timeout=pull_timeout_sec
        )

        if not response.received_messages:
            logger.info("No messages received, continuing...")
            return
        
        ack_ids = []
        nack_ids = []
        
        for received_message in response.received_messages:
            if process_message(received_message):
                ack_ids.append(received_message.ack_id)
            else:
                nack_ids.append(received_message.ack_id)
        
        # Acknowledge processed messages
        if ack_ids:
            subscriber.acknowledge(
                request={"subscription": subscription_name, "ack_ids": ack_ids}
            )
        
        # Nack failed messages
        if nack_ids:
            subscriber.modify_ack_deadline(
                request={
                    "subscription": subscription_name,
                    "ack_ids": nack_ids,
                    "ack_deadline_seconds": 0
                }
            )
                
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down...")
    except Exception as e:
        logger.error(f"Error: {e}")
    finally:
        subscriber.close()

if __name__ == "__main__":
    main()