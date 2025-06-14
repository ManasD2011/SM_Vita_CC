import json
import os
from google.cloud import pubsub_v1

publisher = pubsub_v1.PublisherClient()
project_id = os.environ["GCP_PROJECT"]
topic_id = os.environ.get("TOPIC_ID", "file-meta-topic")
topic_path = publisher.topic_path(project_id, topic_id)

def storage_trigger(event, context):
    file_name = event['name']
    file_size = event.get('size', '0')
    file_format = file_name.split('.')[-1] if '.' in file_name else 'unknown'

    message = {
        "file_name": file_name,
        "size": file_size,
        "format": file_format
    }

    publisher.publish(topic_path, json.dumps(message).encode("utf-8"))
    print(f"Published: {message}")

