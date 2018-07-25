import boto3
import json
import os

client = boto3.client('iot-data')

topic = os.environ.get('RESPONSE_TOPIC')

if not topic:
    raise EnvironmentError('Unable to read topic name from environment')


def _check_event(event):
    if 'device' not in event:
        raise AttributeError('Missing "device" attribute in event')

    if 'message' not in event:
        raise AttributeError('Missing "message" attribute in event')

    return event


def lambda_handler(event, _):
    _check_event(event)

    return client.publish(
        topic=topic,
        qos=1,
        payload=json.dumps(event).encode()
    )
