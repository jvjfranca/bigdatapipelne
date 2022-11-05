import base64
import boto3
from time import time
from os import environ
import json

ddb_client = boto3.client('dynamodb')
TABLE = environ.get['TABLE']


def handler(event, context):
    ttl = int(time.time() + 24*3600*30)
    for record in event['Records']:
        payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')
        print(json.dumps(payload, indent=2))
        ddb_client.put_item(
            TableName=TABLE,
            Item={
                'CardHolder':{'S': payload['nome']},
                'CardNumber':{'S': payload['numero']},
                'TTL': {'N': ttl}
            }
        )