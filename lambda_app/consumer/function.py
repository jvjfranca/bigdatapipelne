import base64
import boto3
from time import time
from os import environ
import json

ddb_client = boto3.client('dynamodb')
TABLE = environ.get('TABLE')


def handler(event, context):
    ttl = str(time() + 24*3600*30)
    for record in event['Records']:
        payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')
        data = json.loads(payload)
        ddb_client.put_item(
            TableName=TABLE,
            Item={
                'numero_cartao': {'S': str(data['numero_cartao'])},
                'transaction_id': {'S': str(data['transaction_id'])},
                'horario_transacao': {'S': str(data['horario_transacao'])},
                'Valor':{'N': str(data['valor'])},
                'TTL': {'N': ttl}
            }
        )