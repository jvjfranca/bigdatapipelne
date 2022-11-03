import boto3
import json
import random
import uuid
from datetime import datetime
from faker import Faker
from geopy.geocoders import Nominatim
from time import sleep

tipo_transacao = [
    'credito',
    'debito'
]

cor_cartao = [
    'preto',
    'prata',
    'amarelo',
    'azul',
    'verde'
]

tipo_cartao = [
    'unlimited',
    'black',
    'platinum',
    'gold',
    'standard'
]

fake = Faker('pt_BR')
locator = Nominatim(user_agent="bbbank")

class KinesisStream:

    def __init__(self, kinesis_client):
        self.kinesis_client = kinesis_client
        self.stream_exists_waiter = kinesis_client.get_waiter('stream_exists')

    def put_record(self):
        self.kinesis_client.put_record(
            StreamName='card-stream',
            Data=json.dumps(gerador_transacoes()),
            PartitionKey=str(uuid.uuid4())
        )

def gerar_cpf():
    cpf = [random.randrange(10) for _ in range(9)]
    for _ in range(2):
        value = sum([(len(cpf) + 1 - i) * v for i, v in enumerate(cpf)]) % 11
        cpf.append(11 - value if value > 1 else 0)
    return "".join(str(x) for x in cpf)

def gerador_transacoes():
    start = datetime(2022,1,1)
    end = datetime(2040,1,1)
    geo = fake.local_latlng('BR')
    latlng = f"{geo[0]},{geo[1]}"
    location = locator.reverse(latlng, exactly_one=True)
    location = location.raw.get('address')
    transacao = {
        "nome": fake.name(),
        "cpf": fake.cpf(),
        "valor": fake.pyfloat(right_digits=2, positive=True, min_value=1, max_value=9999),
        "bandeira": fake.credit_card_provider(),
        "numero_cartao": fake.credit_card_number(),
        "cvv": fake.credit_card_security_code(),
        "exp": fake.credit_card_expire(start, end),
        "tipo_cartao": random.choices(population=tipo_cartao, weights=[5,15,20,25,35])[0],
        "cor_cartao": random.choices(population=cor_cartao, weights=[5,15,20,25,35])[0],
        "tipo_transacao": random.choices(population=tipo_transacao, weights=[65,35])[0],
        "localizacao": {
            'lat': geo[0],
            'lng': geo[1],
            'cidade': geo[2],
            'estado': location['state']
        },
        "horario_transacao": fake.date_time_this_year().strftime("%m/%d/%Y, %H:%M:%S")
    }
    print(f"Transacao enviada: {transacao['valor']}")
    return transacao

def thread_function(name):
    print("Thread %s: starting", name)
    kinesis_client = boto3.client('kinesis')
    cliente = KinesisStream(kinesis_client)
    while True:
        cliente.put_record()
        sleep(1)


if __name__ == "__main__":
    thread_function('xap')