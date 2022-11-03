import boto3
import json
import uuid
import random
from datetime import datetime
from time import sleep
from faker import Faker
from datetime import datetime
from geopy.geocoders import Nominatim

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
    """Encapsulates a Kinesis stream."""

    def __init__(self, kinesis_client):
        """
        :param kinesis_client: A Boto3 Kinesis client.
        """
        self.kinesis_client = kinesis_client
        self.name = None
        self.details = None
        self.stream_exists_waiter = kinesis_client.get_waiter('stream_exists')
    def put_record(self, partition_key):
        """
        Puts data into the stream. The data is formatted as JSON before it is passed
        to the stream.

        :param data: The data to put in the stream.
        :param partition_key: The partition key to use for the data.
        :return: Metadata about the record, including its shard ID and sequence number.
        """

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
        # "nome": f"{random.choice(primeiro_nome)} {random.choices(population=sobrenomes,weights=[10,8,6,9,8,7,9,6,8,2,4,1,3,2,5,1,4,2,3,3])[0]}",
        "nome": fake.name(),
        "cpf": fake.cpf(),
        "valor": fake.pyfloat(right_digits=2, positive=True, min_value=1, max_value=9999),
        # "bandeira": random.choices(population=bandeira, weights=[40, 60])[0],
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
        cliente.put_record(partition_key=str(uuid.uuid4))
        sleep(1)


# if __name__ == "__main__":
#     format = "%(asctime)s: %(message)s"
#     threads = list()
#     for index in range(1):
#         print("Main    : create and start thread %d.", index)
#         x = threading.Thread(target=thread_function, args=(index,))
#         threads.append(x)
#         x.start()

if __name__ == "__main__":
    thread_function('xap')