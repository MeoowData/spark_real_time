from confluent_kafka import Producer
from faker import Faker
import socket
import logging
import sys
import random
import datetime
import json
import time

# gerando as configuracoes para conexao producer ao Kafka
producer = Producer({'bootstrap.servers' : 'broker:29092'})

# configuracoes para o arquivo de log
logging.basicConfig(format = "%(asctime)s::%(levelname)s::\
                              %(name)s::%(message)s",
                    filename = '/usr/src/log/producer.log',
                    filemode = 'w')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# gerando criterios para alimentar o arquivo de log da aplicacao que enviara mensagens como producer
def log(error, mensagem):
    if error is not None:
        logger.critical('Erro encontrado na mensagem inserida, que nao permitiu a continuidade da execucao do programa')
    else:
        message = 'Mensagem produzida dentro do topico {} com os valores {}\n'.format(mensagem.topic(), mensagem.value().decode('utf-8'))
        logger.info(message)
        print(message)

print("Producer inicializado com sucesso!\n")

fake = Faker()

# como queremos que sejam enviados dados por um periodo de tempo indeterminado, para isso podemos gerar um bash script para a execucao do script por tempo indeterminado

# gera um numero aleatório para o número de mensagens, entre 8 e 20
num_msgs = random.randint(8, 20)


lista_bandeiras = ['maestro', 'mastercard', 'visa16', 'visa13', 'visa19', 'amex', 'discover', 'diners', 'jcb15', 'jcb16']

while True:
    
    print("Inicio de uma nova sessão de mensagens!\n")
    
    for i in range(num_msgs):

        bandeira = random.choice(lista_bandeiras)
        dados = {
                 'name' : fake.name(),
                 'bandeira' : fake.credit_card_provider(bandeira),
                 'credit_card' : fake.credit_card_number(card_type = bandeira),
                 'value' : fake.pricetag(),
                 'dia_compra' : str(datetime.date.today()),
                 'horario_compra' : datetime.datetime.now().strftime('%H:%M:%S'),
                 'formato_captura' : random.choice(['Gateway', 'POS', 'TEF'])
                }
        msg = json.dumps(dados)
        producer.poll(1)
        producer.produce('streaming_cartao', msg.encode('utf-8'), callback = log)
        producer.flush()

    time.sleep(random.randint(1, 60))
