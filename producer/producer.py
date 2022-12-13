from kafka import KafkaProducer
import json
import datetime as dt

bootstrap_servers = ['kafka:9092']
topicName = 'test'
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
key = None

headers = []

#message_data = json.dumps({'name': 'Landro', 'age': 35, 'city': 'TesteTest'}).encode('utf-8') 

message_data = [{'cod_cliente': 1, 'agencia': 27, 'valor_op': 100, 'tipo_op': 'saque', 'data': str(dt.date.today()), 'saldo': 666},
                {'cod_cliente': 2, 'agencia': 223, 'valor_op': 500, 'tipo_op': 'deposito', 'data': str(dt.date.today()), 'saldo': 5000},
                {'cod_cliente': 3, 'agencia': 100, 'valor_op': 500, 'tipo_op': 'deposito', 'data': str(dt.date.today()), 'saldo': 5000},
                {'cod_cliente': 4, 'agencia': 27, 'valor_op': 1000, 'tipo_op': 'saque', 'data': str(dt.date.today()), 'saldo': 10000},
                {'cod_cliente': 5, 'agencia': 105, 'valor_op': 300, 'tipo_op': 'deposito', 'data': str(dt.date.today()), 'saldo': 5000}]



for message in message_data:    
    message = json.dumps(message).encode('utf-8')  
    producer.send(topicName,
              message,
              key,
              headers)
    producer.flush()

