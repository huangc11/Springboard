# detector/app.py
import os
import json
from kafka import KafkaConsumer, KafkaProducer

TRANSACTIONS_TOPIC = os.environ.get("TRANSACTIONS_TOPIC")

KAFKA_BROKER_URL = "kafka1:19092"
#TRANSACTIONS_TOPIC = "queueing.transactions"
LEGIT_TOPIC ="legit_topic"
FRAUD_TOPIC ="fraud_topic"


def is_suspicious(transaction):
    """Determine whether a transaction is suspicious."""
    return transaction["amount"] >= 900

if __name__ == "__main__":
    print(' hello !')
    print(TRANSACTIONS_TOPIC)

    consumer = KafkaConsumer(
    TRANSACTIONS_TOPIC,
    bootstrap_servers=KAFKA_BROKER_URL,
    value_deserializer=lambda value: json.loads(value),
    )
    producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER_URL,
    value_serializer=lambda value: json.dumps(value).encode(),
    )
    print(type(consumer))
    for message in consumer:
        transaction: dict = message.value
        topic = FRAUD_TOPIC if is_suspicious(transaction) else LEGIT_TOPIC
        print(topic, transaction) 
        producer.send(topic, value=transaction)



