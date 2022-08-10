# generator/app.py

from random import choices, randint
from string import ascii_letters, digits
account_chars = digits + ascii_letters

def _random_account_id():
    """Return a random account number made of 12 characters."""
    return "".join(choices(account_chars, k=12))

def _random_amount():
    """Return a random amount between 1.00 and 1000.00."""
    return randint(100, 1000000) / 100

def create_random_transaction():
    """Create a fake, randomised transaction."""
    return {
        "source": _random_account_id(),
        "target": _random_account_id(),
        "amount": _random_amount(),
        # Keep it simple: it's all dollars
        "currency": "USD"
    }


import os
import json
from time import sleep
from kafka import KafkaProducer

#from transactions import create_random_transaction
KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL")
TRANSACTIONS_TOPIC = os.environ.get("TRANSACTIONS_TOPIC")
TRANSACTIONS_PER_SECOND = os.environ.get("TRANSACTIONS_PER_SECOND")

KAFKA_BROKER_URL="kafka1:19092"
# = "localhost:9092"

print(TRANSACTIONS_TOPIC)
#TRANSACTIONS_TOPIC = "queueing.transactions"
TRANSACTIONS_PER_SECOND = 10


SLEEP_TIME = 1 / TRANSACTIONS_PER_SECOND
 
if __name__ == "__main__":
    print('TRANSACTIONS_TOPIC:{}'.format(TRANSACTIONS_TOPIC)) 
    producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER_URL,
    # Encode all values as JSON
    value_serializer=lambda value: json.dumps(value).encode(),
    )
    i=0
    while (i<21):
        transaction = create_random_transaction()
        producer.send(TRANSACTIONS_TOPIC, value=transaction)
        #print(TRANSACTIONS_TOPIC)
        print(transaction) # DEBUG
        sleep(SLEEP_TIME)
        i += 1





