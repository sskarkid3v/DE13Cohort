import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer
import uuid

#configure the Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

ACCOUNT_IDS = ['ACC1234', 'ACC5678', 'ACC9012']
TRANSACTION_TYPES = ['DEPOSIT', 'WITHDRAWAL', 'TRANSFER']
CURRENCIES = ['USD', 'EUR', 'NPR']

def generate_transaction():
    transaction = {
        "event_id": str(uuid.uuid4()),
        "account_id": random.choice(ACCOUNT_IDS),
        "transaction_type": random.choice(TRANSACTION_TYPES),
        "amount": round(random.uniform(10, 1000), 2),
        "currency": random.choice(CURRENCIES),
        "channel": random.choice(['ATM', 'ONLINE', 'BRANCH']),
        "merchant": random.choice(['Amazon', 'Walmart', 'Target', 'Local Store']),
        "location": random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston']),
        "timestamp": datetime.utcnow().isoformat()+"Z"
    }
    return transaction

if __name__ == "__main__":
    topic_name="banking.transactions"
    print(f"Producing transactions to topic: {topic_name}\n")
    
    try:
        while True:
            transaction = generate_transaction()
            #sending message to kafka topic
            print(f"Produced transaction: {transaction}")
            producer.send(topic_name, value=transaction)
            
            #making usre the message is actually sent to the topic
            producer.flush()
            time.sleep(1)  # Simulate delay between transactions
    except KeyboardInterrupt:
        print("Stopping transaction producer...")
    finally:
        producer.close()