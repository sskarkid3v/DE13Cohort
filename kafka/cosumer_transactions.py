import json
from kafka import KafkaConsumer

if __name__ == "__main__":
    topic_name = "banking.transactions"
    print(f"Consuming transactions from topic: {topic_name}\n")
    
    #configure the Kafka consumer
    consumer = KafkaConsumer(topic_name,
                             bootstrap_servers='localhost:9092',
                             auto_offset_reset='earliest',
                             enable_auto_commit=True,
                             group_id="transaction-consumers",
                             value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    
    try:
        for message in consumer:
            transaction = message.value
            partition = message.partition
            offset = message.offset
            print(f"Consumed transaction: {transaction} (Partition: {partition}, Offset: {offset})")
    except KeyboardInterrupt:
        print("Stopping transaction consumer...")
    finally:
        consumer.close()