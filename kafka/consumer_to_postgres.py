import json
from datetime import datetime
from logging import exception
import psycopg2
from kafka import KafkaConsumer
from psycopg2 import errors

if __name__ == "__main__":
    topic_name = "banking.transactions"
    
    #1. connect to postgresql
    print("connecting to PostgreSQL database...")
    conn = psycopg2.connect(
        host="localhost",
        database="mydb",
        user="user",
        password="password"
    )
    conn.autocommit = True
    cur = conn.cursor()
    print("connected to PostgreSQL database successfully.\n")
    
    #2. create kafka consumer
    consumer = KafkaConsumer(topic_name,
                             bootstrap_servers='localhost:9092',
                             auto_offset_reset='earliest',
                             enable_auto_commit=True,
                             group_id="transaction-consumers",
                             value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    print(f"Consuming transactions from topic: {topic_name}\n")
    print("Pressing Ctrl+C to stop consuming...\n")
    
    insert_query = """
    INSERT INTO transactions_stream (
        event_id, 
        account_id, 
        transaction_type, 
        amount, 
        currency, 
        channel, 
        merchant, 
        location, 
        event_ts)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    try:
        for message in consumer:
            txn = message.value
            
            #extracting fields from the transaction
            event_id = txn.get("event_id")
            account_id = txn.get("account_id")
            transaction_type = txn.get("transaction_type")
            amount = txn.get("amount")
            currency = txn.get("currency")
            channel = txn.get("channel")
            merchant = txn.get("merchant")
            location = txn.get("location")
            event_ts_str = txn.get("timestamp")
            
            #convert timestamp string to datetime object
            try:
                cur.execute(insert_query, (
                    event_id, 
                    account_id, 
                    transaction_type, 
                    amount, 
                    currency, 
                    channel, 
                    merchant, 
                    location, 
                    event_ts_str
                ))
                print(f"inserted event_id {event_id} into transactions_stream")
            except errors.UniqueViolation:
                #catch duplicate event_id error and skip inserting
                conn.rollback()  #rollback the transaction to clear the error state
                print(f"Duplicate event_id {event_id} - skipping insert")
            except exception as e:
                conn.rollback()  #rollback the transaction to clear the error state
                print(f"Error inserting event_id {event_id}: {e}")
    except KeyboardInterrupt:
        print("Stopping transaction consumer...")
    finally:
        consumer.close()
        cur.close()
        conn.close()
        print("closed kafka consumer and PostgreSQL connection.")