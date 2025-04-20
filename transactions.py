# DISREGARD THIS

import json
import redis
from kafka import KafkaConsumer

KAFKA_URL = "localhost:9092"
TOPIC_TRANSACTIONS = "transactions"

r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

consumer = KafkaConsumer(
    TOPIC_TRANSACTIONS,
    bootstrap_servers=KAFKA_URL,
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

def process_transaction(data):
    account_id = data.get("account_id")
    amount = data.get("amount")
    transaction_type = data.get("transaction_type")
    to_account_id = data.get("to_account_id")

    if not account_id or not amount or not transaction_type:
        print("Invalid transaction data:", data)
        return

    if transaction_type == "deposit":
        r.incrbyfloat(f"account:{account_id}:balance", amount)
        print(f"Deposited {amount} to {account_id}")

    elif transaction_type == "withdraw":
        current_balance = float(r.get(f"account:{account_id}:balance") or 0)
        if current_balance >= amount:
            r.decrbyfloat(f"account:{account_id}:balance", amount)
            print(f"Withdrew {amount} from {account_id}")
        else:
            print(f"Insufficient funds for {account_id}")

    elif transaction_type == "transfer":
        if not to_account_id:
            print("Missing to_account_id for transfer")
            return
        current_balance = float(r.get(f"account:{account_id}:balance") or 0)
        if current_balance >= amount:
            r.decrbyfloat(f"account:{account_id}:balance", amount)
            r.incrbyfloat(f"account:{to_account_id}:balance", amount)
            print(f"Transferred {amount} from {account_id} to {to_account_id}")
        else:
            print(f"Insufficient funds for transfer by {account_id}")

if __name__ == "__main__":
    print("Transaction consumer started...")
    for message in consumer:
        data = message.value
        process_transaction(data)
