# balance_service.py

import json
import redis
from kafka import KafkaConsumer, KafkaProducer

KAFKA_URL = "localhost:9092"
TOPIC_TRANSACTIONS = 'transactions'
TOPIC_BALANCE_UPDATES = 'balance-updates'

consumer = KafkaConsumer(
    TOPIC_TRANSACTIONS,
    bootstrap_servers=KAFKA_URL,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    group_id="balance-service-group"
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_URL,
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

def consume_transactions():
    for message in consumer:
        data = message.value
        account_id = data["account_id"]
        amount = float(data["amount"])
        transaction_type = data["transaction_type"]

        print(f"[Balance Service] Processing {transaction_type} for {account_id} - ${amount}")

        if transaction_type == "deposit":
            current_balance = float(r.get(f"account:{account_id}:balance") or 0)
            current_balance += amount
            r.set(f"account:{account_id}:balance", current_balance)

        elif transaction_type == "withdraw":
            current_balance = float(r.get(f"account:{account_id}:balance") or 0)
            if current_balance < amount:
                print(f"[Balance Service] Insufficient funds for {account_id}")
                continue
            current_balance -= amount
            r.set(f"account:{account_id}:balance", current_balance)

        elif transaction_type == "transfer":
            to_account_id = data.get("to_account_id")
            if not to_account_id:
                print("[Balance Service] Missing to_account_id for transfer.")
                continue

            sender_balance = float(r.get(f"account:{account_id}:balance") or 0)
            if sender_balance < amount:
                print(f"[Balance Service] Insufficient funds for transfer by {account_id}")
                continue

            recipient_balance = float(r.get(f"account:{to_account_id}:balance") or 0)
            sender_balance -= amount
            recipient_balance += amount

            r.set(f"account:{account_id}:balance", sender_balance)
            r.set(f"account:{to_account_id}:balance", recipient_balance)

        elif transaction_type == "convert":
            producer.send(TOPIC_BALANCE_UPDATES, {
                "account_id": account_id,
                "new_balance": current_balance
            })

        else:
            print(f"[Balance Service] Unknown transaction type: {transaction_type}")
            continue

        new_balance = float(r.get(f"account:{account_id}:balance") or 0)
        producer.send(TOPIC_BALANCE_UPDATES, {
            "account_id": account_id,
            "new_balance": new_balance
        })
        print(f"[Balance Service] Updated balance: {new_balance} for {account_id}")

if __name__ == "__main__":
    print("Balance Service consumer running...")
    consume_transactions()
