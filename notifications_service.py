from kafka import KafkaConsumer, KafkaProducer
import json, redis, threading
from datetime import datetime
from zoneinfo import ZoneInfo
import uuid

KAFKA_URL = "localhost:9092"
TRANSACTION_TOPIC = "transactions"
NOTIFICATION_TOPIC = "notifications"

r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_URL,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def format_date():
    return datetime.now(ZoneInfo("Asia/Manila")).strftime("%b %d, %Y - %I:%M %p")

def consume_transactions():
    transaction_consumer = KafkaConsumer(
        TRANSACTION_TOPIC,
        bootstrap_servers=KAFKA_URL,
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )

    for message in transaction_consumer:
        data = message.value
        account_id = data.get("account_id")
        transaction_type = data.get("transaction_type")
        amount = data.get("amount")
        timestamp = format_date()
        notification_id = str(uuid.uuid4())

        if transaction_type == "deposit":
            message = f"[DEPOSIT] +${amount}"
            notification = {
                "notification_id": notification_id,
                "account_id": account_id,
                "message": message,
                "timestamp": timestamp,
                "type": "deposit",
                "amount": amount
            }
            r.rpush(f"notifications:{account_id}", json.dumps(notification))
            producer.send(NOTIFICATION_TOPIC, notification)
            print(f"Notification sent for deposit: {notification}")

        elif transaction_type == "withdraw":
            message = f"[WITHDRAW] -${amount}"
            notification = {
                "notification_id": notification_id,
                "account_id": account_id,
                "message": message,
                "timestamp": timestamp,
                "type": "withdraw",
                "amount": amount
            }
            r.rpush(f"notifications:{account_id}", json.dumps(notification))
            producer.send(NOTIFICATION_TOPIC, notification)
            print(f"Notification sent for withdrawal: {notification}")

        elif transaction_type == "transfer":
            to_account = data.get("to_account_id")

            sender_msg = f"[TRANSFER] -${amount}"
            recipient_msg = f"[TRANSFER] +${amount}"
            sender_notification = {
                "notification_id": notification_id,
                "account_id": account_id,
                "message": sender_msg,
                "timestamp": timestamp,
                "type": "transfer",
                "amount": amount,
                "to_account": to_account
            }
            recipient_notification = {
                "notification_id": notification_id,
                "account_id": to_account,
                "message": recipient_msg,
                "timestamp": timestamp,
                "type": "transfer",
                "amount": amount,
                "from_account": account_id
            }
            r.rpush(f"notifications:{account_id}", json.dumps(sender_notification))
            r.rpush(f"notifications:{to_account}", json.dumps(recipient_notification))

            producer.send(NOTIFICATION_TOPIC, sender_notification)
            producer.send(NOTIFICATION_TOPIC, recipient_notification)
            print(f"Transfer notifications sent: {sender_notification}, {recipient_notification}")

        elif transaction_type == "convert":
            message = f"[CONVERT] -${amount}"
            notification = {
                "notification_id": notification_id,
                "account_id": account_id,
                "message": message,
                "timestamp": timestamp,
                "type": "convert",
                "amount": amount
            }
            r.rpush(f"notifications:{account_id}", json.dumps(notification))
            producer.send(NOTIFICATION_TOPIC, notification)
            print(f"Notification sent for conversion: {notification}")
        else:
            print("Unknown transaction type:", transaction_type)

if __name__ == "__main__":
    print("Notifications Service consumer running...")
    threading.Thread(target=consume_transactions, daemon=True).start()

    import time
    while True:
        time.sleep(1)
