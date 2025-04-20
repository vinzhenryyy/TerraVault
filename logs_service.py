import json
import uuid
import redis
from kafka import KafkaConsumer
from datetime import datetime
from zoneinfo import ZoneInfo

KAFKA_URL = "localhost:9092"
TOPIC_TRANSACTIONS = "transactions"

r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

def consume_logs():
    consumer = KafkaConsumer(
        TOPIC_TRANSACTIONS,
        bootstrap_servers=KAFKA_URL,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        group_id="logs-service-group"
    )

    print("[Logs Service] Listening for transactions...")

    for message in consumer:
        data = message.value
        timestamp = datetime.now(ZoneInfo("Asia/Manila")).isoformat()
        log_id = str(uuid.uuid4())

        data["timestamp"] = timestamp
        data["log_id"] = log_id

        account_id = data.get("account_id")
        if data.get("transaction_type") == "transfer":
            data["receiver_account_id"] = data.get("to_account_id")

        if account_id:
            r.rpush(f"account:{account_id}:logs", json.dumps(data))

        print(f"[Logs Service] Stored log for {account_id}: {log_id}")

if __name__ == "__main__":
    consume_logs()
