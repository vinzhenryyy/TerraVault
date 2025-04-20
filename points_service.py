import json, redis, threading
from kafka import KafkaConsumer, KafkaProducer

KAFKA_URL = "localhost:9092"
TOPIC_TRANSACTIONS = "transactions"
TOPIC_POINTS = "points"
TOPIC_CONVERT_REQUESTS = "points-convert"
TOPIC_CONVERSION_RESPONSES = "conversion-responses"
TOPIC_BALANCE_UPDATES = "balance-updates"

r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)

consumer = KafkaConsumer(
    TOPIC_TRANSACTIONS,
    bootstrap_servers=KAFKA_URL,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    group_id="points-service-group"
)

convert_consumer = KafkaConsumer(
    TOPIC_CONVERT_REQUESTS,
    bootstrap_servers=KAFKA_URL,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    group_id="points-convert-group"
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_URL,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def consume_and_award_points():
    for message in consumer:
        data = message.value
        if data.get("transaction_type") == "deposit":
            account_id = data.get("account_id")
            amount = float(data.get("amount", 0))
            points = round(amount * 0.0001, 2)

            if account_id:
                current_points = float(r.get(f"account:{account_id}:points") or 0)
                current_points += points
                r.set(f"account:{account_id}:points", current_points)

                producer.send(TOPIC_POINTS, {
                    "account_id": account_id,
                    "points": current_points
                })

                print(f"[Points Service] Awarded {points} points to {account_id}. Total: {current_points}")

def consume_convert_requests():
    for message in convert_consumer:
        data = message.value
        account_id = data.get("account_id")
        action = data.get("action")

        if action == "convert":
            points = float(r.get(f"account:{account_id}:points") or 0)
            if points < 100:
                producer.send(TOPIC_CONVERSION_RESPONSES, {
                    "account_id": account_id,
                    "status": "failed",
                    "reason": "Not enough points to convert"
                })
                r.set(f"conversion_status:{account_id}", json.dumps({
                    "status": "failed",
                    "reason": "Insufficient points. Minimum of 100 pts to convert."
                }))
                continue

            amount_to_add = points / 10
            current_balance = float(r.get(f"account:{account_id}:balance") or 0)
            new_balance = current_balance + amount_to_add

            r.set(f"account:{account_id}:balance", new_balance)
            r.set(f"account:{account_id}:points", 0) 

            producer.send(TOPIC_CONVERSION_RESPONSES, {
                "account_id": account_id,
                "status": "success",
                "points_converted": points,
                "amount_added": amount_to_add
            })

            producer.send(TOPIC_TRANSACTIONS, {
                "account_id": account_id,
                "transaction_type": "convert",
                "amount": amount_to_add
            })

            r.set(f"conversion_status:{account_id}", json.dumps({
                "status": "success",
                "points": points,
                "amount": amount_to_add
            }))
            
            print(f"[Points Service] Account ID {account_id} - {points} points converted to balance. Total: {points-points}")


if __name__ == "__main__":
    print("Points Service consumer running...")
    threading.Thread(target=consume_and_award_points, daemon=True).start()
    threading.Thread(target=consume_convert_requests, daemon=True).start()

    import time
    while True:
        time.sleep(1)

