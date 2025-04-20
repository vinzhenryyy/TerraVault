import json, redis, bcrypt, jwt, time
from kafka import KafkaConsumer, KafkaProducer

KAFKA_URL = "localhost:9092"
TOPIC = "user-management"
NOTIFICATION_TOPIC = "notifications"
LOGIN_RESPONSE_TOPIC = "login-responses"
SIGNUP_RESPONSE_TOPICS = "signup-responses"

JWT_SECRET = "your_secret_key"
JWT_ALGORITHM = "HS256"
JWT_EXP_DELTA_SECONDS = 900

r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

MAX_LOGIN_ATTEMPTS = 5
LOCKOUT_TIME_SECONDS = 300
logged_in_users = {}

producer = KafkaProducer(
    bootstrap_servers=KAFKA_URL,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_URL,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    group_id="user_management_group"
)

def save_logged_in_users():
    r.set("logged_in_users", json.dumps(logged_in_users))

def send_notification(message):
    producer.send(NOTIFICATION_TOPIC, {"message": message})

def generate_jwt(account_id: str):
    payload = {
        "account_id": account_id,
        "exp": time.time() + JWT_EXP_DELTA_SECONDS
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)

def handle_signup(data):
    new_account_id = data["account_id"]
    email = data["email"]
    password = data["password"]
    name = data["name"]

    if r.sismember("emails", email):
        print(f"Email {email} already registered.")
        producer.send(SIGNUP_RESPONSE_TOPICS, {
            "account_id": new_account_id,
            "status": "failed",
            "reason": "Email already registered."
        })
        return
    
    hashed_pw = bcrypt.hashpw(password.encode(), bcrypt.gensalt())

    r.set(f"account:{new_account_id}:balance", 500)
    r.set(f"account:{new_account_id}:points", 0)
    r.sadd("emails", email)
    r.hset(f"user:{new_account_id}", "password", hashed_pw.decode('utf-8'))
    r.hset(f"user:{new_account_id}", "name", name)
    r.hset(f"user:{new_account_id}", "email", email)
    print(f"User {new_account_id} registered.")
    send_notification(f"User {new_account_id} has registered an account.")

    producer.send(SIGNUP_RESPONSE_TOPICS, {
        "account_id": new_account_id,
        "status": "success"
    })

def handle_login(data):
    account_id = data["account_id"]
    password = data["password"]
    user_key = f"user:{account_id}"
    user_data = r.hgetall(user_key)

    login_response = {
        "account_id": account_id,
        "status": "failed",
        "reason": ""
    }

    if not user_data:
        login_response["reason"] = "Account not found."
    elif r.exists(f"lockout:{account_id}"):
        ttl = r.ttl(f"lockout:{account_id}")
        login_response["reason"] = f"Account is locked. Try again in {ttl} seconds."
    else:
        stored_password = user_data.get("password")
        if stored_password and bcrypt.checkpw(password.encode(), stored_password.encode()):
            r.set(f"logged_in:{account_id}", "true")
            logged_in_users[account_id] = account_id
            save_logged_in_users()
            send_notification(f"User {account_id} logged in.")
            r.delete(f"failures:{account_id}")
            login_response["status"] = "success"
            login_response.pop("reason", None)
        else:
            fail_key = f"failures{account_id}"
            attempts = int(r.get(fail_key) or 0) + 1
            r.set(fail_key, attempts, ex=LOCKOUT_TIME_SECONDS)
            if attempts >= MAX_LOGIN_ATTEMPTS:
                r.set(f"lockout:{account_id}", 1, ex=LOCKOUT_TIME_SECONDS)
                login_response["reason"] = "Account locked due to too many failed attempts."
            else:
                login_response["reason"] = f"Invalid password. Attempts left: {MAX_LOGIN_ATTEMPTS - attempts}"

    producer.send("login-responses", login_response)

def handle_logout(data):
    account_id = data["account_id"]
    if r.get(f"logged_in:{account_id}"):
        r.delete(f"logged_in:{account_id}")
        send_notification(f"User {account_id} logged out.")
        print(f"User {account_id} logged out.")
    else:
        print(f"User {account_id} was not logged in.")

def consume():
    print("User Management Consumer started...")
    for message in consumer:
        data = message.value
        action = data.get("action")

        if action == "signup":
            handle_signup(data)
        elif action == "login":
            handle_login(data)
        elif action == "logout":
            handle_logout(data)
        else:
            print(f"Unknown action: {action}")

if __name__ == "__main__":
    consume()


