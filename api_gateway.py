from fastapi import FastAPI, Query, HTTPException, Depends, Body, Header
from kafka import KafkaProducer, KafkaConsumer
import json, uuid, time, redis, jwt, threading
from datetime import datetime
from zoneinfo import ZoneInfo
from pydantic import BaseModel, EmailStr
from authentication import verify_token
from fastapi.middleware.cors import CORSMiddleware

KAFKA_URL = "localhost:9092"
TRANSACTION_TOPIC = "transactions"
ACCOUNT_LOGS_TOPIC = "account-logs"
POINTS_TOPIC = "points"
USER_TOPIC = "user-management"
NOTIFICATIONS_TOPIC = "notifications"
BALANCE_UPDATES_TOPIC = "balance-updates"

JWT_SECRET = "your_secret_key"
JWT_ALGORITHM = "HS256"
JWT_EXP_DELTA_SECONDS = 900

app = FastAPI()

producer = KafkaProducer(
    bootstrap_servers=KAFKA_URL,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def format_date():
    return datetime.now(ZoneInfo("Asia/Manila")).strftime("%b %d, %Y - %I:%M %p")

def generate_jwt(account_id: str):
    payload = {
        "account_id": account_id,
        "exp": time.time() + JWT_EXP_DELTA_SECONDS
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)
# ---------------------------
# Models
# ---------------------------

class SignUpRequest(BaseModel):
    name: str
    email: EmailStr
    password: str

class LoginRequest(BaseModel):
    account_id: str
    password: str

class LogoutRequest(BaseModel):
    account_id: str

class TransactionRequest(BaseModel):
    account_id: str
    amount: float
    transaction_type: str
    to_account_id: str = None

class BalanceResponse(BaseModel):
    balance: float

class PointResponse(BaseModel):
    points: float

# ---------------------------
# Endpoints
# ---------------------------

signup_responses = {}
login_responses = {}

signup_response_consumer = KafkaConsumer(
    "signup-responses",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    group_id="api-signup-response-consumer"
)

login_response_consumer = KafkaConsumer(
    "login-responses",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    group_id="api-login-response-consumer"
)

def consume_login_responses():
    for message in login_response_consumer:
        data = message.value
        account_id = data.get("account_id")
        login_responses[account_id] = data

def consume_signup_responses():
    for message in signup_response_consumer:
        data = message.value
        account_id = data.get("account_id")
        signup_responses[account_id] = data

@app.on_event("startup")
def start_background_consumers():
    threading.Thread(target=consume_signup_responses, daemon=True).start()
    threading.Thread(target=consume_login_responses, daemon=True).start()

@app.post("/signup")
def signup(request: SignUpRequest):
    account_id = str(uuid.uuid4().int)[:6] 
    signup_data = {
        "action": "signup",
        "account_id": account_id,
        "name": request.name,
        "email": request.email,
        "password": request.password,
        "timestamp": format_date()
    }

    producer.send(USER_TOPIC, signup_data)

    timeout = 5
    start = time.time()
    while time.time() - start < timeout:
        if account_id in signup_responses:
            result = signup_responses.pop(account_id)
            if result["status"] == "failed":
                raise HTTPException(status_code=400, detail=result["reason"])
            return {"message": "Signup request sent", "account_id": account_id}
        time.sleep(0.1)

    raise HTTPException(status_code=500, detail="Signup failed. Please try again.")

@app.post("/login")
def login(request: LoginRequest):
    login_data = {
        "action": "login",
        "account_id": request.account_id,
        "password": request.password,
        "timestamp": format_date()
    }

    producer.send(USER_TOPIC, login_data)

    timeout = 5
    start = time.time()
    while time.time() - start < timeout:
        if request.account_id in login_responses:
            result = login_responses.pop(request.account_id)
            status = result.get("status")
            if status == "failed":
                raise HTTPException(status_code=401, detail=result.get("reason", "Login failed"))
            elif status == "success":
                token = generate_jwt(request.account_id)
                return {"message": "Login successful", "token": token, "account_id": request.account_id}
        time.sleep(0.1)

    raise HTTPException(status_code=500, detail="Login timed out. Please try again.")

@app.post("/logout")
def logout(request: LogoutRequest):
    logout_data = {
        "action": "logout",
        "account_id": request.account_id,
        "timestamp": format_date()
    }

    producer.send(USER_TOPIC, logout_data)
    return {"message": "Logout request sent"}

@app.get("/get_user_name/{account_id}")
def get_user_name(account_id: str, authorization: str = Header(...)):
    try:
        if not authorization.startswith("Bearer "):
            raise HTTPException(status_code=401, detail="Invalid auth header")

        token = authorization.split(" ")[1]
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        if payload["account_id"] != account_id:
            raise HTTPException(status_code=403, detail="Token does not match account")
    except jwt.PyJWTError:
        raise HTTPException(status_code=401, detail="Invalid token")

    name = r.hget(f"user:{account_id}", "name")
    if not name:
        raise HTTPException(status_code=404, detail="User not found")
    return {"name": name}

@app.post("/refresh-token")
def refresh_token(authorization: str = Header(...)):
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid auth header")

    token = authorization.split(" ")[1]
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM], options={"verify_exp": False})
        exp = payload.get("exp")
        current_time = time.time()

        if exp - current_time < 180:
            account_id = payload["account_id"]
            new_token = jwt.encode({
                "account_id": account_id,
                "exp": current_time + JWT_EXP_DELTA_SECONDS
            }, JWT_SECRET, algorithm=JWT_ALGORITHM)
            return {"token": new_token}
        else:
            return {"token": token}
    except jwt.PyJWTError:
        raise HTTPException(status_code=401, detail="Invalid token")

@app.post("/transaction")
def make_transaction(request: TransactionRequest, account_id: str = Depends(verify_token)):
    is_logged_in = r.get(f"logged_in:{request.account_id}")
    if not is_logged_in:
        raise HTTPException(status_code=403, detail="User not logged in")

    if request.transaction_type not in ["deposit", "withdraw", "transfer", "convert"]:
        raise HTTPException(status_code=400, detail="Invalid transaction type!")

    transaction_data = {
        "account_id": request.account_id,
        "amount": request.amount,
        "transaction_type": request.transaction_type,
    }

    if request.transaction_type == "transfer":
        if not request.to_account_id:
            raise HTTPException(status_code=400, detail="to_account_id is required for transfers.")
        transaction_data["to_account_id"] = request.to_account_id

    producer.send(TRANSACTION_TOPIC, transaction_data)
    producer.send(NOTIFICATIONS_TOPIC, {
        "account_id": request.account_id,
        "message": f"{request.transaction_type.capitalize()} of ${request.amount} initiated."
    })

    return {"message": "Transaction request sent for processing."}

@app.get("/get_balance/{account_id}", response_model=BalanceResponse)
def get_balance(account_id: str):
    balance = r.get(f"account:{account_id}:balance")
    if balance is None:
        raise HTTPException(status_code=404, detail="Account not found")
    return {"balance": float(balance)}

@app.get("/get_points/{account_id}", response_model=PointResponse)
def get_points(account_id: str):
    points = r.get(f"account:{account_id}:points")
    if points is None:
        raise HTTPException(status_code=404, detail="Account not found")
    return {"points": float(points)}

@app.post("/convert_points/{account_id}")
def convert_points_to_balance(account_id: str):
    message = {
        "account_id": account_id,
        "action": "convert"
    }
    producer.send("points-convert", message)
    return {"message": "Conversion request sent to points service."}

@app.get("/conversion_status/{account_id}")
def get_conversion_status(account_id: str):
    result = r.get(f"conversion_status:{account_id}")
    if result is None:
        raise HTTPException(status_code=404, detail="No recent conversion")
    return json.loads(result)

@app.get("/logs")
def get_logs(account_id: str = Query(...)):
    key = f"account:{account_id}:logs"
    logs = r.lrange(key, 0, -1)
    parsed_logs = [json.loads(log) for log in logs]
    return {"logs": parsed_logs}

@app.delete("/logs")
def delete_all_logs(account_id: str = Query(...)):
    key = f"account:{account_id}:logs"
    deleted = r.delete(key)
    if deleted == 0:
        raise HTTPException(status_code=404, detail="No logs found for this account.")
    return {"message": f"All logs deleted for account {account_id}."}

@app.delete("/logs/{log_id}")
def delete_log(log_id: str, account_id: str = Query(...)):
    key = f"account:{account_id}:logs"
    logs = r.lrange(key, 0, -1)

    found = False
    for log in logs:
        log_obj = json.loads(log)
        if log_obj.get("log_id") == log_id:
            r.lrem(key, 1, log)
            found = True
            break

    if not found:
        raise HTTPException(status_code=404, detail="Log not found.")

    return {"message": f"Log {log_id} deleted successfully."}

@app.get("/notifications/{account_id}")
def get_notifications(account_id: str):
    try:
        notifications = r.lrange(f"notifications:{account_id}", 0, -1)
        return {"notifications": [json.loads(notif) for notif in notifications]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/clear_notifications/{account_id}")
async def clear_notifications(account_id: str):
    try:
        r.delete(f"notifications:{account_id}")
        return {"message": "Notifications cleared successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/clear_notification/{account_id}/{notification_id}")
async def clear_single_notification(account_id: str, notification_id: str):
    try:
        notifications_key = f"notifications:{account_id}"
        notifications = r.lrange(notifications_key, 0, -1)

        notifications = [json.loads(notif) for notif in notifications]
        notifications = [notif for notif in notifications if notif["notification_id"] != notification_id]

        r.delete(notifications_key)

        for notif in notifications:
            r.rpush(notifications_key, json.dumps(notif))

        return {"message": "Notification cleared successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=5001)