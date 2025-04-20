import jwt
from fastapi import HTTPException, Header
from jwt import PyJWTError

JWT_SECRET = "your_secret_key"
JWT_ALGORITHM = "HS256"

def verify_token(authorization: str = Header(...)):
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid auth header")

    token = authorization.split(" ")[1]
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return payload["account_id"]
    except PyJWTError:
        raise HTTPException(status_code=401, detail="Invalid or expired token")