from datetime import datetime, timedelta
import os
import dotenv

import jwt
from passlib.context import CryptContext


dotenv.load_dotenv()

SECRET_KEY = os.getenv('JWT_SECRET_KEY')
ALGO = 'HS256'
EXPIRES_IN = 3600


def create_access_token(user_data: dict) -> str:
    user_data_to_encode = user_data.copy()
    user_data_to_encode['exp'] = datetime.now() + timedelta(seconds=EXPIRES_IN)
    return jwt.encode(user_data_to_encode, SECRET_KEY, algorithm=ALGO)


def validate_access_token(token: str) -> dict:
    try:
        return jwt.decode(token, SECRET_KEY, algorithms=[ALGO])
    except jwt.ExpiredSignatureError:
        return {'error': 'Token has expired'}
    except jwt.InvalidTokenError:
        return {'error': 'Invalid token'}


pwd_context = CryptContext(schemes=['bcrypt'], deprecated='auto')


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(password: str, hashed_password: str) -> bool:
    return pwd_context.verify(password, hashed_password)
