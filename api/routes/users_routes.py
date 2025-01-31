from db import dev_db as db
from api.models import User
from api.routes.utils import create_access_token, hash_password, verify_password

from fastapi import HTTPException
from fastapi.routing import APIRouter
from fastapi.responses import JSONResponse


user_router = APIRouter()


@user_router.post("/login")
async def login(user: User):
    try:
        if stored_user_data := await db.users.get_user(email=user.email):
            if verify_password(user.password, stored_user_data['password']):
                access_token = create_access_token(stored_user_data)
                return { "status_code": 200, "access_token": access_token, 'user': stored_user_data }

        return HTTPException(
            status_code=401,
            detail="Invalid Credentials!"
        )

    except Exception as e:
        return JSONResponse(
            status_code=400,
            content={"detail": str(e)}
        )


@user_router.post("/register")
async def register(user: User):
    try:
        if await db.users.is_username_valid(user.username):
            hashed_password = hash_password(user.password)
            await db.users.store_user(username=user.username, email=user.email, password=hashed_password)
            access_token = create_access_token(user.__dict__)
            return { 'status_code': 200, 'access_token': access_token, 'user': user.__dict__ }

        return HTTPException(
            status_code=401,
            detail="Username already exists!"
        )

    except Exception as e:
        return JSONResponse(
            status_code=400,
            content={"detail": str(e)}
        )
