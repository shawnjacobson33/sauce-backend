from db import db
from fastapi_app.models import User
from fastapi_app.routes.utils import create_access_token, hash_password

from fastapi import HTTPException
from fastapi.routing import APIRouter
from fastapi.responses import JSONResponse


user_router = APIRouter()


@user_router.get("/login")
async def login(user: User):
    try:
        user_data = user.__dict__
        user_data['password'] = hash_password(user_data['password'])
        if user_data := await db.users.get_user(**user_data):
            access_token = create_access_token(user_data)
            return { "access_token": access_token, "token_type": "bearer" }

        return HTTPException(
            status_code=401,
            detail="Failed to authorize!"
        )

    except Exception as e:
        return JSONResponse(
            status_code=400,
            content={"detail": str(e)}
        )


@user_router.get("/register")
async def register(user: User):
    try:
        if await db.users.is_username_valid(user.username):
            user_data = user.__dict__
            await db.users.store_user(**user_data)
            access_token = create_access_token(user_data)
            return { 'access_token': access_token, 'token_type': 'bearer' }

        return HTTPException(
            status_code=401,
            detail="Username already exists!"
        )

    except Exception as e:
        return JSONResponse(
            status_code=400,
            content={"detail": str(e)}
        )
