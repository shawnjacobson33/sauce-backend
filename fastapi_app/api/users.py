from fastapi_app.main import app
from fastapi_app.models import User

from db import db


@app.get('/login')
async def login(user: User):
    if user_data := await db.users.get_user(user.email):
        pass

