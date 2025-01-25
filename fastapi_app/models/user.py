from pydantic import BaseModel


class User(BaseModel):
    email: str
    username: str = None
    password: str = None