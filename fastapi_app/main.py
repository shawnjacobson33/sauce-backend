from fastapi import FastAPI

from fastapi_app.routes import user_router, betting_lines_router


app = FastAPI()


app.include_router(user_router, prefix='/users')
app.include_router(betting_lines_router, prefix='/betting_lines')
