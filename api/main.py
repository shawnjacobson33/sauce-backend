from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routes import user_router, betting_lines_router


app = FastAPI()


app.add_middleware(
    CORSMiddleware,
    allow_origins=['http://localhost:3000'],
    allow_credentials=True,
    allow_headers=['*'],
    allow_methods=['*']
)


app.include_router(user_router, prefix='/users')
app.include_router(betting_lines_router, prefix='/data')
