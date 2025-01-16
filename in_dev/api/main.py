from fastapi import FastAPI

from in_dev.api import router as api_router


app = FastAPI()

app.include_router(api_router)
