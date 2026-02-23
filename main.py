import logging

from fastapi import FastAPI
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from database.manager import get_all
from database.public_models.models import Client
from routes.client_router import client_router

app = FastAPI()
logger = logging.getLogger("uvicorn.error")


@app.get("/")
def read_root():
    clients: list[Client] = get_all(Client)
    logger.info("Total number of clients %s", len(clients))
    return JSONResponse(status_code=200, content=jsonable_encoder({"clients": clients}))


@app.get("/error")
def base():
    return JSONResponse(status_code=500, content={"status": "Error endpoint"})


app.include_router(client_router)
