import logging

from fastapi import FastAPI
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from database.manager import get_all
from database.public_models.models import Client
from routes.brand_router import brand_router
from routes.client_router import client_router
from routes.company_router import company_router
from routes.conversation_router import conversation_router
from routes.geography_router import geography_router
from routes.permissions_router import permissions_router
from routes.sow_router import sow_router
from routes.tenant_user_router import tenant_user_router
from routes.topic_router import topic_router
from routes.trend_router import trend_router

app = FastAPI()
logger = logging.getLogger("uvicorn.error")


@app.get("/")
def read_root() -> JSONResponse:
    clients: list[Client] = get_all(Client)
    return JSONResponse(status_code=200, content=jsonable_encoder({"clients": clients}))


@app.get("/error")
def error() -> JSONResponse:
    return JSONResponse(status_code=500, content={"status": "Generic error"})


@app.get("/_health")
def health() -> JSONResponse:
    return JSONResponse(status_code=200, content={"status": "OK"})


app.include_router(brand_router)
app.include_router(conversation_router)
app.include_router(client_router)
app.include_router(company_router)
app.include_router(geography_router)
app.include_router(permissions_router)
app.include_router(sow_router)
app.include_router(tenant_user_router)
app.include_router(topic_router)
app.include_router(trend_router)
