import logging

from fastapi import APIRouter, Depends
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from database.manager import get_all
from database.public_models.models import Client
from database.tenant_models.models import TenantSow
from jwt_validator import validate_jwt

logger = logging.getLogger("uvicorn.error")

client_router = APIRouter(prefix="/api/v2", dependencies=[Depends(validate_jwt)])


@client_router.post("/protected")
def protected(authorization: dict = Depends(validate_jwt)):
    org_id = authorization.get("orgId")
    clients: list[Client] = get_all(Client)
    logger.info("Total number of clients %s", len(clients))
    sows: list[TenantSow] = get_all(TenantSow, tenant_schema=org_id)
    logger.info("Total number of SOWs %s", len(sows))
    return JSONResponse(
        status_code=200,
        content=jsonable_encoder({"clients": clients, "sows": sows}),
    )
